use std::{collections::HashMap, ffi::CString, io::{self, Error, ErrorKind, Write}, os::unix::fs::FileExt, sync::Arc};
use tokio::{io::AsyncReadExt, sync::Mutex as tmutx};
use tokio::fs::{File,self};
use std::collections::btree_set::BTreeSet;
use crate::{database::write_data, gerr, lexer_functions::AlbaTypes};

type MvccType = Arc<tmutx<HashMap<u64,(bool,Vec<AlbaTypes>)>>>;
pub struct Container{
    pub file : std::fs::File,
    pub element_size : usize,
    pub columns : Vec<AlbaTypes>,
    pub str_size : usize,
    pub mvcc : MvccType,
    pub text_mvcc : Arc<tmutx<HashMap<String,(bool,String)>>>,
    pub headers_offset : u64,
    pub column_names : Vec<String>,
    pub location : String,
    pub graveyard : Arc<tmutx<BTreeSet<u64>>>
}



pub trait New {
    async fn new(path : &str,location : String,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> where Self: Sized ;

}

impl New for Container {
    async fn new(path : &str,location : String,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> {
        Ok(Container{
            file : std::fs::OpenOptions::new().read(true).write(true).open(&path)?,
            element_size,
            columns,
            str_size,
            mvcc: Arc::new(tmutx::new(HashMap::new())),
            text_mvcc: Arc::new(tmutx::new(HashMap::new())),
            headers_offset ,
            column_names,
            location,
            graveyard: Arc::new(tmutx::new(BTreeSet::new()))
        })
    }
    
}

async fn sync_file(file: &std::fs::File) -> std::io::Result<()> {
    file.sync_all()
}
async fn try_open_file(path: &str) -> io::Result<Option<File>> {
    match File::open(path).await {
        Ok(file) => Ok(Some(file)),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
impl Container{
    pub async fn len(&self) -> Result<u64,Error>{
        Ok(self.file.metadata()?.len())
    }
    pub async fn arrlen(&self) -> Result<u64, Error> {
        let file_len = self.len().await?;
        let file_rows = if file_len > self.headers_offset {
            (file_len - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        let mvcc_max = {
            let mvcc = match self.mvcc.try_lock() {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Failed to acquire mvcc lock immediately: {:?}", e);
                    return Err(gerr("Could not lock mvcc"));
                }
            };
            mvcc.keys().copied().max().map_or(0, |max_index| max_index + 1)
        };
        Ok(file_rows.max(mvcc_max))
    }
    
    
    pub async fn get_next_addr(&self) -> Result<u64, Error> {
        let mut graveyard = self.graveyard.lock().await;
        if graveyard.len() > 0{
            if let Some(id) = graveyard.pop_first(){
                return Ok(id)
            }
        }
        let current_rows = self.arrlen().await?;
        let mvcc_guard = self.mvcc.lock().await;
        for (&key, (deleted, _)) in mvcc_guard.iter() {
            if *deleted {
                return Ok(key);
            }
        }
        Ok(current_rows)
    } 
    pub async fn push_row(&mut self, data : &Vec<AlbaTypes>) -> Result<(),Error>{
        let ind = self.get_next_addr().await?;
        let mut mvcc_guard = self.mvcc.lock().await;
        mvcc_guard.insert(ind, (false,data.clone()));
        Ok(())
    }
    pub async fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.lock().await;
        mvcc_guard.clear();
        drop(mvcc_guard);

        let mut txt_mvcc = self.text_mvcc.lock().await;
        txt_mvcc.clear();
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), Error> {
        let mut total = self.arrlen().await?;
        println!("commit: Locking MVCC...");
        let mut mvcc_guard = self.mvcc.lock().await;
    
        println!("commit: Separating insertions and deletions...");
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc_guard.iter() {
            let v = (*index, value.1.clone());
            if value.0 {
                deletes.push(v);
            } else {
                insertions.push(v);
            }
        }
        mvcc_guard.clear();
    
        println!("commit: Sorting insertions and deletions...");
        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);
    
        println!("commit: Preparing for disk write...");
        let hdr_off = self.headers_offset;
        let row_sz = self.element_size as u64;
        let buf = vec![0u8; self.element_size];
    
        println!("commit: Writing insertions...");
        for (row_index, row_data) in insertions {
            let serialized = self.serialize_row(&row_data)?;
            let offset = hdr_off + row_index * row_sz;
            self.file.write_all_at(serialized.as_slice(), offset)?;
        }
        
        let mut graveyard = self.graveyard.lock().await;
        for del in &deletes {
            println!("{:?}",del);
            let from = hdr_off + del.0 * row_sz;
            self.file.write_all_at(&buf, from)?;
            total -= 1;
            graveyard.insert(del.0);
        }
        drop(graveyard);
        let new_len = hdr_off + total * row_sz;
        self.file.set_len(new_len)?;
        
        let mut txt_mvcc = self.text_mvcc.lock().await;

        for (i, txt) in  txt_mvcc.iter(){
            let path = format!("{}/rf/{}", self.location, i); 
            if !txt.0 {
                let mut file: std::fs::File = std::fs::File::create_new(&path)?;
                if let Err(e) = file.write_all(txt.1.as_bytes()){
                    return Err(gerr(&format!("Failed to write in text file: {}",e)))
                };
                
                let buffer = txt.1.as_bytes();
                let c_path = match CString::new(path).map_err(|e| e.to_string()){Ok(a) => a, Err(e) => return Err(gerr(&e))};
                    let result = unsafe {
                        write_data(buffer.as_ptr(), buffer.len(), c_path.as_ptr())
                    };

                    if result != 1 {
                        eprintln!("C write_data failed")
                    }
            } else if std::fs::exists(&path)?{
                fs::remove_file(&path).await?
            }
        
        }

        txt_mvcc.clear();
        txt_mvcc.shrink_to_fit();
    
        println!("commit: COMMIT SUCCESSFUL!");
        println!("commit: Starting to sync...");
                
        sync_file(&self.file).await?;
        println!("commit: Sync!");
    
        Ok(())
    }
    pub async fn get_rows(&self, index: (u64, u64)) -> Result<Vec<Vec<AlbaTypes>>, Error> {
        let mut lidx = index.1;
        let maxl = if self.len().await? > self.headers_offset {
            (self.len().await? - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        if lidx > maxl {
            lidx = maxl;
        }
    
        let idxs: (u64, u64) = (
            index.0 * self.element_size as u64 + self.headers_offset,
            lidx * self.element_size as u64 + self.headers_offset,
        );
        
        let buff_size: usize = (idxs.1 - idxs.0) as usize;
        let mut buff = vec![0u8; buff_size];
        self.file.read_exact_at(&mut buff, idxs.0)?;
    
        let v_c = self.mvcc.lock().await;
        let t_c = self.text_mvcc.lock().await;
    
        let mut result: Vec<Vec<AlbaTypes>> = Vec::new(); 
        for i in index.0..lidx {
            if let Some((deleted, row_data)) = v_c.get(&i) {
                if *deleted {
                    continue;
                }
    
                let mut row = row_data.clone();
                match v_c.get(&i){
                    Some(row_in_mvcc) => {
                        if !row_in_mvcc.0{
                            row = row_in_mvcc.1.clone()
                        }
                    },
                    None => {}
                }
                for value in row.iter_mut() {
                    if let AlbaTypes::Text(c) = value {
                        if let Some((deleted, new_text)) = t_c.get(c) {
                            if !*deleted {
                                *value = AlbaTypes::Text(new_text.to_string());
                            }
                        }else{
                            match fs::File::open(format!("{}/rf/{}",self.location,c)).await{
                                Ok(mut a) => {
                                    let mut bu : Vec<u8> = Vec::new();
                                    a.read_to_end(&mut bu).await?;
                                    *value = AlbaTypes::Text(match String::from_utf8(bu){Ok(a) => a,Err(e)=>return Err(gerr(&e.to_string()))})
                                },
                                Err(e) => {return Err(e)}
                            }
                        }
                    }
                }
    
                result.push(row); 
            } else {
                let start = ((i - index.0) * self.element_size as u64) as usize;
                let end = start + self.element_size;
                let row_buf = &buff[start..end];
                let row = self.deserialize_row(row_buf.to_vec()).await?;
                result.push(row);
            }
        }
    
        Ok(result)
    }
    
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(self.element_size);
    
        for (item, ty) in row.iter().zip(self.columns.iter()) {
            match (item, ty) {
                (AlbaTypes::Bigint(v), AlbaTypes::Bigint(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Int(v), AlbaTypes::Int(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Float(v), AlbaTypes::Float(_)) => {
                    buffer.extend_from_slice(&v.to_be_bytes());
                },
                (AlbaTypes::Bool(v), AlbaTypes::Bool(_)) => {
                    buffer.push(if *v { 1 } else { 0 });
                },
                (AlbaTypes::Text(s), AlbaTypes::Text(_)) => {
                    let mut bytes = s.as_bytes().to_vec();
                    bytes.resize(self.str_size, 0);
                    buffer.extend_from_slice(&bytes);
                },
                (AlbaTypes::NONE, AlbaTypes::NONE) => {
                },
                _ => {
                    return Err(gerr("Mismatched types during serialization."));
                }
            }
        }
    
        Ok(buffer)
    }
    
    async fn deserialize_row(&self, buf: Vec<u8>) -> Result<Vec<AlbaTypes>,Error> {
        let mut index = 0;
        let mut value : Vec<AlbaTypes> = Vec::new();
        for item in self.columns.iter(){
            match item {
                AlbaTypes::Bigint(_) => {
                    let size = size_of::<i64>();
                    let paydo : [u8; 8] = match buf[index..index+size].try_into() { Ok(a) => a, Err(e) => return Err(gerr(&e.to_string()))};
                    index += size;
                    value.push(AlbaTypes::Bigint(i64::from_be_bytes(paydo)))
                },
                AlbaTypes::Int(_) => {
                    let size = size_of::<i32>();
                    let paydo : [u8; 4] = match buf[index..index+size].try_into() { Ok(a) => a, Err(e) => return Err(gerr(&e.to_string()))};
                    index += size;
                    value.push(AlbaTypes::Int(i32::from_be_bytes(paydo)))
                },
                AlbaTypes::Float(_) => {
                    let size = size_of::<f64>();
                    let paydo : [u8; 8] = match buf[index..index+size].try_into() { Ok(a) => a, Err(e) => return Err(gerr(&e.to_string()))};
                    index += size;
                    value.push(AlbaTypes::Float(f64::from_be_bytes(paydo)))
                },
                AlbaTypes::Bool(_) => {
                    let size = size_of::<bool>();
                    let paydo : [u8; 1] = match buf[index..index+size].try_into() { Ok(a) => a, Err(e) => return Err(gerr(&e.to_string()))};
                    index += size;
                    let bool_value = paydo[0] != 0;
                    value.push(AlbaTypes::Bool(bool_value))
                },
                AlbaTypes::Text(_) => {
                    let size = self.str_size;
                    let paydo : Vec<u8> = match buf[index..index+size].try_into() { Ok(a) => a, Err(e) => return Err(gerr(&e.to_string()))};
                    index += size;
                    let trimmed = paydo.iter()
                        .take_while(|&&c| c != 0) 
                        .cloned()
                        .collect::<Vec<u8>>();

                    let str_id = match String::from_utf8(trimmed) {
                        Ok(s) => s,
                        Err(e) => return Err(gerr(&format!("Erro ao converter bytes para String: {}", e))),
                    };
                    let mut file = match try_open_file(&format!("{}/rf/{}",self.location,str_id)).await? {
                        Some(a) => a,
                        None => {value.push(AlbaTypes::Text(str_id)); continue},
                    };
                    let mut buffer : Vec<u8> = Vec::with_capacity(100);
                    match file.read_to_end(&mut buffer).await{
                        Ok(_) => {
                            value.push(AlbaTypes::Text(
                                match String::from_utf8(buffer){
                                    Ok(str) => str,
                                    Err(e) => {
                                        return Err(gerr(&e.to_string()))
                                    }
                                }
                            ));
                        },
                        Err(e) => {
                            eprintln!(r#"failed to search for a compatible text file on the "rf" dir, using the id instead. Err: {}"#,e);
                            value.push(AlbaTypes::Text(str_id));
                        }
                    };
                },
                AlbaTypes::NONE => {
                    value.push(AlbaTypes::NONE);
                }
            }
        }
        Ok(value)
    }
}