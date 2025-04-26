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

fn serialize_closed_string(item : &AlbaTypes,s : &String,buffer : &mut Vec<u8>){
    let mut bytes = Vec::with_capacity(item.size());
    let mut str_bytes = s.as_bytes().to_vec();
    let str_length = str_bytes.len().to_le_bytes().to_vec();
    str_bytes.truncate(item.size()-size_of::<usize>());
    bytes.extend_from_slice(&str_length);
    bytes.extend_from_slice(&str_bytes);
    bytes.resize(item.size(),0);
    buffer.extend_from_slice(&bytes);
}
fn serialize_closed_blob(item : &AlbaTypes,blob : &mut Vec<u8>,buffer : &mut Vec<u8>){
    let mut bytes: Vec<u8> = Vec::with_capacity(item.size());
    let blob_length: Vec<u8> = blob.len().to_le_bytes().to_vec();
    blob.truncate(item.size()-size_of::<usize>());
    bytes.extend_from_slice(&blob_length);
    bytes.extend_from_slice(blob);
    bytes.resize(item.size(),0);
    buffer.extend_from_slice(&bytes);
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
fn handle_fixed_string(buf: &[u8],index: &mut usize,instance_size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = &buf[*index..*index+instance_size];
    let mut size : [u8;8] = [0u8;8];
    size.clone_from_slice(bytes); 
    let string_length = usize::from_le_bytes(size);
    let string_bytes = &bytes[8..string_length];
    
    *index += instance_size;
    let trimmed: Vec<u8> = string_bytes.iter()
        .take_while(|&&b| b != 0)
        .cloned()
        .collect();
    let s = String::from_utf8(trimmed)
        .map_err(|e| gerr(&format!("String decoding failed: {}", e)))?;
    
    match instance_size {
        10 => values.push(AlbaTypes::NanoString(s)),
        100 => values.push(AlbaTypes::SmallString(s)),
        500 => values.push(AlbaTypes::MediumString(s)),
        2000 => values.push(AlbaTypes::BigString(s)),
        3000 => values.push(AlbaTypes::LargeString(s)),
        _ => unreachable!(),
    }
    Ok(())
}

fn handle_bytes(buf: &[u8],index: &mut usize,size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = buf[*index..*index+size].to_vec();
    let mut blob_size : [u8;8] = [0u8;8];
    blob_size.clone_from_slice(&bytes); 
    let blob_length = usize::from_le_bytes(blob_size);
    let blob = bytes[8..blob_length].to_vec();
    *index += size;
    
    match size {
        10 => values.push(AlbaTypes::NanoBytes(blob)),
        1000 => values.push(AlbaTypes::SmallBytes(blob)),
        10_000 => values.push(AlbaTypes::MediumBytes(blob)),
        100_000 => values.push(AlbaTypes::BigSBytes(blob)),
        1_000_000 => values.push(AlbaTypes::LargeBytes(blob)),
        _ => unreachable!(),
    }
    Ok(())
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
                (AlbaTypes::Char(c), AlbaTypes::Char(_)) => {
                    let code = *c as u32;
                    buffer.extend_from_slice(&code.to_le_bytes());
                },
                (AlbaTypes::Text(s), AlbaTypes::Text(_)) => {
                    let mut bytes = s.as_bytes().to_vec();
                    bytes.resize(self.str_size, 0);
                    buffer.extend_from_slice(&bytes);
                },
                (AlbaTypes::NanoString(s), AlbaTypes::NanoString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::SmallString(s), AlbaTypes::SmallString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::MediumString(s), AlbaTypes::MediumString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::BigString(s), AlbaTypes::BigString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::LargeString(s), AlbaTypes::LargeString(_)) => {
                    serialize_closed_string(item,s,&mut buffer);
                },
                (AlbaTypes::NanoBytes(v ), AlbaTypes::NanoBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::SmallBytes(v), AlbaTypes::SmallBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::MediumBytes(v), AlbaTypes::MediumBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::BigSBytes(v), AlbaTypes::BigSBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::LargeBytes(v), AlbaTypes::LargeBytes(_)) => {
                    let mut blob: Vec<u8> = v.to_owned();
                    serialize_closed_blob(item, &mut blob, &mut buffer);
                },
                (AlbaTypes::NONE, AlbaTypes::NONE) => {
                    let size = item.size();
                    buffer.extend(vec![0u8; size]);
                },
                _ => return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Type mismatch between value {:?} and column type {:?}", item, ty)
                )),
            }
        }
    
        // Validate buffer size matches element_size
        if buffer.len() != self.element_size {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Serialized size mismatch: expected {}, got {}",
                    self.element_size,
                    buffer.len()
                )
            ));
        }
    
        Ok(buffer)
    }
    async fn deserialize_row(&self, buf: Vec<u8>) -> Result<Vec<AlbaTypes>, Error> {
        let mut index = 0;
        let mut values = Vec::new();
    
        for column_type in &self.columns {
            match column_type {
                // Primitive types
                AlbaTypes::Bigint(_) => {
                    let size = std::mem::size_of::<i64>();
                    let bytes: [u8; 8] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read bigint: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Bigint(i64::from_be_bytes(bytes)));
                },
                
                AlbaTypes::Int(_) => {
                    let size = std::mem::size_of::<i32>();
                    let bytes: [u8; 4] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read int: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Int(i32::from_be_bytes(bytes)));
                },
    
                AlbaTypes::Float(_) => {
                    let size = std::mem::size_of::<f64>();
                    let bytes: [u8; 8] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read float: {}", e)))?;
                    index += size;
                    values.push(AlbaTypes::Float(f64::from_be_bytes(bytes)));
                },
    
                AlbaTypes::Bool(_) => {
                    let size = std::mem::size_of::<bool>();
                    let byte = *buf.get(index).ok_or(gerr("Incomplete bool data"))?;
                    index += size;
                    values.push(AlbaTypes::Bool(byte != 0));
                },
    
                AlbaTypes::Char(_) => {
                    let size = std::mem::size_of::<u32>();
                    let bytes: [u8; 4] = buf[index..index+size].try_into()
                        .map_err(|e| gerr(&format!("Failed to read char: {}", e)))?;
                    index += size;
                    let code = u32::from_le_bytes(bytes);
                    values.push(AlbaTypes::Char(match char::from_u32(code){
                        Some(a) => a,
                        None => {
                            return Err(gerr("Invalid Unicode scalar value"))
                        }
                    }));
                },
    
                // Text types
                AlbaTypes::Text(_) => {
                    let size = self.str_size;
                    let bytes = buf[index..index+size].to_vec();
                    index += size;
                    let trimmed: Vec<u8> = bytes.into_iter()
                        .take_while(|&b| b != 0)
                        .collect();
                    let str_id = String::from_utf8(trimmed)
                        .map_err(|e| gerr(&format!("Text decoding failed: {}", e)))?;
    
                    // Check external text storage
                    let mut file = match try_open_file(&format!("{}/rf/{}", self.location, str_id)).await? {
                        Some(f) => f,
                        None => {
                            values.push(AlbaTypes::Text(str_id));
                            continue;
                        }
                    };
    
                    let mut content = Vec::new();
                    file.read_to_end(&mut content).await?;
                    values.push(AlbaTypes::Text(String::from_utf8(content)
                        .map_err(|e| gerr(&format!("Text file corrupt: {}", e)))?));
                },
    
                // Fixed-size string types
                AlbaTypes::NanoString(_) => handle_fixed_string(&buf, &mut index, 10, &mut values)?,
                AlbaTypes::SmallString(_) => handle_fixed_string(&buf, &mut index, 100, &mut values)?,
                AlbaTypes::MediumString(_) => handle_fixed_string(&buf, &mut index, 500, &mut values)?,
                AlbaTypes::BigString(_) => handle_fixed_string(&buf, &mut index, 2000, &mut values)?,
                AlbaTypes::LargeString(_) => handle_fixed_string(&buf, &mut index, 3000, &mut values)?,
    
                // Byte array types
                AlbaTypes::NanoBytes(_) => handle_bytes(&buf, &mut index, 10, &mut values)?,
                AlbaTypes::SmallBytes(_) => handle_bytes(&buf, &mut index, 1000, &mut values)?,
                AlbaTypes::MediumBytes(_) => handle_bytes(&buf, &mut index, 10_000, &mut values)?,
                AlbaTypes::BigSBytes(_) => handle_bytes(&buf, &mut index, 100_000, &mut values)?,
                AlbaTypes::LargeBytes(_) => handle_bytes(&buf, &mut index, 1_000_000, &mut values)?,
    
                // Null handling
                AlbaTypes::NONE => {
                    values.push(AlbaTypes::NONE);
                }
            }
        }
    
        Ok(values)
    }
    
}