
use std::{collections::{BTreeMap, BTreeSet, HashMap}, fs::{self, File, OpenOptions}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, ErrorKind, Read, Write}, os::{fd::AsRawFd, unix::fs::{FileExt, MetadataExt}}, sync::{Arc,Mutex}, thread};
use crate::{alba_types::{into_schema,AlbaTypes}, database::{batch_write_data, WriteEntry}, gerr, indexing::IndexingHashmap as IndexingHashMap };
use bitvec::prelude::*;
pub const MAX_GRAVEYARD_LENGTH_IN_MEMORY : usize = 1250;

type MvccType = Arc<Mutex<(BTreeMap<u64,(MvccState,Vec<AlbaTypes>)>,HashMap<String,(bool,String)>)>>;

#[derive(Debug)]
pub struct MvccRecord(Arc<Mutex<File>>);
impl MvccRecord{
    fn new(name : String) -> Result<Self,Error>{
        let file = OpenOptions::new().read(true).write(true).append(true).create(!fs::exists(&name)?).open(name)?;
        Ok(MvccRecord(Arc::new(Mutex::new(file))))
    }
     fn put(&mut self,bytes : Vec<u8>) -> Result<(),Error>{
        let reference = self.0.clone();
        let _ = thread::spawn(move || {
            let mut bibi = reference.lock().unwrap();
            let e0 = bibi.write_all(&bytes);
            let e1 = bibi.sync_all();
            if let Err(e) = e0{eprintln!("ERROR{:?}",e)};
            if let Err(e) = e1{eprintln!("ERROR {:?}",e)};
        });
        Ok(())
    }
     fn yield_(&mut self) -> Result<Vec<u8>,Error>{
        let mut buffer = Vec::new();
        self.0.lock().unwrap().read_to_end(&mut buffer)?;
        Ok(buffer)
    }
     fn clear(&mut self) -> Result<(),Error> {
        self.0.lock().unwrap().set_len(0)?; self.sync()?;Ok(())
    }
     fn sync(&mut self) -> Result<(),Error>{
        let reference = self.0.clone();
        thread::spawn( move ||{    
            let n = reference.lock().unwrap();
            let _ = n.sync_all();
        });
        Ok(())
    }
}

pub struct Container{
    pub file : Arc<Mutex<std::fs::File>>,
    pub element_size : usize,
    pub headers : Vec<(String,AlbaTypes)>,
    pub mvcc : MvccType,
    pub headers_offset : u64,
    pub graveyard : Arc<Mutex<BTreeSet<u64>>>,
    pub index_map : Arc<Mutex<IndexingHashMap>>,
    pub mvcc_record : Arc<Mutex<MvccRecord>>

}
#[derive(Debug,Copy,Clone)]
pub enum MvccState{
    Delete,
    Insert,
    Edit
}


pub fn get_index(i: AlbaTypes) -> u64 {
    let mut hasher = DefaultHasher::new();
    match i {
        AlbaTypes::Int(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Bigint(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Float(b) => {
            b.to_bits().hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Bool(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Char(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::UInt(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::UBigint(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NanoInt(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::UNanoInt(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Short(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::UShort(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::HugeInt(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::UHugeInt(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
        AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) |
        AlbaTypes::LightPassword(b) | AlbaTypes::MediumPassword(b) | AlbaTypes::HeavyPassword(b) |
        AlbaTypes::Slice4(b) | AlbaTypes::Slice3(b) | AlbaTypes::Slice2(b) |
        AlbaTypes::Slice1(b) | AlbaTypes::Slice0(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NanoString(b) | AlbaTypes::SmallString(b) | AlbaTypes::MediumString(b) |
        AlbaTypes::BigString(b) | AlbaTypes::LargeString(b) | AlbaTypes::Text(b) |
        AlbaTypes::Email(b) => {
            b.hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::Geo((lat, lon)) => {
            lat.to_bits().hash(&mut hasher);
            lon.to_bits().hash(&mut hasher);
            hasher.finish()
        },
        AlbaTypes::NONE => {
            0u64.hash(&mut hasher);
            hasher.finish()
        },
    }
}

impl Container {
    pub  fn new(path : &str,element_size : usize, columns : Vec<AlbaTypes>,headers_offset : u64,column_names : Vec<String>) -> Result<Arc<Mutex<Self>>,Error> {
        let mut  headers = Vec::new();
        for index in 0..((columns.len()+column_names.len())/2){
            let name = match column_names.get(index){
                Some(nm) => nm,
                None => {
                    return Err(gerr("Failed to create container, the size of column types and names must be equal. And this error is a consequence of that property not being respected."))
                } 
            };
            let value = match columns.get(index){
                Some(vl) => vl,
                None => {
                    return Err(gerr("Failed to create container, the size of column types and names must be equal. And this error is a consequence of that property not being respected."))
                }
            };
            if name.is_empty(){
                continue;
            }
            if let AlbaTypes::NONE = value{
                continue
            }
            headers.push((name.to_owned(), value.to_owned()));
        }
        let regen_hm = !fs::exists(format!("{}.hashmap",path))? && fs::exists(path.to_string())?;
        let file =std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
        let mut hash_header = HashMap::new();
        for i in headers.iter(){
            hash_header.insert(i.0.clone(),i.1.clone());
        }
        let container = Arc::new(Mutex::new(Container{
            element_size,
            mvcc: Arc::new(Mutex::new((BTreeMap::new(),HashMap::new()))),
            headers_offset,
            headers,
            graveyard: Arc::new(Mutex::new(BTreeSet::new())),
            mvcc_record: Arc::new(Mutex::new(MvccRecord::new(format!("{}.mr",path))?)),
            index_map: Arc::new(Mutex::new(IndexingHashMap::new(path.to_string())?)),
            file: Arc::new(Mutex::new(file))
        }));
        let mut c = container.lock().unwrap();
        c.load_mvcc()?;
        if regen_hm{c.build_hm()?};
        drop(c);
        Ok(container)
    }
    
}
impl Container{
    pub  fn build_hm(&mut self) -> Result<(),Error>{
        let file = self.file.lock().unwrap();
        let element_size = self.element_size;
        let headers_offset = self.headers_offset;
        let mut b = self.index_map.lock().unwrap();
        let empty = vec![255u8;element_size];
                    
                        let total_rows = (file.metadata()?.len() as usize - headers_offset as usize)/element_size;
                        let rows_per_it = ((4096*5) / element_size).max(1);
                        let chunk_size = (rows_per_it * element_size).min(total_rows*element_size);
                        let count_its = (total_rows / rows_per_it).max(1);
                        let mut v = Vec::new(); 
                        for i in 0..count_its{ 
                            let mut buffer = vec![0u8;chunk_size];
                            let file_offset = headers_offset + (i * chunk_size) as u64;
                            file.read_exact_at(&mut buffer, file_offset).unwrap();

                            for (j,row_bin) in buffer.chunks_exact(element_size).enumerate(){
            
                                let offset_in_file = headers_offset as usize+i*chunk_size+j*element_size;
                                if row_bin == empty{
                                    continue;
                                }
                            let bare_row = self.deserialize_row(row_bin)?;
                            v.push((true,get_index(bare_row[0].clone()), offset_in_file as u64));                          
                            
                            }
                        }       
        b.write(v)?;
        Ok(())
    }
    pub fn column_names(&self) -> Vec<String>{
        self.headers.iter().map(|v|v.0.to_string()).collect()
    }
}
/* 
fn handle_fixed_string(buf: &[u8],index: &mut usize,instance_size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = &buf[*index..*index+instance_size];
    let mut size_bytes : [u8;8] = [0u8;8];
    size_bytes.clone_from_slice(&bytes[..8]); 

    let string_length = u64::from_be_bytes(size_bytes) as usize;

    if 8 + string_length > instance_size {
        return Err(gerr(&format!("Invalid string length in data, expected at most {} but got {}", instance_size - 8, string_length)));
    }

    let string_bytes = &bytes[8..(8 + string_length)];
    
    *index += instance_size;
    let s = String::from_utf8_lossy(string_bytes).to_string();
    
    match instance_size {
        18 => values.push(AlbaTypes::NanoString(s)),
        108 => values.push(AlbaTypes::SmallString(s)),
        508 => values.push(AlbaTypes::MediumString(s)),
        2_008 => values.push(AlbaTypes::BigString(s)),
        3_008 => values.push(AlbaTypes::LargeString(s)),
        _ => unreachable!(),
    }
    Ok(())
} */

/* fn handle_bytes(buf: &[u8],index: &mut usize,size: usize,values: &mut Vec<AlbaTypes>) -> Result<(), Error> {
    let bytes = buf[*index..*index+size].to_vec();
    let mut blob_size : [u8;8] = [0u8;8];
    blob_size.clone_from_slice(&bytes[..8]); 
    let blob_length = u64::from_le_bytes(blob_size);
    let blob : Vec<u8> = if blob_length > 0 {
        if blob_length >= bytes.len() as u64{
            bytes[8..].to_vec()
        }else{
           bytes[8..(8+blob_length as usize)].to_vec() 
        }
        
    }else{
        
        let blob = Vec::new();
        match size {
            18 => values.push(AlbaTypes::NanoBytes(blob)),
            1008 => values.push(AlbaTypes::SmallBytes(blob)),
            10_008 => values.push(AlbaTypes::MediumBytes(blob)),
            100_008 => values.push(AlbaTypes::BigSBytes(blob)),
            1_000_008 => values.push(AlbaTypes::LargeBytes(blob)),
            _ => unreachable!(),
        }
        return Ok(())
    };

    *index += size;
    
    match size {
        18 => values.push(AlbaTypes::NanoBytes(blob)),
        1008 => values.push(AlbaTypes::SmallBytes(blob)),
        10_008 => values.push(AlbaTypes::MediumBytes(blob)),
        100_008 => values.push(AlbaTypes::BigSBytes(blob)),
        1_000_008 => values.push(AlbaTypes::LargeBytes(blob)),
        _ => unreachable!(),
    }
    Ok(())
} */
const VACCUM_SIZE : u64 = 4194304;
const MAX_VACUUM_LENGTH : usize = 625000;
impl Container{
    pub  fn get_next_addr(&self) -> Result<u64, Error> {
        let mv = self.mvcc.lock().unwrap();
        let mut gy = self.graveyard.lock().unwrap();
        if let Some(s) = gy.pop_first(){
            return Ok(s)
        }
        let m = mv.0.keys().max();
        let size = self.file.lock().unwrap().metadata()?.size();
        if let Some(m) = m{
            return Ok(*m+self.element_size as u64)
        }
        Ok(size)
    }
    pub  fn vacuum(&mut self) -> Result<(),Error> {
        self.graveyard.lock().unwrap().clear();
        let mut mvcc = self.mvcc.lock().unwrap();
        mvcc.0.clear(); mvcc.1.clear();

        let fi = self.file.lock().unwrap();
        let element_size = self.element_size as u64;
        let length = (fi.metadata()?.size()-self.headers_offset)/element_size;

        if length == 0{
            return Ok(());
        }

        let mut map = bitvec!();
        let mut readen = 0u64;
        let chunk_size : u64 = VACCUM_SIZE/self.element_size as u64;
        let empty = vec![255u8;self.element_size];
        let mut pairs : Vec<(u64,u64)> = Vec::new();
        
        for _ in 0..(length/chunk_size).max(1){
            let etr = (length - readen).min(chunk_size) as u64; //elements to read
            let offset : u64 = self.headers_offset + (readen * element_size);
            readen += etr;
            let mut buffer = vec![0u8;(element_size*etr) as usize];
            fi.read_exact_at(&mut buffer, offset)?;
            for j in buffer.chunks_exact(self.element_size){
                map.push(j != empty)
            }
            drop(buffer); 
        }
        map.shrink_to_fit();
        let mut cursor : usize = 0;
        let mut back_c : usize = map.len()-1;
        let mut run = false; // false ~ forward | true ~ backwards
        
        while cursor < back_c{
            if run == false{
                if let Some(val) = map.get(cursor){
                    if !*val{
                        run = true;
                    }else{
                        cursor += 1;
                    }
                }
            }else if run == true{
                if let Some(val) = map.get(back_c){
                    if *val{
                        pairs.push((cursor as u64, back_c as u64));
                        if pairs.len() > MAX_VACUUM_LENGTH{
                            break;
                        }
                        run = false;
                    }else{
                        back_c = back_c.saturating_sub(1);
                    }
                }
            }
        }
        let mut indexing = self.index_map.lock().unwrap();
        let mut dead_v = Vec::new();
        for (dead, alive) in pairs{
            let mut buffer = vec![0u8;self.element_size];
            let alive_offset = (alive*element_size) + self.headers_offset;
            fi.read_exact_at(&mut buffer,alive_offset)?;
            let row_pk = self.deserialize_row(&buffer)?[0].clone();
            let dead_offset = (dead*element_size)+ self.headers_offset;
            fi.write_all_at(&buffer, dead_offset)?;
            fi.write_all_at(&vec![255u8;self.element_size], alive_offset)?;
            dead_v.push((true,get_index(row_pk),dead_offset));
            map.swap(dead as usize, alive as usize);
        }
        std::thread::scope(|_| {
            let _ = fi.sync_all();
        });
        for dead_v in dead_v.chunks(30000){
            indexing.write(dead_v.to_vec())?;
        }
            
        let mut rows_to_remove = 0u64;
        let mut index = map.len()-1;
        while let Some(val) = map.get(index){
                if *val{break;}else{rows_to_remove+=1;if index==0{break;};index-=1;}
        }

        if rows_to_remove > 0{
            let new_len = fi.metadata()?.size().saturating_sub(rows_to_remove*element_size).max(self.headers_offset);
            fi.set_len(new_len)?;
            fi.sync_all()?;
        }
        
        Ok(())
    }
    pub  fn load_mvcc(&mut self) -> Result<(),Error>{
        let mut mvcc_record = self.mvcc_record.lock().unwrap();
        let b = mvcc_record.yield_()?;
        let mut mvcc = self.mvcc.lock().unwrap();
        for i in b.chunks_exact(1 + self.element_size){
            let s = match i[0] {0 => MvccState::Insert,1 => MvccState::Edit,_ => MvccState::Delete};
            let row = self.deserialize_row(&i[1..self.element_size])?;
            let key = {
                let mut load = [0u8;8];
                load[..].copy_from_slice(&i[self.element_size..]);
                u64::from_le_bytes(load)
            };
            mvcc.0.insert(key, (s,row));
        }
        Ok(())
    }
    pub  fn record_mvcc(&mut self, key : u64, data : Vec<AlbaTypes>,state: MvccState) -> Result<(),Error>{
        let mut b = Vec::new();
        b.push(match state{MvccState::Delete => 2, MvccState::Insert => 0, MvccState::Edit => 1});
        b.extend_from_slice(&self.serialize_row(&data)?);
        b.extend_from_slice(&key.to_le_bytes());
        let mut l = self.mvcc_record.lock().unwrap();
        l.put(b)?;
        Ok(())
    }
    pub  fn push_row(&mut self, data : Vec<AlbaTypes>) -> Result<(),Error>{
        let mut indexing = self.index_map.lock().unwrap();
        let i = get_index(data[0].clone());
        if indexing.get(vec![i])?.len() > 0{
            return Err(Error::new(ErrorKind::AddrInUse,"This primary key is in use, they must be always unique."))
        }
        drop(indexing);
        let ind = self.get_next_addr()?;
        let mut mvcc_guard = self.mvcc.lock().unwrap();
        //println!("PUSH_ROW - OFFSET : {}",ind);
        let d = data.clone();
        mvcc_guard.0.insert(ind, (MvccState::Insert,data));
        drop(mvcc_guard);
        let _ = self.record_mvcc(ind, d, MvccState::Insert);
        Ok(())
    }
    pub  fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.lock().unwrap();
        mvcc_guard.0.clear();
        mvcc_guard.1.clear();
        let mut mvcc_rec = self.mvcc_record.lock().unwrap();
        let _ = mvcc_rec.clear();
        drop(mvcc_guard);
        Ok(())
    }
    pub  fn commit(&mut self) -> Result<(), Error> {
        //let mut virtual_ward : HashMap<usize, DataReference> = HashMap::new();
        let mut mvcc = self.mvcc.lock().unwrap();
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut edits:Vec<(u64,Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc.0.iter() {
            
            let v = (*index, value.1.clone());
            match value.0{
                MvccState::Delete => deletes.push(v),
                MvccState::Insert => insertions.push(v),
                MvccState::Edit => edits.push(v)
            }
        }
        mvcc.0.clear();
        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);

        let mut writting : Vec<(u64,Vec<u8>)> = Vec::new();
        let schema = self.columns();
        //println!("schema {:?}",schema);
        let mut index_batch : Vec<(AlbaTypes,u64)> = Vec::new();
        for (row_index, mut row_data) in insertions {
            //println!("\nrow_data: {:?}\n",row_data);
            into_schema(&mut row_data, &schema)?;
            let serialized = self.serialize_row(&row_data).unwrap();
            index_batch.push((row_data[0].clone(),row_index));
            let offset = row_index;
            writting.push((offset,serialized));
        }
        let mut indexing = self.index_map.lock().unwrap();
        let mut indexing_writes = Vec::new();
        for (row_index, mut row_data) in edits{
            //println!("\nrow_data: {:?}\n",row_data);
            into_schema(&mut row_data, &schema)?;
            let serialized = self.serialize_row(&row_data).unwrap();
            let key = get_index(row_data[0].clone());
            indexing_writes.push((false,key,0));
            index_batch.push((row_data[0].clone(),row_index));
            let offset = row_index;
            writting.push((offset,serialized)); 
        }

        drop(schema);


        let buf = vec![255u8; self.element_size];
        let mut gy = self.graveyard.lock().unwrap();
        let mut gyl = gy.len();
        for del in &deletes {
            let offset = del.0;
            if gyl < MAX_GRAVEYARD_LENGTH_IN_MEMORY{
                gy.insert(offset);
                gyl += 1;
            }
            let key = get_index(del.1[0].clone());

            indexing_writes.push((false,key,0));
            writting.push((offset,buf.clone()));
        }
       
        // if let Some(s) = STRIX.get(){
        //     let mut l = s.lock().unwrap();
        //     l.wards.push(Mutex::new((std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path)?,virtual_ward)));
        // }
        
        let mut l = Vec::new();
        for i in writting{
            let len = i.1.len();
            l.push(WriteEntry{
                buffer: Arc::new(i.1),
                length: len,
                offset: i.0 as i64
            });
        }
;
        let f = self.file.lock().unwrap();
        let c = f.as_raw_fd();

        for (alb,off) in index_batch{
            let key = get_index(alb);
            indexing_writes.push((true,key,off));    
        };
        for sl in indexing_writes.chunks(30000){
            indexing.write(sl.to_vec())?;
        }

        for l in l.chunks(30000){
            let l_1 = l.len();
            batch_write_data(l.to_vec(), l_1, c);
        }

        
        
        let mut mvcc_record = self.mvcc_record.lock().unwrap();
        mvcc_record.clear()?;
        mvcc.1.clear(); mvcc.0.clear(); 
        Ok(())
    }
    
    pub fn columns(&self) -> Vec<AlbaTypes>{
        self.headers.iter().map(|v|v.1.clone()).collect()
    }
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        for i in row{
            i.serialize_into(&mut buffer);
        }
        //println!("data: {:?}",buffer);
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

    pub fn deserialize_row(&self, buf: &[u8]) -> Result<Vec<AlbaTypes>, Error> {
        let mut index = 0;
        let mut values = Vec::new();

        for column_type in &self.columns() {
            let size = column_type.size();
            if index + size > buf.len() {
                return Err(gerr("Buffer too short for row"));
            }
            let field_buf = &buf[index..index + size];
            index += size;

            let value = match column_type {
                AlbaTypes::NONE => AlbaTypes::NONE,

                AlbaTypes::Bool(_) => AlbaTypes::Bool(field_buf[0] != 0),

                AlbaTypes::Char(_) => {
                    let bytes: [u8; 4] = field_buf[0..4].try_into().map_err(|e| gerr(&format!("Failed to read char: {}", e)))?;
                    let code = u32::from_le_bytes(bytes);
                    AlbaTypes::Char(char::from_u32(code).ok_or(gerr("Invalid char code"))?)
                }

                AlbaTypes::NanoInt(_) => {
                    let bytes: [u8; 1] = field_buf[0..1].try_into().unwrap();
                    AlbaTypes::NanoInt(i8::from_le_bytes(bytes))
                }

                AlbaTypes::UNanoInt(_) => {
                    let bytes: [u8; 1] = field_buf[0..1].try_into().unwrap();
                    AlbaTypes::UNanoInt(u8::from_le_bytes(bytes))
                }

                AlbaTypes::Short(_) => {
                    let bytes: [u8; 2] = field_buf[0..2].try_into().map_err(|e| gerr(&format!("Failed to read short: {}", e)))?;
                    AlbaTypes::Short(i16::from_le_bytes(bytes))
                }

                AlbaTypes::UShort(_) => {
                    let bytes: [u8; 2] = field_buf[0..2].try_into().map_err(|e| gerr(&format!("Failed to read ushort: {}", e)))?;
                    AlbaTypes::UShort(u16::from_le_bytes(bytes))
                }

                AlbaTypes::Int(_) => {
                    let bytes: [u8; 4] = field_buf[0..4].try_into().map_err(|e| gerr(&format!("Failed to read int: {}", e)))?;
                    AlbaTypes::Int(i32::from_le_bytes(bytes))
                }

                AlbaTypes::UInt(_) => {
                    let bytes: [u8; 4] = field_buf[0..4].try_into().map_err(|e| gerr(&format!("Failed to read uint: {}", e)))?;
                    AlbaTypes::UInt(u32::from_le_bytes(bytes))
                }

                AlbaTypes::Bigint(_) => {
                    let bytes: [u8; 8] = field_buf[0..8].try_into().map_err(|e| gerr(&format!("Failed to read bigint: {}", e)))?;
                    AlbaTypes::Bigint(i64::from_le_bytes(bytes))
                }

                AlbaTypes::UBigint(_) => {
                    let bytes: [u8; 8] = field_buf[0..8].try_into().map_err(|e| gerr(&format!("Failed to read ubigint: {}", e)))?;
                    AlbaTypes::UBigint(u64::from_le_bytes(bytes))
                }

                AlbaTypes::Float(_) => {
                    let bytes: [u8; 8] = field_buf[0..8].try_into().map_err(|e| gerr(&format!("Failed to read float: {}", e)))?;
                    AlbaTypes::Float(f64::from_le_bytes(bytes))
                }

                AlbaTypes::HugeInt(_) => {
                    let bytes: [u8; 16] = field_buf[0..16].try_into().map_err(|e| gerr(&format!("Failed to read hugeint: {}", e)))?;
                    AlbaTypes::HugeInt(i128::from_le_bytes(bytes))
                }

                AlbaTypes::UHugeInt(_) => {
                    let bytes: [u8; 16] = field_buf[0..16].try_into().map_err(|e| gerr(&format!("Failed to read uhugeint: {}", e)))?;
                    AlbaTypes::UHugeInt(u128::from_le_bytes(bytes))
                }

                AlbaTypes::Geo(_) => {
                    let lat_bytes: [u8; 8] = field_buf[0..8].try_into().map_err(|e| gerr(&format!("Failed to read geo lat: {}", e)))?;
                    let lon_bytes: [u8; 8] = field_buf[8..16].try_into().map_err(|e| gerr(&format!("Failed to read geo lon: {}", e)))?;
                    AlbaTypes::Geo((f64::from_le_bytes(lat_bytes), f64::from_le_bytes(lon_bytes)))
                }

                AlbaTypes::Text(_) => {
                    let s = String::from_utf8_lossy(field_buf).trim_end_matches('\0').to_string();
                    AlbaTypes::Text(s)
                }

                AlbaTypes::NanoString(_) | AlbaTypes::SmallString(_) | AlbaTypes::MediumString(_) | AlbaTypes::BigString(_) | AlbaTypes::LargeString(_) => {
                    let len_bytes: [u8; 8] = field_buf[0..8].try_into().unwrap();
                    let len = usize::from_be_bytes(len_bytes);
                    let data_end = 8 + len.min(size - 8);
                    let s = String::from_utf8(field_buf[8..data_end].to_vec()).map_err(|e| gerr(&format!("Invalid string: {}", e)))?;
                    match column_type {
                        AlbaTypes::NanoString(_) => AlbaTypes::NanoString(s),
                        AlbaTypes::SmallString(_) => AlbaTypes::SmallString(s),
                        AlbaTypes::MediumString(_) => AlbaTypes::MediumString(s),
                        AlbaTypes::BigString(_) => AlbaTypes::BigString(s),
                        AlbaTypes::LargeString(_) => AlbaTypes::LargeString(s),
                        _ => unreachable!(),
                    }
                }

                AlbaTypes::Email(_) => {
                    let s = String::from_utf8_lossy(field_buf).trim_end_matches('\0').to_string();
                    AlbaTypes::Email(s)
                }

                AlbaTypes::NanoBytes(_) | AlbaTypes::SmallBytes(_) | AlbaTypes::MediumBytes(_) | AlbaTypes::BigSBytes(_) | AlbaTypes::LargeBytes(_) => {
                    let len_bytes: [u8; 8] = field_buf[0..8].try_into().unwrap();
                    let len = usize::from_le_bytes(len_bytes);
                    let data_end = 8 + len.min(size - 8);
                    let b = field_buf[8..data_end].to_vec();
                    match column_type {
                        AlbaTypes::NanoBytes(_) => AlbaTypes::NanoBytes(b),
                        AlbaTypes::SmallBytes(_) => AlbaTypes::SmallBytes(b),
                        AlbaTypes::MediumBytes(_) => AlbaTypes::MediumBytes(b),
                        AlbaTypes::BigSBytes(_) => AlbaTypes::BigSBytes(b),
                        AlbaTypes::LargeBytes(_) => AlbaTypes::LargeBytes(b),
                        _ => unreachable!(),
                    }
                }

                AlbaTypes::LightPassword(_) | AlbaTypes::MediumPassword(_) | AlbaTypes::HeavyPassword(_) | AlbaTypes::Slice4(_) | AlbaTypes::Slice3(_) | AlbaTypes::Slice2(_) | AlbaTypes::Slice1(_) | AlbaTypes::Slice0(_) => {
                    let b = field_buf.to_vec();
                    match column_type {
                        AlbaTypes::LightPassword(_) => AlbaTypes::LightPassword(b),
                        AlbaTypes::MediumPassword(_) => AlbaTypes::MediumPassword(b),
                        AlbaTypes::HeavyPassword(_) => AlbaTypes::HeavyPassword(b),
                        AlbaTypes::Slice4(_) => AlbaTypes::Slice4(b),
                        AlbaTypes::Slice3(_) => AlbaTypes::Slice3(b),
                        AlbaTypes::Slice2(_) => AlbaTypes::Slice2(b),
                        AlbaTypes::Slice1(_) => AlbaTypes::Slice1(b),
                        AlbaTypes::Slice0(_) => AlbaTypes::Slice0(b),
                        _ => unreachable!(),
                    }
                }
            };
            values.push(value);
        }

        if index != buf.len() {
            return Err(gerr("Extra data in buffer"));
        }

        Ok(values)
    }   
    
}
