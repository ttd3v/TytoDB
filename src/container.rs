use std::{collections::HashMap, ffi::CString, io::{self, Error, ErrorKind, Write}, os::unix::fs::FileExt, sync::Arc};
use ahash::AHashMap;
use tokio::{io::AsyncReadExt, sync::RwLock};
use tokio::fs::{File,self};
use xxhash_rust::const_xxh3;
use std::collections::{btree_set::BTreeSet,btree_map::BTreeMap};
use crate::{database::{write_data, QueryConditions, WrittingQuery, STRIX}, gerr, index_sizes::{self, IndexSizes}, index_tree::{self, IndexTree}, lexer_functions::{AlbaTypes, Token}, logerr, strix::DataReference};


type MvccType = Arc<RwLock<(AHashMap<u64,(bool,Vec<AlbaTypes>)>,HashMap<String,(bool,String)>)>>;
#[derive(Debug)]
pub struct Container{
    pub file : std::fs::File,
    pub element_size : usize,
    pub headers : HashMap<String,AlbaTypes>,
    pub str_size : usize,
    pub mvcc : MvccType,
    pub headers_offset : u64,
    pub location : String,
    pub graveyard : Arc<RwLock<BTreeSet<u64>>>,
    pub indexes : IndexTree,
    file_path : String

}

#[derive(Clone)]
pub enum QueryCandidates {
    All,
    Some(BTreeSet<IndexSizes>)
}
fn bind_query_candidates(m : QueryCandidates,n : QueryCandidates) -> QueryCandidates{
    if let QueryCandidates::Some(a) = n{
        if let QueryCandidates::Some(mut b) = m{
            b.extend(a.iter());
            return QueryCandidates::Some(b)
        }
    }
    QueryCandidates::All
}
fn link_query_candidates(m : QueryCandidates,n : QueryCandidates) -> QueryCandidates{
    if let QueryCandidates::Some(a) = n{
        if let QueryCandidates::Some(b) = m{
            let mut c = BTreeSet::new();
            for i in b.iter(){
                if a.contains(i){
                    c.insert(i.clone());
                }
            }
            return QueryCandidates::Some(c)
        }
    }
    QueryCandidates::All 
}
fn weird_thing_to_query_candidates(m : BTreeSet<BTreeSet<IndexSizes>>) -> QueryCandidates{
    let mut main = QueryCandidates::Some(BTreeSet::new());
    for i in m{
        main = bind_query_candidates(main, QueryCandidates::Some(i))
    } 
    main
}


pub fn is_coherent(input1: &Token, input2: &AlbaTypes) -> bool {
    matches!(
        (input1, input2),
        (Token::String(_), AlbaTypes::Text(_)
            | AlbaTypes::Char(_)
            | AlbaTypes::NanoString(_)
            | AlbaTypes::SmallString(_)
            | AlbaTypes::MediumString(_)
            | AlbaTypes::BigString(_)
            | AlbaTypes::LargeString(_)
            | AlbaTypes::NanoBytes(_)
            | AlbaTypes::SmallBytes(_)
            | AlbaTypes::MediumBytes(_)
            | AlbaTypes::BigSBytes(_)
            | AlbaTypes::LargeBytes(_))
        | (Token::Int(_), AlbaTypes::Int(_) | AlbaTypes::Bigint(_))
        | (Token::Float(_), AlbaTypes::Float(_))
        | (Token::Bool(_), AlbaTypes::Bool(_))
    )
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
        let mut  headers = HashMap::new();
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
            headers.insert(name.to_owned(), value.to_owned());
        }
        let file = std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
        let mut container = Container{
            file,
            element_size,
            str_size,
            mvcc: Arc::new(RwLock::new((AHashMap::new(),HashMap::new()))),
            headers_offset ,
            headers,
            location,
            graveyard: Arc::new(RwLock::new(BTreeSet::new())),
            indexes: IndexTree::default(),
            file_path: path.to_string()
        };
        container.load_indexing().await?;
        Ok(container)
    }
    
}
/*
    TODO: INDEX LOADING
*/

const INDEXING_CHUNK_SIZE : u64 = 50;
impl Container{
    pub fn column_names(&self) -> Vec<String>{
        self.headers.keys().cloned().collect()
    }
    async fn load_indexing(&mut self) -> Result<(),Error>{
        self.indexes.clear()?;
        let row_length = self.arrlen().await?;
        if row_length == 0{
            return Ok(())
        }
        let mut cursor : u64 = 0;
        while cursor < row_length{
            let end_index = if cursor+INDEXING_CHUNK_SIZE > row_length{
                cursor+INDEXING_CHUNK_SIZE - row_length
            }else{
                cursor+INDEXING_CHUNK_SIZE
            };
            let rows_not_formatted = self.get_rows((cursor,end_index)).await?;
            let mut rows = Vec::with_capacity(INDEXING_CHUNK_SIZE as usize);

            for i in rows_not_formatted.iter().enumerate(){
                rows.push((i.0*self.element_size,i.1.to_owned()));
            }
            
            let _ = self.indexes.insert_gently(self.column_names(), rows)?;
            cursor = end_index;
        }
        Ok(())
    }
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
        2_000 => values.push(AlbaTypes::BigString(s)),
        3_000 => values.push(AlbaTypes::LargeString(s)),
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
            let mvcc = self.mvcc.read().await;
            mvcc.0.keys().copied().max().map_or(0, |max_index| max_index + 1)
        };
        Ok(file_rows.max(mvcc_max))
    }
    pub fn get_alba_type_from_column_name(&self,column_name : &String) -> Option<&AlbaTypes>{
        if  let Some(g) = self.headers.get(column_name){
            return Some(g)
        }
        None
    }
    pub async fn candidates_from_unit(&self,condition : &(Token,Token,Token)) -> Result<QueryCandidates,Error>{
        let column_name_token = &condition.0;

        let column_name : &String = if let Token::String(cn) = column_name_token{
            cn
        }else{
            return Err(gerr("Invalid column name type, expected \"Token::String\"."))
        };
        

        let operator = if let Token::Operator(oprt) = &condition.1{
            oprt
        }else{
            return Err(gerr("Invalid operator type, expected \"Token::Operator\"."))
        };

        let column_alba_type = match self.get_alba_type_from_column_name(column_name){
            Some(at) => at,
            None => return Err(
                gerr(
                    &format!("There is no column named \"{}\", even while you try to use it. Verify if that is or isn\'t your fault, if isn\'t then the error may belong to the database system.",column_name)
                )
            )
        };

        if !is_coherent(&condition.2, column_alba_type) {
            return Err(gerr(
                &format!(
                    r#"Type mismatch: Got {:?} with {:?}. Valid pairs:\n\
                     - Token::String: Text, Char, *String, *Bytes\n\
                     - Token::Int: Int, Bigint\n\
                     - Token::Float: Float\n\
                     - Token::Bool: Bool"#,
                    condition.2, column_alba_type
                )
            ));
        }

        Ok(match operator.as_str(){
            "==" | "=" => {
                match self.indexes.get_all_in_group(&column_name, condition.2.clone())?{
                    Some(a) => {
                        QueryCandidates::Some(a.clone())
                    },
                    None =>{
                        QueryCandidates::All
                    }
                }
            },
            ">=" | ">" => {
                match self.indexes.get_most_in_group_raising(&column_name, condition.2.clone())?{
                    Some(a) => {
                        weird_thing_to_query_candidates(a.clone())
                    },
                    None =>{
                        QueryCandidates::All
                    }
                }
            },
            "<=" | "<" => {
                match self.indexes.get_most_in_group_lowering(&column_name, condition.2.clone())?{
                    Some(a) => {
                        weird_thing_to_query_candidates(a.clone())
                    },
                    None =>{
                        QueryCandidates::All
                    }
                }
            },
            _ => {
                QueryCandidates::All
            }
        })
    }
    pub async fn get_query_candidates(&self,query_conditions : &QueryConditions) -> Result<BTreeSet<IndexSizes>,Error>{
        let candidates_set : BTreeSet<IndexSizes> = BTreeSet::new();
        let mut condition_groups : Vec<QueryCandidates> = Vec::new();
        for i in query_conditions.0.iter(){
            let wsoeufh = self.candidates_from_unit(i).await?;
            condition_groups.push(wsoeufh);
        }
        let mut the_main_thing_idk_how_to_name_this = QueryCandidates::Some(BTreeSet::new());
        let mut conditons_hmap : AHashMap<usize, char> = AHashMap::new();
        for i in &query_conditions.1{
            conditons_hmap.insert(i.0.clone(), i.1.clone());
        }
        let mut comparision : char = 'o';
        for i in condition_groups.iter().enumerate(){
            if let Some(a) = conditons_hmap.get(&i.0){
                comparision = *a;
            }
            match comparision{
                'o' => {
                    the_main_thing_idk_how_to_name_this = link_query_candidates(the_main_thing_idk_how_to_name_this, i.1.clone())
                },
                _ => {
                    the_main_thing_idk_how_to_name_this = bind_query_candidates(the_main_thing_idk_how_to_name_this, i.1.clone())
                }
            }
        }
        if let QueryCandidates::Some(tmtihtnt) = the_main_thing_idk_how_to_name_this{
            return Ok(tmtihtnt)
        }
        return Ok(candidates_set)
    }
    pub async fn get_next_addr(&self) -> Result<u64, Error> {
        let mut graveyard = self.graveyard.write().await;
        if graveyard.len() > 0{
            if let Some(id) = graveyard.pop_first(){
                return Ok(id)
            }
        }
        let current_rows = self.arrlen().await?;
        let mvcc = self.mvcc.read().await;
        for (&key, (deleted, _)) in mvcc.0.iter() {
            if *deleted {
                return Ok(key);
            }
        }
        Ok(current_rows)
    } 
    pub async fn push_row(&mut self, data : &Vec<AlbaTypes>) -> Result<(),Error>{
        let ind = self.get_next_addr().await?;
        let mut mvcc_guard = self.mvcc.write().await;
        mvcc_guard.0.insert(ind, (false,data.clone()));
        Ok(())
    }
    pub async fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.write().await;
        mvcc_guard.0.clear();
        mvcc_guard.1.clear();
        drop(mvcc_guard);
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), Error> {
        let mut total = self.arrlen().await?;
        let mut virtual_ward : AHashMap<usize, DataReference> = AHashMap::new();
        println!("commit: Locking MVCC...");
        let mut mvcc = self.mvcc.write().await;
    
        println!("commit: Separating insertions and deletions...");
        let mut insertions: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        let mut deletes: Vec<(u64, Vec<AlbaTypes>)> = Vec::new();
        for (index, value) in mvcc.0.iter() {
            let v = (*index, value.1.clone());
            if value.0 {
                deletes.push(v);
            } else {
                insertions.push(v);
            }
        }
        mvcc.0.clear();
    
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
            virtual_ward.insert(offset as usize, (const_xxh3::xxh3_64(serialized.as_slice()),serialized));
        }
        
        let mut graveyard = self.graveyard.write().await;
        for del in &deletes {
            println!("{:?}",del);
            let from = hdr_off + del.0 * row_sz;
            self.file.write_all_at(&buf, from)?;
            virtual_ward.insert(from as usize, (const_xxh3::xxh3_64(&buf),buf.clone()));
            total -= 1;
            graveyard.insert(del.0);
        }
        drop(graveyard);
        let new_len = hdr_off + total * row_sz;
        self.file.set_len(new_len)?;
        

        for (i, txt) in  mvcc.1.iter(){
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
                        logerr!("C write_data failed")
                    }
            } else if std::fs::exists(&path)?{
                fs::remove_file(&path).await?
            }
        
        }

        mvcc.1.clear();
        mvcc.1.shrink_to_fit();
    
        println!("commit: COMMIT SUCCESSFUL!");
        println!("commit: Starting to sync...");
                
        println!("commit: Sync!");
        if let Some(s) = STRIX.get(){
            let mut l = s.write().await;
            l.wards.push(RwLock::new((std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path)?,virtual_ward)));
        }
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
    
        let mvcc = self.mvcc.read().await;
    
        let mut result: Vec<Vec<AlbaTypes>> = Vec::new(); 
        for i in index.0..lidx {
            if let Some((deleted, row_data)) = mvcc.0.get(&i) {
                if *deleted {
                    continue;
                }
    
                let mut row = row_data.clone();
                match mvcc.0.get(&i){
                    Some(row_in_mvcc) => {
                        if !row_in_mvcc.0{
                            row = row_in_mvcc.1.clone()
                        }
                    },
                    None => {}
                }
                for value in row.iter_mut() {
                    if let AlbaTypes::Text(c) = value {
                        if let Some((deleted, new_text)) = mvcc.1.get(c) {
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
    pub async fn get_spread_rows(&mut self, index: &mut Vec<u64>) -> Result<Vec<Vec<AlbaTypes>>, Error> {
        let maxl = if self.len().await? > self.headers_offset {
            (self.len().await? - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        for i in index.iter_mut(){
            if *i > maxl {
                *i = maxl;
            }
        }
        index.sort();
        let mut buffers : BTreeMap<u64,Vec<u8>> = BTreeMap::new();
        let mut result : Vec<Vec<AlbaTypes>> = Vec::new();
        let mvcc = self.mvcc.read().await;
        for i in index{
            if let Some(g) = mvcc.0.get(&i){
                if !g.0{
                    result.push(g.1.clone());
                    continue;
                }
            }
            buffers.insert(*i, vec![0u8;self.element_size]);
        }
        drop(mvcc);
        let mut it = buffers.iter_mut();
        while let Some(b) = it.next(){
            
            if let Some(c) = it.next(){
                if *c.0-*b.0 < 10{
                    let buff_size = ((*c.0 - *b.0 + 1) * self.element_size as u64) as usize;
                    let mut buff = vec![0u8;buff_size];
                    self.file.read_exact_at(&mut buff, (b.0*self.element_size as u64)+self.headers_offset)?;
                    *b.1 = buff[0..self.element_size].to_vec();
                    let c_off = ((*c.0-*b.0)*self.element_size as u64) as usize;
                    *c.1 = buff[c_off..(c_off+self.element_size)].to_vec();
                    result.push(self.deserialize_row(b.1.to_vec()).await?);
                    result.push(self.deserialize_row(c.1.to_vec()).await?);
                    continue;
                }else{
                    self.file.read_exact_at(b.1, (b.0*self.element_size as u64)+self.headers_offset)?;
                    self.file.read_exact_at(c.1, (c.0*self.element_size as u64)+self.headers_offset)?;
                    result.push(self.deserialize_row(b.1.to_vec()).await?);
                    result.push(self.deserialize_row(c.1.to_vec()).await?);

                    continue;
                }
            }
            self.file.read_exact_at(b.1, (b.0*self.element_size as u64)+self.headers_offset)?;
            result.push(self.deserialize_row(b.1.to_vec()).await?);
        }
        
        result.shrink_to_fit();
        Ok(result)
    }
    
    pub async fn heavy_get_spread_rows(&mut self, index: &mut BTreeSet<IndexSizes>) -> Result<WrittingQuery, Error> {
        let maxl = if self.len().await? > self.headers_offset {
            (self.len().await? - self.headers_offset) / self.element_size as u64
        } else {
            0
        };
        let mut buffers : BTreeMap<IndexSizes,Vec<u8>> = BTreeMap::new();
        let mut result : WrittingQuery = BTreeMap::new();
        let mvcc = self.mvcc.read().await;
        for i in index.iter(){
            if let Some(g) = mvcc.0.get(&(IndexSizes::to_usize(*i) as u64)){
                if !g.0||*i > IndexSizes::U64(maxl){
                    result.insert(*i,g.1.clone());
                    continue;
                }
            }
            buffers.insert(*i ,vec![0u8;self.element_size]);
        }
        drop(mvcc);
        let element_size = IndexSizes::Usize(self.element_size);
        let mut it = buffers.iter_mut();
        while let Some(b) = it.next(){
            
            if let Some(c) = it.next(){
                if (*c.0-*b.0) < IndexSizes::U64(10){
                    let buff_size = (*c.0 - *b.0 + IndexSizes::U64(1)) * element_size ;
                    let mut buff = vec![0u8;buff_size.as_usize()];
                    self.file.read_exact_at(&mut buff, (b.0.as_u64()*element_size.as_u64())+self.headers_offset)?;
                    *b.1 = buff[0..self.element_size].to_vec();
                    let c_off = ((*c.0-*b.0)*element_size).as_usize() ;
                    *c.1 = buff[c_off..(c_off+self.element_size)].to_vec();
                    result.insert(b.0.to_owned(),self.deserialize_row(b.1.to_vec()).await?);
                    result.insert(c.0.to_owned(),self.deserialize_row(c.1.to_vec()).await?);
                    continue;
                }else{
                    self.file.read_exact_at(b.1, (b.0.as_u64()*self.element_size as u64)+self.headers_offset)?;
                    self.file.read_exact_at(c.1, (c.0.as_u64()*self.element_size as u64)+self.headers_offset)?;
                    result.insert(b.0.to_owned(),self.deserialize_row(b.1.to_vec()).await?);
                    result.insert(c.0.to_owned(),self.deserialize_row(c.1.to_vec()).await?);

                    continue;
                }
            }
            self.file.read_exact_at(b.1, (b.0.as_u64()*self.element_size as u64)+self.headers_offset)?;
            result.insert(b.0.to_owned(),self.deserialize_row(b.1.to_vec()).await?);
        }
        Ok(result)
    }
    
    pub fn columns(&self) -> Vec<&AlbaTypes>{
        self.headers.values().by_ref().collect()
    }
    pub fn columns_owned(&self) -> Vec<AlbaTypes>{
        self.headers.values().cloned().collect()
    }
    pub fn serialize_row(&self, row: &[AlbaTypes]) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::with_capacity(self.element_size);
    
        for (item, ty) in row.iter().zip(self.columns().iter()) {
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
    
        for column_type in &self.columns() {
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