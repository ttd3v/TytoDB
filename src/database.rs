use std::{collections::HashMap, ffi::CString, fs::{self, File}, io::{self, Error, ErrorKind, Read, Write}, mem::discriminant, os::unix::fs::FileExt, path::PathBuf, pin::Pin, sync::{Arc, Mutex}, vec};
use serde::{Serialize,Deserialize};
use size_of;
use serde_yaml;
use crate::{debug_tokens, gerr, lexer_functions::{AlbaTypes, Token}, parse, AlbaContainer, AstSearch, AST};
use rand::{Rng, distributions::Alphanumeric};

#[link(name = "io", kind = "static")]
unsafe extern "C" {
    pub fn write_data(buffer: *const u8, len: usize, path: *const std::os::raw::c_char) -> i32;
}

fn generate_secure_code(len: usize) -> String {
    let mut rng = rand::rngs::OsRng;
    let code: String = (0..len)
        .map(|_| rng.sample(Alphanumeric))
        .map(char::from)
        .collect();
    code
}

pub struct Database{
    location : String,
    settings : Settings,
    containers : Vec<String>,
    headers : Vec<(Vec<String>,Vec<AlbaTypes>)>,
    container : HashMap<String,Container>
}

fn check_for_reference_folder(location : &String) -> Result<(), Error>{
    let path = format!("{}/rf",location);
    if !match fs::exists(path.clone()){Ok(a)=>a,Err(e)=>{return Err(e)}}{
        return match fs::create_dir(path){
            Ok(a)=>Ok(a),
            Err(e)=>Err(e)
        }
    }
    Ok(())
}

#[derive(Serialize,Deserialize, Default,Debug)]
struct Settings{
    max_columns : u32,
    min_columns : u32,
    max_str_length : usize,
    memory_limit : u64,
    auto_commit : bool
}

fn text_type_identifier(max_str_length: usize) -> u8 {
    if max_str_length > 255 {
        255u8
    } else {
        max_str_length as u8
    }
}
fn from_usize_to_u8(i:usize) -> u8{
    return i as u8;
}

type MvccType = Arc<Mutex<HashMap<u64,(bool,Vec<AlbaTypes>)>>>;
struct Container{
    file : File,
    element_size : usize,
    columns : Vec<AlbaTypes>,
    str_size : usize,
    mvcc : MvccType,
    text_mvcc : Arc<Mutex<HashMap<String,(bool,String)>>>,
    headers_offset : u64,
    column_names : Vec<String>,
    location : String,
}



trait New {
    async fn new(path : &str,location : String,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> where Self: Sized ;

}

impl New for Container {
    async fn new(path : &str,location : String,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> {
        Ok(Container{
            file : fs::OpenOptions::new().read(true).write(true).open(&path)?,
            element_size,
            columns,
            str_size,
            mvcc: Arc::new(Mutex::new(HashMap::new())),
            text_mvcc: Arc::new(Mutex::new(HashMap::new())),
            headers_offset ,
            column_names,
            location
        })
    }
    
}
fn sync_file(file: &std::fs::File) -> std::io::Result<()> {
    file.sync_all()
}
fn try_open_file(path: &str) -> io::Result<Option<File>> {
    match File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
impl Container{
    fn len(&self) -> Result<u64,Error>{
        Ok(self.file.metadata()?.len())
    }
    fn arrlen(&self) -> Result<u64, Error> {
        let file_len = self.len()?;
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
    
    
    pub fn get_next_addr(&self) -> Result<u64, Error> {
        let current_rows = self.arrlen()?;
        let mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        for (&key, (deleted, _)) in mvcc_guard.iter() {
            if *deleted {
                return Ok(key);
            }
        }
        Ok(current_rows)
    } 
    pub fn push_row(&mut self, data : &Vec<AlbaTypes>) -> Result<(),Error>{
        let ind = self.get_next_addr()?;
        let mut mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        mvcc_guard.insert(ind, (false,data.clone()));
        Ok(())
    }
    async fn rollback(&mut self) -> Result<(),Error> {
        let mut mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        mvcc_guard.clear();
        drop(mvcc_guard);

        let mut txt_mvcc = match self.text_mvcc.lock(){
            Ok(a) => a,
            Err(e) => {
                return Err(gerr(&e.to_string()))
            }
        };
        txt_mvcc.clear();
        Ok(())
    }
    async fn commit(&mut self) -> Result<(), Error> {
        let mut total = self.arrlen()?;
        println!("commit: Locking MVCC...");
        let mut mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
    
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
        let mut buf = vec![0u8; self.element_size];
    
        println!("commit: Writing insertions...");
        for (row_index, row_data) in insertions {
            let serialized = self.serialize_row(&row_data)?;
            let offset = hdr_off + row_index * row_sz;
            self.file.write_all_at(serialized.as_slice(), offset)?;
        }
    
        for del in &deletes {
            println!("{:?}",del);
            for i in (del.0 + 1)..total {
                println!("{}",i);
                let from = hdr_off + i * row_sz;
                let to   = hdr_off + (i - 1) * row_sz;
                self.file.read_exact_at(&mut buf, from)?;
                self.file.write_all_at(&buf, to)?;
            }
            total -= 1;
        }
        let new_len = hdr_off + total * row_sz;
        self.file.set_len(new_len)?;
        
        let mut txt_mvcc = match self.text_mvcc.lock(){
            Ok(a) => a,
            Err(e) => {
                return Err(gerr(&e.to_string()))
            }
        };

        for (i, txt) in  txt_mvcc.iter(){
            let path = format!("{}/rf/{}", self.location, i); 
            if !txt.0 {
                let mut file: File = fs::File::create_new(&path)?;
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
            } else if fs::exists(&path)?{
                fs::remove_file(&path)?
            }
        
        }

        txt_mvcc.clear();
        txt_mvcc.shrink_to_fit();
    
        println!("commit: COMMIT SUCCESSFUL!");
        println!("commit: Starting to sync...");
                
        let file = self.file.try_clone()?; 
        tokio::task::spawn_blocking(move || sync_file(&file));
        println!("commit: Sync!");
    
        Ok(())
    }
    pub fn get_rows(&self, index: (u64, u64)) -> Result<Vec<Vec<AlbaTypes>>, Error> {
        let mut lidx = index.1;
        let maxl = self.len()? - (self.headers_offset / self.element_size as u64);
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
    
        let v_c = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        let t_c = self.text_mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
    
        let mut result: Vec<Vec<AlbaTypes>> = Vec::new(); 
        for i in index.0..lidx {
            if let Some((deleted, row_data)) = v_c.get(&i) {
                if *deleted {
                    continue;
                }
    
                let mut row = row_data.clone();
                for value in row.iter_mut() {
                    if let AlbaTypes::Text(c) = value {
                        if let Some((deleted, new_text)) = t_c.get(c) {
                            if !*deleted {
                                *value = AlbaTypes::Text(new_text.to_string());
                            }
                        }
                    }
                }
    
                result.push(row); 
            } else {
                let start = ((i - index.0) * self.element_size as u64) as usize;
                let end = start + self.element_size;
                let row_buf = &buff[start..end];
                let row = self.deserialize_row(row_buf.to_vec())?;
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
    
    fn deserialize_row(&self, buf: Vec<u8>) -> Result<Vec<AlbaTypes>,Error> {
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
                    let mut file = match try_open_file(&format!("{}/rf/{}",self.location,str_id))? {
                        Some(a) => a,
                        None => {value.push(AlbaTypes::Text(str_id)); continue},
                    };
                    let mut buffer : Vec<u8> = Vec::with_capacity(100);
                    match file.read_to_end(&mut buffer){
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
const SETTINGS_FILE : &str = "settings.yaml";
fn calculate_header_size(max_str_len: usize, max_columns: usize) -> usize {
    let column_names_size = max_str_len * max_columns;
    let column_types_size = max_columns;
    column_names_size + column_types_size
}




type QueryPage = (u64,u64,String);
type QueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);
#[derive(Debug)]
pub struct Query{
    pub rows: Rows,
    pub pages : Vec<QueryPage>,
    pub current_page : QueryPage,
    pub column_names : Vec<String>,
    pub column_types : Vec<AlbaTypes>
}
impl Query{
    fn new() -> Self{
        Query { rows: (Vec::new(),Vec::new()), pages: Vec::new(), current_page: (0,0,String::new()), column_names: Vec::new(), column_types: Vec::new() }
    }
    fn join(&mut self,foreign : Query){
        self.pages.extend_from_slice(&foreign.pages);
    }
}


fn condition_checker(row: &Vec<AlbaTypes>, col_names: &Vec<String>, conditions: &QueryConditions) -> Result<bool, Error> {
    if col_names.len() != row.len() {
        return Err(gerr(&format!("Row data does not match column names: expected {} columns, got {} values", col_names.len(), row.len())));
    }
    
    let mut indexes: HashMap<String, AlbaTypes> = HashMap::new();
    for i in row.iter().enumerate() {
        let string = match col_names.get(i.0) {
            Some(a) => a,
            None => return Ok(false)
        };
        indexes.insert(string.clone(), i.1.clone());
    }

    let and_or_indexes = &conditions.1;
    let operators = &conditions.0;

    if operators.is_empty() {
        return Ok(true);
    }

    let mut booleans: Vec<bool> = Vec::with_capacity(operators.len());
    for b in operators {
        let first = if let Token::String(a) = &b.0 {
            match indexes.get(a) {
                Some(a) => a.clone(),
                None => return Err(gerr(&format!("In query condition, column '{}' not found in row data", a)))
            }
        } else {
            return Err(gerr("In query condition, expected a string for column name, but found a different token type"));
        };
        
        let operator = if let Token::Operator(a) = &b.1 {
            a
        } else {
            return Err(gerr("Invalid type for operator"));
        };

        let result = match (first,&b.2) {
            (AlbaTypes::Bool(val1),Token::Bool(val2)) => {
                match operator.as_str() {
                    "=" | "==" => val1 == *val2,
                    "!=" | "<>" => val1 != *val2,
                    _ => return Err(gerr(&format!("Invalid operator '{}' for boolean comparison", operator)))
                }
                    
            },
            (AlbaTypes::Int(val1),Token::Int(val)) => {
                let val2 = *val as i32;
                match operator.as_str() {
                    "=" | "==" => val1 == val2,
                    "!=" | "<>" => val1 != val2,
                    ">" => val1 > val2,
                    "<" => val1 < val2,
                    ">=" => val1 >= val2,
                    "<=" => val1 <= val2,
                    _ => return Err(gerr(&format!("Invalid operator '{}' for integer comparison", operator)))
                        
                }
            },
            (AlbaTypes::Bigint(val1),Token::Int(val2)) => {
                let val2_i64 = *val2 as i64;
                match operator.as_str() {
                    "=" | "==" => val1 == val2_i64,
                    "!=" | "<>" => val1 != val2_i64,
                    ">" => val1 > val2_i64,
                    "<" => val1 < val2_i64,
                    ">=" => val1 >= val2_i64,
                    "<=" => val1 <= val2_i64,
                    _ => return Err(gerr(&format!("Invalid operator '{}' for bigint comparison", operator)))
                }
                    
            },
            (AlbaTypes::Float(val1),Token::Float(val2)) => {
                match operator.as_str() {
                    "=" | "==" => (val1 - val2).abs() < f64::EPSILON,
                    "!=" | "<>" => (val1 - val2).abs() >= f64::EPSILON,
                    ">" => val1 > *val2,
                    "<" => val1 < *val2,
                    ">=" => val1 >= *val2,
                    "<=" => val1 <= *val2,
                    _ => return Err(gerr(&format!("Invalid operator '{}' for float comparison", operator)))
                }
            },
            (AlbaTypes::Text(val1),Token::String(val2)) => {
                match operator.as_str() {
                    "=" | "==" => val1 == *val2,
                    "!=" | "<>" => val1 != *val2,
                    "&>" => val1.contains(val2),
                    "&&>" => val1.to_lowercase().contains(&val2.to_lowercase()),
                    "&&&>" => val1.to_lowercase().trim().contains(val2.to_lowercase().trim()),
                    _ => return Err(gerr(&format!("Invalid operator '{}' for string comparison", operator)))
                }
                    
            },
            (AlbaTypes::NONE,_) => return Err(gerr("Cannot compare NULL values")),
            _ => {
                return Err(gerr("Failed"))
            }
        };

        booleans.push(result);
    }
    if booleans.is_empty() {
        return Ok(true); 
    }

    let mut final_result = booleans[0];
    for (idx, op) in and_or_indexes.iter() {
        let next_bool = booleans.get(*idx + 1).ok_or(gerr("Invalid condition index"))?;
        final_result = match op {
            'A' | 'a' => final_result && *next_bool,
            'O' | 'o' => final_result || *next_bool,
            _ => return Err(gerr(&format!("Unknown logical operator: {}", op)))
        };
    }

    Ok(final_result)
}

impl Database{
    fn ancient_query(&mut self, col_names : &Vec<String>,containers : &[AlbaContainer],conditions : &QueryConditions) -> Result<Query,Error>{
        let mut final_query = Query::new();
        if let AlbaContainer::Real(container_name) = &containers[0]{
            let container = match self.container.get(container_name){
                Some(a) => a,
                None => return Err(gerr(&format!("No container named {}",container_name)))
            };
            let length = container.arrlen()?;
            let page_size = (length*container.element_size as u64)/self.settings.memory_limit;
            let mut cursor : u64 = 0;

            final_query.column_names = col_names.clone();
            while cursor < length{
                let rows = container.get_rows((cursor,cursor+page_size))?;
                let start_index = cursor.clone();
                let mut end_index = start_index;
                for (number,row) in rows.iter().enumerate(){
                    if condition_checker(row, &col_names, &conditions)?{
                        end_index = start_index + number as u64;
                    }
                }
                cursor += rows.len() as u64;
                final_query.pages.push((start_index,end_index,container_name.to_string()));
            }
            return Ok(final_query);
        }
        Err(gerr("No valid containers specified for query processing"))
    }

    fn query_diver(&mut self,col_names : &Vec<String>,containers : &[AlbaContainer],conditions : &QueryConditions) -> Result<Query,Error>{
        
        let cl = containers.len();

        if cl == 1{
            return Ok(self.ancient_query(col_names, containers,conditions)?)
        }
        if cl > 1{
            let mut final_query = Query::new();
            final_query.column_names = col_names.to_vec();

            let middle = cl/2;
            let q1 = self.query_diver(&col_names, &containers[..middle],&conditions)?;
            let q2 = self.query_diver(&col_names, &containers[middle..],&conditions)?;
            final_query.join(q1);
            final_query.join(q2);
            return Ok(final_query)
        }


        Err(gerr("No valid containers specified for query processing"))
    }

}
impl Database{
    pub async fn query<'a>(&'a mut self,col_names: Vec<String>,containers: Vec<AlbaContainer>,conditions:QueryConditions ) -> Result<Query, Error>{
        Ok(self.query_diver(&col_names, &containers, &conditions)?)
    }
}



impl Database {
    fn set_default_settings(&self) -> Result<(), Error> {
        let path = format!("{}/{}", self.location,SETTINGS_FILE);
        if !match fs::metadata(&path) { Ok(_) => true, Err(_) => false } {
            let mut file =fs::File::create_new(&path)?;
            let content = format!(r#"
# WARNING: If you change 'max_columns' or 'max_str_length' after creating a container, it might not work until you revert the changes.
max_columns: {}
min_columns: {}
max_str_length: {}

auto_commit: {}
            
# Memory limit: defines how much memory the database can use during operations. Setting a higher value might improve performance, but exceeding hardware limits could have the opposite effect.
memory_limit: {}
            "#, 50, 1, 128, true,104_857_600);
            match file.write_all(content.as_bytes()) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    async fn load_containers(&mut self) -> Result<(), Error> {
        let path = std::path::PathBuf::from(format!("{}/containers.yaml",&self.location));


        
        if !path.exists() {
            let yaml = serde_yaml::to_string(&self.containers)
                .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            fs::write(&path, yaml)?;
            return Ok(());
        }

        let raw = fs::read_to_string(&path)?;
        self.containers = serde_yaml::from_str(&raw)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string()))?;


        self.headers.clear();
        for contain in self.containers.iter(){

            let he = self.get_container_headers(&contain)?;
     
            self.headers.push(he.clone());
            let mut element_size : usize = 0;
            for el in he.1.iter(){
                element_size += match el {
                    AlbaTypes::Bigint(_) => size_of::<i64>(),
                    AlbaTypes::Int(_) => size_of::<i32>(),
                    AlbaTypes::Float(_) => size_of::<f64>(),
                    AlbaTypes::Bool(_) => size_of::<bool>(),
                    AlbaTypes::Text(_) => self.settings.max_str_length,
                    AlbaTypes::NONE => 0
                }
            }
            self.container.insert(contain.to_string(),Container::new(&format!("{}/{}",self.location,contain),self.location.clone(), element_size, he.1, self.settings.max_str_length,calculate_header_size(self.settings.max_str_length,self.settings.max_columns as usize) as u64,he.0.clone()).await?);
        }
        println!("headers: {:?}",self.headers);
        Ok(())
    }

    fn save_containers(&self) -> Result<(), Error> {
        let path = std::path::PathBuf::from(&self.location).join("containers.yaml");
        let yaml = serde_yaml::to_string(&self.containers)
            .map_err(|e| Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        fs::write(&path, yaml)?;
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(),Error>{
        for (_,c) in self.container.iter_mut(){
            c.commit().await?;
        }
        Ok(())
    }
    pub async fn rollback(&mut self) -> Result<(),Error>{
        for (_,c) in self.container.iter_mut(){
            c.rollback().await?;
        }
        Ok(())
    }
    fn load_settings(&mut self) -> Result<(), Error> {
        let dir = PathBuf::from(&self.location);
        let path = dir.join(SETTINGS_FILE);
        fs::create_dir_all(&dir)?;
        if path.exists() && fs::metadata(&path)?.is_dir() {
            fs::remove_dir(&path)?;
        }
        if !path.is_file() {
            println!("not file");
            self.set_default_settings()?; 
        }
        let mut rewrite = true;
        let raw = fs::read_to_string(&path)
            .map_err(|e| Error::new(e.kind(), format!("Failed to read {}: {}", SETTINGS_FILE, e)))?;
        let mut settings: Settings = serde_yaml::from_str(&raw)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("Invalid {}: {}", SETTINGS_FILE, e)))?;

        if settings.max_columns <= settings.min_columns {
            settings.min_columns = 1;
            rewrite = true;
        }
        if settings.max_columns <= 1 {
            settings.max_columns = 10;
            rewrite = true;
        }
        if settings.min_columns > settings.max_columns {
            settings.min_columns = 1;
            rewrite = true;
        }
        if settings.memory_limit < 1_048_576 {
            settings.memory_limit = 1_048_576;
            rewrite = true;
        }
        if settings.max_str_length < 1 {
            settings.max_str_length = 1;
            rewrite = true;
        }
        if rewrite {
            let new_yaml = serde_yaml::to_string(&settings)
                .map_err(|e| Error::new(ErrorKind::Other, format!("Serialize failed: {}", e)))?;
            fs::write(&path, new_yaml)
                .map_err(|e| Error::new(e.kind(), format!("Failed to rewrite {}: {}", SETTINGS_FILE, e)))?;
        }

        self.settings = settings;
        Ok(())
    }

    fn get_container_headers(&self, container_name: &str) -> Result<(Vec<String>, Vec<AlbaTypes>), Error> {
        let path = format!("{}/{}",self.location,container_name);
        let max_columns : usize = self.settings.max_columns as usize;
        let max_str_len : usize = self.settings.max_str_length;
        let strhs = max_str_len * max_columns;
        let exists = fs::exists(&path)?;
        if exists{
            let header_size = calculate_header_size(self.settings.max_str_length,self.settings.max_columns as usize);
            let file = fs::File::open(&path)?;
            let mut buffer : Vec<u8> = vec![0u8;header_size];
            file.read_exact_at(&mut buffer,0)?;
            let name_headers_bytes = &buffer[..strhs];
            let types_headers_bytes = &buffer[strhs..];
            let mut column_names: Vec<String> = Vec::with_capacity(max_columns);
            let mut column_types: Vec<AlbaTypes> = Vec::with_capacity(max_columns);

            for i in 0..max_columns {
                let start_pos = i * max_str_len;
                let mut actual_length = 0;
                for j in 0..max_str_len {
                    if j + start_pos >= name_headers_bytes.len() || name_headers_bytes[start_pos + j] == 0 {
                        break;
                    }
                    actual_length += 1;
                }
                if actual_length > 0 {
                    match String::from_utf8(name_headers_bytes[start_pos..start_pos + actual_length].to_vec()) {
                        Ok(name) => column_names.push(name),
                        Err(_) => return Err(gerr("Invalid UTF-8 in column name"))
                    }
                } else {
                    column_names.push(String::new());
                }
            }
            for i in 0..max_columns {
                if i < types_headers_bytes.len() {
                    let type_byte = types_headers_bytes[i];
                    let alba_type = match type_byte {
                        0 => AlbaTypes::NONE,
                        size if size == from_usize_to_u8(size_of::<i32>()) => AlbaTypes::Int(0),
                        size if size == from_usize_to_u8(size_of::<i64>()) => {
                            AlbaTypes::Bigint(0)
                        },
                        size if size == text_type_identifier(max_str_len) => AlbaTypes::Text(String::new()),
                        size if size == from_usize_to_u8(size_of::<f64>()) => AlbaTypes::Float(0.0),
                        size if size == from_usize_to_u8(size_of::<bool>()) => AlbaTypes::Bool(false),
                        _ => return Err(gerr("Unknown type size in column value types"))
                    };
                    
                    column_types.push(alba_type);
                } else {
                    column_types.push(AlbaTypes::NONE);
                }
            }
            
            let mut valid_column_count = max_columns;
            for i in 0..max_columns {
                if column_names[i].is_empty() && matches!(column_types[i], AlbaTypes::NONE) {
                    valid_column_count = i;
                    break;
                }
            }
            column_names.truncate(valid_column_count);
            column_types.truncate(valid_column_count);
            column_names.shrink_to_fit();
            column_types.shrink_to_fit();
            
            return Ok((column_names, column_types));
    
        }
        Err(gerr("Container not found"))
    }
    
    pub async fn run(&mut self, ast : AST) -> Result<Query,Error>{

        let min_column : usize = self.settings.min_columns as usize;
        let max_columns : usize = self.settings.max_columns as usize;
        let max_str_len : usize = self.settings.max_str_length;
        match ast{
            AST::CreateContainer(structure) => {
              if !match fs::exists(format!("{}/{}",self.location,structure.name)) {Ok(a)=>a,Err(bruh)=>{return Err(bruh)}}{
                let cn_len = structure.col_nam.len();
                let cv_len = structure.col_val.len();

                if cn_len != cv_len{
                    return Err(gerr(&format!("Mismatch in CREATE CONTAINER: provided {} column names but {} column types", cn_len, cv_len)))
                }
                if cn_len < min_column || cv_len < min_column{
                    if cn_len < min_column {
                        return Err(gerr(&format!("Column count must be {} or more", min_column)))
                    }                    
                }
                if cn_len > max_columns || cv_len > max_columns{
                    return Err(gerr(format!("Exceeded maximum column count of {}",max_columns).as_str()))
                }

                let mut column_name_headers: Vec<String> = Vec::with_capacity(max_columns);
                let mut column_val_headers: Vec<AlbaTypes> = Vec::with_capacity(max_columns);

                for _ in 0..max_columns {
                    column_name_headers.push("".to_string());
                    column_val_headers.push(AlbaTypes::NONE);
                }


                for (num, str_val) in structure.col_nam.iter().enumerate().take(max_columns) {
                    column_name_headers[num] = str_val.to_string();
                }
                for (num, v) in structure.col_val.iter().enumerate().take(max_columns) {
                    column_val_headers[num] = v.clone();
                }
                
                let mut column_name_bytes: Vec<Vec<u8>> = vec![vec![0u8; max_str_len]; max_columns];
                let mut column_val_bytes: Vec<u8> = vec![0u8; max_columns];

                for (i,item) in column_name_headers.iter().enumerate(){
                    let bytes = item.as_bytes();
                    if bytes.len() > max_str_len {
                        return Err(Error::new(std::io::ErrorKind::Other, "String too long"));
                    }
                    column_name_bytes[i][..bytes.len()].copy_from_slice(bytes);
                }
                for (i, item) in column_val_headers.iter().enumerate(){
                    column_val_bytes[i] = match item{
                        AlbaTypes::Int(_) => from_usize_to_u8(size_of::<i32>()),
                        AlbaTypes::Bigint(_) => from_usize_to_u8(size_of::<i64>()),
                        AlbaTypes::Float(_) => from_usize_to_u8(size_of::<f64>()),
                        AlbaTypes::Bool(_) => from_usize_to_u8(size_of::<bool>()),
                        AlbaTypes::Text(_) => max_str_len as u8,
                        AlbaTypes::NONE => 0
                    }
                }

                if let Err(e) = check_for_reference_folder(&self.location){
                    return Err(e)
                }
                let mut file = match fs::File::create(format!("{}/{}",self.location,structure.name)){
                    Ok(f)=>{f}
                    ,Err(e)=>{return Err(e)}
                };

                let mut flattened: Vec<u8> = Vec::new();
                for arr in &column_name_bytes {
                    flattened.extend_from_slice(arr);
                }
                for arr in &column_val_bytes {
                    flattened.push(*arr);
                }
                match file.write_all(&flattened) {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(e)
                    }
                };
                self.containers.push(structure.name.clone());
                let mut element_size : usize = 0;
                for el in column_val_headers.iter(){
                    element_size += match el {
                        AlbaTypes::Bigint(_) => size_of::<i64>(),
                        AlbaTypes::Int(_) => size_of::<i32>(),
                        AlbaTypes::Float(_) => size_of::<f64>(),
                        AlbaTypes::Bool(_) => size_of::<bool>(),
                        AlbaTypes::Text(_) => self.settings.max_str_length,
                        AlbaTypes::NONE => 0
                    }
                }
                self.container.insert(structure.name.clone(), Container::new(&format!("{}/{}",self.location,structure.name),self.location.clone(), element_size, column_val_headers.clone(), self.settings.max_str_length,calculate_header_size(self.settings.max_str_length,self.settings.max_columns as usize) as u64,column_name_headers.clone()).await?);
                if let Err(e) = self.save_containers(){return Err(gerr(&e.to_string()))};
                let headers = self.get_container_headers(&structure.name)?;
                self.headers.push(headers);
                self.headers.shrink_to_fit();

                
            }else{
                return Err(gerr("A container with the specified name already exists"))
            }
            },
            AST::CreateRow(mut structure) => {
                // CREATE ROW [col_nam][col_val] ON <container:name>
                let container = match self.container.get_mut(&structure.container) {
                    None => return Err(gerr(&format!("Container '{}' does not exist.", structure.container))),
                    Some(a) => a
                };
                if structure.col_nam.len() != structure.col_val.len() {
                    return Err(gerr(&format!(
                        "In CREATE ROW, expected {} values for the specified columns, but got {}",
                        structure.col_nam.len(),
                        structure.col_val.len()
                    )));
                }
                for i in &structure.col_nam{
                    if !container.column_names.contains(&i){
                        return Err(gerr(&format!("There is no column {} in the container {}",i,structure.container)))
                    }
                }

                let mut written_text : Vec<String> = Vec::new();
                let mut val: Vec<AlbaTypes> = container.columns.clone();
                for (index, col_name) in structure.col_nam.iter().enumerate() {
                    match container.column_names.iter().position(|c| c == col_name) {
                        Some(ri) => {
                            let input_val = structure.col_val.get(index)
                                .ok_or_else(|| gerr("Internal error: missing value during assignment."))?;
                            let expected_val = container.columns.get(ri)
                                .ok_or_else(|| gerr("Internal error: missing column type definition."))?;
                            let _ = discriminant(input_val) == discriminant(expected_val)
                                || (matches!(expected_val, AlbaTypes::Bigint(_)) && matches!(input_val, AlbaTypes::Int(_)));

                
                            if discriminant(input_val) == discriminant(expected_val) {
                                if discriminant(expected_val) == discriminant(&AlbaTypes::Text(String::new())){
                                    let mut code = generate_secure_code(self.settings.max_str_length);
                                    let txt_path = format!("{}/rf/{}",self.location,code);
                                    while fs::exists(&txt_path)?{
                                        let code_full = generate_secure_code(self.settings.max_str_length);
                                        code = code_full.chars().take(max_str_len).collect::<String>();

                                    }
                                    written_text.push(code.clone());
                                    let to_write = match structure.col_val.get(index).clone(){
                                        Some(a) => a.to_owned(),
                                        None => {return Err(gerr("failed to get the given value"))}
                                    };
                                    
                                    structure.col_val[index] = AlbaTypes::Text(code.clone());

                                    let mut txt_mvcc = match container.text_mvcc.lock(){
                                        Ok(a) => a,
                                        Err(e) => {
                                            return Err(gerr(&e.to_string()))
                                        }
                                    };
                                    let text_to_write : String = match to_write{
                                        AlbaTypes::Text(a) => a.to_string(),
                                        _ => "".to_string()
                                    };
                                    txt_mvcc.insert(code,(false,text_to_write));
                                    drop(txt_mvcc);
                                }else{
                                    val[ri] = input_val.clone();
                                }
                            } else {
                                return Err(gerr(&format!(
                                    "Type mismatch for column '{}': expected {:?}, got {:?}.",
                                    col_name, expected_val, input_val
                                )));
                            }
                        },
                        None => return Err(gerr(&format!(
                            "Column '{}' not found in container '{}'.",
                            col_name, structure.container
                        )))
                    }
                }
                container.push_row(&val)?;
                if self.settings.auto_commit{
                    container.commit().await?;
                }
            },
            AST::Search(structure) => {
                return Ok(self.query(structure.col_nam, structure.container, structure.conditions).await?)
            }
            _ =>{return Err(gerr("Failed to parse"));}
        }
    
        Ok(Query::new())
    }
    pub async fn execute(&mut self,input : &str) -> Result<Query, Error>{
        let ast = parse(input.to_owned())?;
        Ok(self.run(ast).await?)
    }
    
}
type Rows = (Vec<String>,Vec<Vec<AlbaTypes>>);

pub async fn connect(input_path : &str) -> Result<Database, Error>{
    let path : &str = if input_path.ends_with('/') {
        &input_path[..input_path.len()-1]
    }else{
        input_path
    };

    let db_path = PathBuf::from(path);
    println!("{}",path);
    if db_path.exists() {
        if !db_path.is_dir() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("`{}` exists but is not a directory", path),
            ));
        }
    } else {
        fs::create_dir_all(&db_path)?;
    }

    let mut db = Database{location:path.to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new()};
    if let Err(e) = db.load_settings(){
        eprintln!("err: load_settings");
        return Err(e)
    };if let Err(e) = db.load_containers().await{
        eprintln!("err: load_containers");
        return Err(e)
    };
    println!("{:?}",db.settings);
    return Ok(db)
}