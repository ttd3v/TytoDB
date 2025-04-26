/////////////////////////////////////////////////
/////////     DEFAULT_SETTINGS    ///////////////
/////////////////////////////////////////////////

pub const MAX_STR_LEN : usize = 128;
const DEFAULT_SETTINGS : &str = r#"
max_columns: 50
min_columns: 1
auto_commit: false            
memory_limit: 1048576000
ip: 127.0.0.1
connections_port: 153971
data_port: 893127
max_connections: 10
max_connection_requests_per_minute: 10
max_data_requests_per_minute: 10000000
on_insecure_rejection_delay_ms: 100
safety_level: strict # strict | permissive
request_handling: sync # sync | asynchronous
secret_key_count: 10
"#;
#[derive(Serialize, Deserialize, Debug, Default)]
enum SafetyLevel {
    #[default]
    Strict,
    Permissive,
}

#[derive(Serialize, Deserialize, Debug, Default)]
enum RequestHandling {
    #[default]
    Sync,
    Asynchronous,
}
#[derive(Serialize,Deserialize, Default,Debug)]
struct Settings{
    max_columns : u32,
    min_columns : u32,
    memory_limit : u64,
    auto_commit : bool,
    ip:String,
    connections_port: u32,
    data_port: u32,
    max_connections: u32,
    max_connection_requests_per_minute: u32,
    max_data_requests_per_minute: u32,
    on_insecure_rejection_delay_ms: u64,
    safety_level: SafetyLevel,
    request_handling: RequestHandling,
    secret_key_count: u64
}

const SECRET_KEY_PATH : &str = "~/.TytoDB.keys";
const DATABASE_PATH : &str = "~/TytoDB";

/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////


use std::{collections::{HashMap, HashSet}, fs, io::{Error, ErrorKind, Read, Write}, mem::discriminant, os::unix::fs::FileExt, path::PathBuf, sync::{Arc,Mutex}};

use base64::{alphabet, engine, Engine};
use lazy_static::lazy_static;
use serde::{Serialize,Deserialize};
use size_of;
use serde_yaml;
use crate::{container::{Container, New}, gerr, lexer_functions::{AlbaTypes, Token}, parser::parse, AlbaContainer, AST};
use rand::{Rng, distributions::Alphanumeric};
use regex::Regex;
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex as tmutx};

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


#[derive(Default)]
pub struct Database{
    location : String,
    settings : Settings,
    containers : Vec<String>,
    headers : Vec<(Vec<String>,Vec<AlbaTypes>)>,
    container : HashMap<String,Container>,
    connections : Arc<tmutx<HashSet<[u8;32]>>>,
    queries : Arc<tmutx<HashMap<String,Query>>>,
    secret_keys : Arc<tmutx<HashMap<[u8;32],Vec<u8>>>>,
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
const SETTINGS_FILE : &str = "settings.yaml";
fn calculate_header_size(max_columns: usize) -> usize {
    let column_names_size = MAX_STR_LEN * max_columns;
    let column_types_size = max_columns;
    column_names_size + column_types_size
}



const PAGE_SIZE : usize = 100;
type QueryPage = (Vec<u64>,String);
type QueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query{
    pub rows: Rows,
    pub pages : Vec<QueryPage>,
    pub current_page : usize,
    pub column_names : Vec<String>,
    column_types : Vec<AlbaTypes>,
    id : String,
}
impl Query{
    fn new(column_types : Vec<AlbaTypes>) -> Self{
        Query { rows: (Vec::new(),Vec::new()), pages: Vec::new(), current_page: 0, column_names: Vec::new(), column_types: column_types,id:generate_secure_code(100)}
    }
    pub fn join(&mut self, foreign: Query) {
        if foreign.column_types != self.column_types {
            return;
        }
        let mut foreign_vec = foreign.pages;
        for (i, _cn) in self.pages.iter_mut() {
            for (f_i, _f_cn) in foreign_vec.iter_mut() {
                if i.len() < PAGE_SIZE && !f_i.is_empty() {
                    while i.len() < PAGE_SIZE && !f_i.is_empty() {
                        if let Some(a) = f_i.pop() {
                            i.push(a);
                        }
                    }
                }
            }
        }
    }
    pub async fn load_rows(&mut self,database : &mut Database) -> Result<(),Error>{
        if self.pages.len() == 0{
            return Ok(())
        }
        let page = match self.pages.get(self.current_page) {
            Some(a) => a,
            None => return Err(gerr("There is no page"))
        };
        let container = match database.container.get(&page.1){
            Some(a) => a,
            None => {
                return Err(gerr(&format!("There is no container in the given database named {}",page.1)))
            }
        };
        let mut rows = Vec::new();
        for i in &page.0{
            match container.get_rows((*i,*i+1)).await?.get(0) {
                Some(a) => rows.push(a.clone()),
                None => {continue;}
            }
        }
        self.rows = (container.column_names.clone(),rows);
        Ok(())
    }
    pub async fn next(&mut self, database: &mut Database) -> Result<(), Error> {
        if self.pages.is_empty() {
            return Ok(());
        }
        let new_page = self.current_page.saturating_add(1);
        if new_page >= self.pages.len() {
            return Ok(()); 
        }
        self.current_page = new_page;
        self.load_rows(database).await?;
        Ok(())
    }

    pub async fn previous(&mut self, database: &mut Database) -> Result<(), Error> {
        if self.pages.is_empty() {
            return Ok(());
        }
        let new_page = self.current_page.saturating_sub(1);
        if self.current_page == 0 {
            return Ok(()); 
        }
        self.current_page = new_page;
        self.load_rows(database).await?;
        Ok(())
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
                    "&&>" => val1.to_lowercase().contains(val2.to_lowercase().as_str()),
                    "&&&>" => {
                        let reg = match Regex::new(&val2){
                            Ok(a) => a,
                            Err(e) => return Err(gerr(&e.to_string()))
                        };
                        reg.is_match(&val1)
                    },
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
    async fn ancient_query(&mut self, col_names: &Vec<String>, containers: &[AlbaContainer], conditions: &QueryConditions) -> Result<Query, Error> {
        
        if let AlbaContainer::Real(container_name) = &containers[0] {
            let container = match self.container.get(container_name) {
                Some(a) => a,
                None => return Err(gerr(&format!("No container named {}", container_name))),
            };
            let mut final_query = Query::new(container.columns.clone());
            let length = container.arrlen().await?;
            let mut page_size = (length * container.element_size as u64) / self.settings.memory_limit;
            if page_size < 1 {
                page_size = 1;
            }
            let mut cursor: u64 = 0;
            final_query.column_names = col_names.clone();
            let mut list : Vec<u64> = Vec::new();
            while cursor < length {
                let rows = container.get_rows((cursor, cursor + page_size)).await?;
    
                for (number, row) in rows.iter().enumerate() {
                    let row_index = cursor + number as u64;
                    if condition_checker(row, &container.column_names, conditions)? {
                        list.push(row_index);
                    }
                }
                cursor += page_size as u64;
            }
    
            final_query.pages = vec![(list,container_name.clone())];
            return Ok(final_query);
        }
        Err(gerr("No valid containers specified for query processing"))
    }

    async fn query_diver(&mut self, col_names: &Vec<String>, containers: &[AlbaContainer], conditions: &QueryConditions) -> Result<Query, Error> {  
        if containers.is_empty() {
            return Err(gerr("No valid containers specified for query processing"));
        }
        let mut result = self.ancient_query(col_names, &containers[0..1], conditions).await?;
        for i in 1..containers.len() {
            let single_container_slice = &containers[i..i+1];
            let query = self.ancient_query(col_names, single_container_slice, conditions).await?;
            result.join(query);
        }
        Ok(result)
    }
    pub async fn query(&mut self,col_names: Vec<String>,containers: Vec<AlbaContainer>,conditions:QueryConditions ) -> Result<Query, Error>{
        let mut query: Query = self.query_diver(&col_names, &containers, &conditions).await?;
        query.load_rows(self).await?;
        println!("{:?}",query);
        return Ok(query)
    }



    fn set_default_settings(&self) -> Result<(), Error> {
        let path = format!("{}/{}", self.location,SETTINGS_FILE);
        if !match fs::metadata(&path) { Ok(_) => true, Err(_) => false } {
            let mut file =fs::File::create_new(&path)?;
            let content = DEFAULT_SETTINGS.to_string();
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
                element_size += el.size()
            }
            self.container.insert(contain.to_string(),Container::new(&format!("{}/{}",self.location,contain),self.location.clone(), element_size, he.1, MAX_STR_LEN,calculate_header_size(self.settings.max_columns as usize) as u64,he.0.clone()).await?);
        }
        for (_,wedfygt) in self.container.iter(){
            let count = wedfygt.len().await? - wedfygt.headers_offset / wedfygt.element_size as u64;
            for i in 0..count{
                let mut wb = vec![0u8;wedfygt.element_size];
                wedfygt.file.read_exact_at(&mut wb, wedfygt.headers_offset as u64 + (wedfygt.element_size as u64*i as u64) )?;
                if wb == vec![0u8;wedfygt.element_size]{
                    wedfygt.graveyard.lock().await.insert(i);
                }
            }
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
        let strhs = MAX_STR_LEN * max_columns;
        let exists = fs::exists(&path)?;
        if exists{
            let header_size = calculate_header_size(self.settings.max_columns as usize);
            let file = fs::File::open(&path)?;
            let mut buffer : Vec<u8> = vec![0u8;header_size];
            file.read_exact_at(&mut buffer,0)?;
            let name_headers_bytes = &buffer[..strhs];
            let types_headers_bytes = &buffer[strhs..];
            let mut column_names: Vec<String> = Vec::with_capacity(max_columns);
            let mut column_types: Vec<AlbaTypes> = Vec::with_capacity(max_columns);

            for i in 0..max_columns {
                let start_pos = i * MAX_STR_LEN;
                let mut actual_length = 0;
                for j in 0..MAX_STR_LEN {
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
                        size if size == text_type_identifier(MAX_STR_LEN) => AlbaTypes::Text(String::new()),
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
                
                let mut column_name_bytes: Vec<Vec<u8>> = vec![vec![0u8; MAX_STR_LEN]; max_columns];
                let mut column_val_bytes: Vec<u8> = vec![0u8; max_columns];

                for (i,item) in column_name_headers.iter().enumerate(){
                    let bytes = item.as_bytes();
                    if bytes.len() > MAX_STR_LEN {
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
                        AlbaTypes::Text(_) => MAX_STR_LEN as u8,
                        AlbaTypes::NONE => 0,
                        AlbaTypes::Char(_) => from_usize_to_u8(size_of::<char>()),
                        AlbaTypes::NanoString(_) => from_usize_to_u8(10),
                        AlbaTypes::SmallString(_) => from_usize_to_u8(100),
                        AlbaTypes::MediumString(_) => from_usize_to_u8(500),
                        AlbaTypes::BigString(_) => from_usize_to_u8(2000),
                        AlbaTypes::LargeString(_) => from_usize_to_u8(3000),
                        AlbaTypes::NanoBytes(_) => from_usize_to_u8(10),
                        AlbaTypes::SmallBytes(_) => from_usize_to_u8(1000),
                        AlbaTypes::MediumBytes(_) => from_usize_to_u8(10_000),
                        AlbaTypes::BigSBytes(_) => from_usize_to_u8(100_000),
                        AlbaTypes::LargeBytes(_) => from_usize_to_u8(1_000_000),
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
                    element_size += el.size()
                }
                self.container.insert(structure.name.clone(), Container::new(&format!("{}/{}",self.location,structure.name),self.location.clone(), element_size, column_val_headers.clone(), MAX_STR_LEN,calculate_header_size(self.settings.max_columns as usize) as u64,column_name_headers.clone()).await?);
                if let Err(e) = self.save_containers(){return Err(gerr(&e.to_string()))};
                let headers = self.get_container_headers(&structure.name)?;
                self.headers.push(headers);
                self.headers.shrink_to_fit();

                
            }else{
                return Err(gerr("A container with the specified name already exists"))
            }
            },
            AST::CreateRow(structure) => {
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
                for i in &structure.col_nam {
                    if !container.column_names.contains(&i) {
                        return Err(gerr(&format!("There is no column {} in the container {}", i, structure.container)))
                    }
                }
            
                let mut val: Vec<AlbaTypes> = container.columns.clone();
                for (index, col_name) in structure.col_nam.iter().enumerate() {
                    match container.column_names.iter().position(|c| c == col_name) {
                        Some(ri) => {
                            let input_val = structure.col_val.get(index).cloned().unwrap();
                            let expected_val = container.columns[ri].clone();
                            match (&expected_val, &input_val) {
                                (AlbaTypes::Text(_), AlbaTypes::Text(s)) => {
                                    let mut code = generate_secure_code(MAX_STR_LEN);
                                    let txt_path = format!("{}/rf/{}", self.location, code);
                                    while fs::exists(&txt_path)? {
                                        let code_full = generate_secure_code(MAX_STR_LEN);
                                        code = code_full.chars().take(MAX_STR_LEN).collect::<String>();
                                    }
                                    val[ri] = AlbaTypes::Text(code.clone());
                                    let mut txt_mvcc = container.text_mvcc.lock().await;
                                    txt_mvcc.insert(code, (false, s.to_string()));
                                },
                                (AlbaTypes::Bigint(_), AlbaTypes::Int(i)) => {
                                    val[ri] = AlbaTypes::Bigint(*i as i64);
                                },
                                _ if discriminant(&input_val) == discriminant(&expected_val) => {
                                    val[ri] = input_val;
                                },
                                _ => {
                                    return Err(gerr(&format!(
                                        "Type mismatch for column '{}': expected {:?}, got {:?}.",
                                        col_name, expected_val, input_val
                                    )));
                                }
                            }
                        },
                        None => return Err(gerr(&format!(
                            "Column '{}' not found in container '{}'.",
                            col_name, structure.container
                        ))),
                    }
                }
                container.push_row(&val).await?;
                if self.settings.auto_commit {
                    container.commit().await?;
                }
            },
            AST::Search(structure) => {
                return Ok(self.query(structure.col_nam, structure.container, structure.conditions).await?)
            },
            AST::EditRow(structure) => {
                let container = match self.container.get(&structure.container){
                    Some(i) => i,
                    None => {return Err(gerr(&format!("There is no container named {}",structure.container)))}
                };
                let mut values = container.columns.clone();
                let mut hashmap : HashMap<String,usize> = HashMap::new(); 
                for (index,value) in container.column_names.iter().enumerate(){
                    hashmap.insert(value.to_string(), index);
                }
                for i in structure.col_nam.iter().enumerate(){
                    if i.0 >= structure.col_val.len(){
                        continue;
                    }
                    let value: &AlbaTypes = &structure.col_val[i.0];
                    match hashmap.get(i.1){
                        Some(a) => {
                            values[*a] = values[*a].try_from_existing(value.clone())?;
                        },
                        None => continue
                    }
                }
                
                let mut row_group =  self.settings.memory_limit / container.element_size as u64;
                let mut mvcc = container.mvcc.lock().await;
                let text_mvcc = container.text_mvcc.lock().await;
                if row_group < 1{
                    row_group = 1
                }
                let arrl = container.arrlen().await?;
                if row_group > arrl{
                    row_group = arrl
                }
                for i in 0..(arrl/row_group){
                    let li = container.get_rows(((i-1)*row_group,i*row_group)).await?;
                    for j in li.iter().enumerate(){
                        let id = (j.0 as u64*i as u64 * row_group as u64) as u64;
                        match mvcc.get(&id){
                            Some(a) => {
                                if !a.0 && condition_checker(&a.1, &container.column_names, &structure.conditions)?{
                                    mvcc.insert(id,(false,values.clone()));
                                }
                            },
                            None => {
                                if condition_checker(&j.1, &container.column_names, &structure.conditions)?{
                                    mvcc.insert(id,(false,values.clone()));
                                } 
                            }
                        }
                    }
                }
                if self.settings.auto_commit{
                    drop(mvcc);
                    drop(text_mvcc);
                    self.commit().await?;
                }
            },
            AST::DeleteRow(structure) => {
                let container = match self.container.get(&structure.container){
                    Some(i) => i,
                    None => {return Err(gerr(&format!("There is no container named {}",structure.container)))}
                };
                
                let mut row_group =  self.settings.memory_limit / container.element_size as u64;
                let mut mvcc = container.mvcc.lock().await;
                if row_group < 1{
                    row_group = 1
                }
                let arrl = container.arrlen().await?;
                if row_group > arrl{
                    row_group = arrl
                }
                for i in 0..(arrl/row_group){
                    let li = container.get_rows(((i-1)*row_group,i*row_group)).await?;
                    for j in li.iter().enumerate(){
                        let id = (j.0 as u64*i as u64 * row_group as u64) as u64;
                        match &structure.conditions{
                            Some(conditions) => {
                                if condition_checker(&j.1, &container.column_names, &conditions)?{
                                    mvcc.insert(id,(true,j.1.clone()));
                                } 
                            },
                            None => {
                                mvcc.insert(id,(true,j.1.clone()));
                            }
                        }
                        
                    }
                }
                if self.settings.auto_commit{
                    drop(mvcc);
                    self.commit().await?;
                }
            },
            AST::DeleteContainer(structure) => {
                if self.containers.contains(&structure.container){
                    let container = match self.container.get(&structure.container){
                        Some(a) => a,
                        None => {return Err(gerr(&format!("There is no database with the name {}",structure.container)))}
                    };
                    for i in container.column_names.iter().enumerate(){
                        if structure.container == *i.1{
                            self.containers.remove(i.0);
                        }
                    }
                    self.container.remove(&structure.container);
                    fs::remove_file(format!("{}/{}",self.location,structure.container))?;
                    self.save_containers()?;

                }else{
                    return Err(gerr(&format!("There is no database with the name {}",structure.container)))
                }
            },
            AST::Commit(structure) => {
                match structure.container{
                    Some(container) => {
                        match self.container.get_mut(&container){
                            Some(a) => {
                                a.commit().await?;
                                return Ok(Query::new(Vec::new()))
                            },
                            None => {
                                return Err(gerr(&format!("There is no container named {}",container)))
                            }
                        }
                    },
                    None => {
                        self.commit().await?;
                    }
                }
            },
            AST::Rollback(structure) => {
                match structure.container{
                    Some(container) => {
                        match self.container.get_mut(&container){
                            Some(a) => {
                                a.rollback().await?;
                                return Ok(Query::new(Vec::new()))
                            },
                            None => {
                                return Err(gerr(&format!("There is no container named {}",container)))
                            }
                        }
                    },
                    None => {
                        self.rollback().await?;
                    }
                }
            },
            AST::QueryControlNext(cmd) => {
                let mut q = {
                    let mut guard = self.queries.lock().await;
                    guard
                        .remove(&cmd.id)
                        .expect("query must exist")
                };
                q.next(self).await?;
                self.queries.lock().await.insert(cmd.id, q);
            },
            AST::QueryControlPrevious(cmd) => {
                let mut q = {
                    let mut guard = self.queries.lock().await;
                    guard
                        .remove(&cmd.id)
                        .expect("query must exist")
                };
            
                q.previous(self).await?;
            
                self.queries.lock().await.insert(cmd.id, q);
            }
            // _ =>{return Err(gerr("Failed to parse"));}
        }
    
        Ok(Query::new(Vec::new()))
    }
    pub async fn execute(&mut self,input : &str,arguments : Vec<String>) -> Result<Query, Error>{
        let ast = parse(input.to_owned(),arguments)?;
        Ok(self.run(ast).await?)
    }
    
}

type Rows = (Vec<String>,Vec<Vec<AlbaTypes>>);

pub async fn connect() -> Result<Database, Error>{
    let path : &str = if DATABASE_PATH.ends_with('/') {
        &DATABASE_PATH[..DATABASE_PATH.len()-1]
    }else{
        DATABASE_PATH
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

    let mut db = Database{location:path.to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new(),queries:Arc::new(tmutx::new(HashMap::new())),connections:Arc::new(tmutx::new(HashSet::new())),secret_keys:Arc::new(tmutx::new(HashMap::new()))};
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

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SESSION_ID_LENGTH : usize = 32;

async fn handle_connections_tcp_inner(socket : &mut TcpStream,arc_db: Arc<tmutx<Database>>){
    let mut buffer: [u8; 32] = [0; 32];
    match socket.read(&mut buffer).await {
        Ok(_) => {
            let db: tokio::sync::MutexGuard<'_, Database> = arc_db.lock().await;
            let mut content : Vec<u8> = Vec::new();
            
            if db.secret_keys.lock().await.get(&buffer).is_some(){
                let id: [u8; 32] = blake3::hash(generate_secure_code(SESSION_ID_LENGTH).as_bytes()).as_bytes().to_owned();
                if db.connections.lock().await.insert(id.clone()){
                    content = id.to_vec();
                }
            }
            let _ = socket.write_all(&content).await;
        }
        Err(e) => eprintln!("Failed to read from socket: {}", e),
    };
}

async fn handle_connections_tcp(listener : TcpListener,ardb : Arc<tmutx<Database>>,parallel : bool){
    loop {
        let (mut socket, addr) = listener.accept().await.unwrap();
        println!("Accepted connection from: {}", addr);
        let arc_db: Arc<tmutx<Database>> = ardb.clone(); 
        if parallel{
            tokio::spawn(async move {
                handle_connections_tcp_inner(&mut socket, arc_db).await        
            });
        }else{
            handle_connections_tcp_inner(&mut socket, arc_db).await
        }
    }
}

use aes_gcm::{aead::{Aead, KeyInit, OsRng}, aes::cipher::{self}, AeadCore, Key};
use aes_gcm::Aes256Gcm;
use lzma::{compress as lzma_compress,decompress as lzma_decompress};

lazy_static!{
    static ref cipher_map : Arc<Mutex<HashMap<Vec<u8>,aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>>>>> = Arc::new(Mutex::new(HashMap::new()));
    static ref session_secret_rel : Arc<tmutx<HashMap<[u8;32],Vec<u8>>>> = Arc::new(tmutx::new(HashMap::new()));
    static ref session_id_curr_query : Arc<tmutx<HashMap<[u8;32],Query>>> = Arc::new(tmutx::new(HashMap::new()));
}

fn encrypt(content : &[u8],secret_key : &[u8]) -> Vec<u8>{
    let key = Key::<Aes256Gcm>::from_slice(secret_key);
    let mut cm = cipher_map.lock().unwrap();
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(&key.to_vec()){
        a
    } else{
        cm.insert(key.to_vec(), Aes256Gcm::new(key));
        drop(cm);
        return encrypt(content, secret_key)
    };
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); 
    let mut result : Vec<u8> = Vec::new();
    result.extend_from_slice(&nonce.to_vec());
    result.extend_from_slice(&cipher.encrypt(&nonce, content.as_ref()).unwrap());
    result
}
fn decrypt(cipher_text : &[u8],secret_key : &[u8]) -> Result<Vec<u8>,()>{
    let key = Key::<Aes256Gcm>::from_slice(secret_key);
    let nonce = &cipher_text[0..12];
    let cipher_b = &cipher_text[12..];
    let mut cm = cipher_map.lock().unwrap();
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(&key.to_vec()){
        a
    } else{
        cm.insert(key.to_vec(), Aes256Gcm::new(key));
        drop(cm);
        return decrypt(cipher_text, secret_key)
    };
    match cipher.decrypt(nonce.into(), cipher_b.as_ref()){
        Ok(a) => Ok(a),
        Err(_) => Err(())
    }
}


#[derive(Deserialize)]
struct DataConnection{
    command : String,
    arguments : Vec<String>
}
fn interpolate(x: u32) -> u32 {
    1 + 3 * x.max(0).min(1)
}

async fn handle_data_tcp_inner(socket : &mut TcpStream,arc_db: Arc<tmutx<Database>>){
    let mut buffer: Vec<u8> = Vec::with_capacity(64);
    let mut response: Vec<u8> = Vec::new();
    match socket.read_to_end(&mut buffer).await {
        Ok(_) => {
            if buffer.len() < 32 {
                eprintln!("Received data is too short: {} bytes", buffer.len());
                let _ = socket.write_all(&response).await;
                return;
            }
            let session_id: &[u8] = &buffer[..32];
            let cipher_payload = &buffer[32..];
            let mut db = arc_db.lock().await;
            let secrets_lock = db.secret_keys.lock().await;
            let ssr = session_secret_rel.lock().await;
            let secrets: HashMap<[u8; 32], Vec<u8>> = secrets_lock.clone();

            let mut payload: Vec<u8> = Vec::new();
            let mut secret_from_client: Vec<u8> = Vec::with_capacity(32);
            if let Some(sec) = ssr.get(session_id) {
                match decrypt(cipher_payload, sec) {
                    Ok(k) => {
                        secret_from_client = sec.to_vec();
                        payload = match lzma_decompress(&k) {
                            Ok(a) => a,
                            Err(e) => {
                                eprintln!("Failed to decompress payload with session secret: {}", e);
                                let _ = socket.write_all(&response).await;
                                return;
                            }
                        };
                    }
                    Err(_) => {
                        eprintln!("Decryption failed with session secret for session_id: {:?}", session_id);
                        let mut b = false;
                        for (_, secret) in &secrets {
                            match decrypt(cipher_payload, secret) {
                                Ok(k) => {
                                    secret_from_client = secret.clone();
                                    payload = match lzma_decompress(&k) {
                                        Ok(a) => a,
                                        Err(e) => {
                                            eprintln!("Failed to decompress payload with secret key: {}", e);
                                            let _ = socket.write_all(&response).await;
                                            return;
                                        }
                                    };
                                    b = true;
                                    break;
                                }
                                Err(_) => continue,
                            }
                        }
                        if !b {
                            eprintln!("Decryption failed with all available secret keys");
                            let _ = socket.write_all(&response).await;
                            return;
                        }
                    }
                }
            } else {
                eprintln!("No session secret found for session_id: {:?}", session_id);
                let _ = socket.write_all(&response).await;
                return;
            }

            drop(secrets_lock);
            match serde_json::from_slice::<DataConnection>(&payload) {
                Ok(v) => {
                    match db.execute(&v.command,v.arguments).await {
                        Ok(query_result) => {
                            db.queries.lock().await.insert(query_result.id.clone(), query_result.clone());
                            drop(db);
                            match serde_json::to_string(&query_result) {
                                Ok(q) => {
                                    let bytes = q.as_bytes();
                                    match lzma_compress(bytes, interpolate(bytes.len() as u32)) {
                                        Ok(a) => {
                                            response = encrypt(&a, &secret_from_client);
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to compress query result: {}", e);
                                            let _ = socket.write_all(&response).await;
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to serialize query result: {}", e);
                                    let _ = socket.write_all(&response).await;
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to execute command '{}': {}", v.command, e);
                            let _ = socket.write_all(&response).await;
                            return;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to deserialize payload: {}", e);
                    let _ = socket.write_all(&response).await;
                    return;
                }
            }

            if let Err(e) = socket.write_all(&response).await {
                eprintln!("Failed to write response to socket: {}", e);
            }
        }
        Err(e) => eprintln!("Failed to read from socket: {}", e),
    }
}

async fn handle_data_tcp(listener: TcpListener, arc_db: Arc<tmutx<Database>>,parallel : bool) {
    loop {
        let (mut socket, addr) = match listener.accept().await {
            Ok((socket, addr)) => (socket, addr),
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
                continue;
            }
        };
        println!("Accepted connection from: {}", addr);
        let arc_db: Arc<tmutx<Database>> = arc_db.clone();
        if parallel{
            tokio::spawn(async move {
                handle_data_tcp_inner(&mut socket, arc_db).await
            }); 
        }else{
            handle_data_tcp_inner(&mut socket, arc_db).await
        }
        
    }
}
impl Database{
    pub async fn run_database(self){
        let crazy_config = engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_encode_padding(true)
        .with_decode_padding_mode(engine::DecodePaddingMode::RequireNone);
        let eng = base64::engine::GeneralPurpose::new(&alphabet::Alphabet::new("+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").unwrap(), crazy_config);

        if fs::exists(SECRET_KEY_PATH).unwrap(){
            let mut buffer : Vec<u8> = Vec::new();
            fs::File::open(SECRET_KEY_PATH).unwrap().read_to_end(&mut buffer).unwrap();
            let val = serde_yaml::from_slice::<Vec<String>>(&buffer).unwrap();
            let bv : Vec<Vec<u8>> = val.iter().map(|s|{eng.decode(s).unwrap()}).collect();
            let mut sk = self.secret_keys.lock().await;
            for i in bv{
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i);
            }
        }else{
            let mut file = fs::File::create_new(SECRET_KEY_PATH).unwrap();
            let mut keys : Vec<Vec<u8>> = Vec::new();
            for _ in 0..self.settings.secret_key_count{
                keys.push(Aes256Gcm::generate_key(OsRng).to_vec());
            }
            let mut sk = self.secret_keys.lock().await;
            for i in keys.iter(){
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i.clone());
            }

            let mut b64_list : Vec<String> = Vec::new();
            for i in keys{
                b64_list.push(eng.encode(i))
            }
            serde_yaml::to_writer(&mut file, &b64_list).unwrap();
            file.flush().unwrap();
            file.sync_all().unwrap();
        }
        
        let parallel = if let RequestHandling::Asynchronous = self.settings.request_handling{
            true
        }else{
            false
        } ;

        let settings = &self.settings;
        let connections_tcp: TcpListener = TcpListener::bind(format!("{}:{}",settings.ip,settings.connections_port)).await.unwrap();
        let data_tcp = TcpListener::bind(format!("{}:{}",settings.ip,settings.data_port)).await.unwrap();
        let mtx_db = Arc::new(tmutx::new(self));
        let connections_task = tokio::spawn(handle_connections_tcp(connections_tcp,mtx_db.clone(),parallel));
        let data_task = tokio::spawn(handle_data_tcp(data_tcp,mtx_db,parallel));
        if let Err(e) = tokio::try_join!(connections_task, data_task){
            eprintln!("Error: {}",e);
        }
    }
}