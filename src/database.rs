use std::{collections::{BTreeMap, BTreeSet, HashMap}, fs, io::{Error, ErrorKind, Read, Write}, mem::discriminant, os::unix::fs::FileExt, path::PathBuf, str::FromStr, sync::Arc, thread};
use ahash::{AHashMap, AHashSet};
use base64::{alphabet, engine, Engine};
use futures::{StreamExt, TryStreamExt};
use lazy_static::lazy_static;
use serde::{Serialize,Deserialize};
use serde_yaml;
use crate::{container::{Container, New}, gerr, index_sizes::IndexSizes, lexer_functions::{AlbaTypes, Token}, logerr, loginfo, parser::parse, strix::{start_strix, Strix}, AlbaContainer, AST};
use rand::{Rng, distributions::Alphanumeric};
use regex::Regex;
use tokio::{net::{TcpListener, TcpStream}, sync::{OnceCell,RwLock}};
use std::time::Instant;
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
connections_port: 1515
data_port: 8989
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
    #[serde(rename="strict")]
    Strict,
    #[serde(rename="permissive")]
    Permissive,
}

#[derive(Serialize, Deserialize, Debug, Default)]
enum RequestHandling {
    #[default]
    #[serde(rename="sync")]
    Sync,
    #[serde(rename="asynchronous")]
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

const SECRET_KEY_PATH : &str = "TytoDB/.tytodb-keys";
const DATABASE_PATH : &str = "TytoDB";

fn database_path() -> String{
    let first = std::env::var("HOME").unwrap();
    return format!("{}/{}",first,DATABASE_PATH)
}
fn secret_key_path() -> String{
    let first = std::env::var("HOME").unwrap();
    return format!("{}/{}",first,SECRET_KEY_PATH)
}
/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////



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

pub const STRIX : OnceCell<Arc<RwLock<Strix>>> = OnceCell::const_new();

#[derive(Default,Debug)]
pub struct Database{
    location : String,
    settings : Settings,
    containers : Vec<String>,
    headers : Vec<(Vec<String>,Vec<AlbaTypes>)>,
    container : HashMap<String,Container>,
    queries : Arc<RwLock<HashMap<String,Query>>>,
    secret_keys : Arc<RwLock<HashMap<[u8;32],Vec<u8>>>>,
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



const SETTINGS_FILE : &str = "settings.yaml";
fn calculate_header_size(max_columns: usize) -> usize {
    let column_names_size = MAX_STR_LEN * max_columns;
    let column_types_size = max_columns;
    column_names_size + column_types_size
}

pub type WrittingQuery = BTreeMap<IndexSizes,Vec<AlbaTypes>>;

const PAGE_SIZE : usize = 100;
type QueryPage = (Vec<u64>,String);
pub type QueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>,Vec<Vec<AlbaTypes>>);
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query{
    pub rows: Rows,
    pub pages : Vec<QueryPage>,
    pub current_page : usize,
    pub column_names : Vec<String>,
    pub column_types : Vec<AlbaTypes>,
    pub id : String,
}
impl Query {
    pub fn duplicate(&self) -> Self {
        Query {
            rows: self.rows.clone(),
            pages: self.pages.clone(),
            current_page: self.current_page, 
            column_names: self.column_names.clone(),
            column_types: self.column_types.clone(),
            id: self.id.clone(),
        }
    }
}
impl Query{
    fn new(column_types : Vec<AlbaTypes>) -> Self{
        Query { rows: (Vec::new(),Vec::new()), pages: Vec::new(), current_page: 0, column_names: Vec::new(), column_types: column_types,id:generate_secure_code(100)}
    }
    fn new_none(column_types : Vec<AlbaTypes>) -> Self{
        Query { rows: (Vec::new(),Vec::new()), pages: Vec::new(), current_page: 0, column_names: Vec::new(), column_types: column_types,id:"".to_string()}
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
        self.rows = (container.column_names(),rows);
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
    pub fn push(&mut self,subject : (Vec<u64>,String)){
        self.pages.push(subject);
    }
}



fn condition_checker(row: &Vec<AlbaTypes>, col_names: &Vec<String>, conditions: &QueryConditions) -> Result<bool, Error> {
    if col_names.len() != row.len() {
        return Err(gerr(&format!("Row data does not match column names: expected {} columns, got {} values", col_names.len(), row.len())));
    }
    
    let mut indexes: HashMap<String, AlbaTypes> = HashMap::new();
    for (i, val) in row.iter().enumerate() {
        let string = match col_names.get(i) {
            Some(a) => a,
            None => return Err(gerr("Column name index out of bounds"))
        };
        indexes.insert(string.clone(), val.clone());
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

        let result = match (first, &b.2) {
            (AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
             AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s), 
             Token::String(val2)) => {
                match operator.as_str() {
                    "=" | "==" => s == *val2,
                    "!=" | "<>" => s != *val2,
                    "&>" => s.contains(val2),
                    "&&>" => s.to_lowercase().contains(&val2.to_lowercase()),
                    "&&&>" => {
                        let reg = Regex::new(val2).map_err(|e| gerr(&e.to_string()))?;
                        reg.is_match(&s)
                    },
                    _ => return Err(gerr(&format!("Invalid operator '{}' for string comparison", operator)))
                }
            },
            (AlbaTypes::Char(c), Token::String(val2)) => {
                let char_str = c.to_string();
                match operator.as_str() {
                    "=" | "==" => char_str == *val2,
                    "!=" | "<>" => char_str != *val2,
                    _ => return Err(gerr(&format!("Operator '{}' not supported for char comparison", operator)))
                }
            },
            (AlbaTypes::Bool(val1), Token::Bool(val2)) => {
                match operator.as_str() {
                    "=" | "==" => val1 == *val2,
                    "!=" | "<>" => val1 != *val2,
                    _ => return Err(gerr(&format!("Invalid operator '{}' for boolean comparison", operator)))
                }
            },
            (AlbaTypes::Int(val1), Token::Int(val)) => {
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
            (AlbaTypes::Bigint(val1), Token::Int(val2)) => {
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
            (AlbaTypes::Float(val1), Token::Float(val2)) => {
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
            (AlbaTypes::NONE, _) => return Err(gerr("Cannot compare NULL values")),
            _ => return Err(gerr("Type mismatch in condition")),
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
    async fn interact_with_ancient_query_bucket(bucket : &mut Vec<IndexSizes>,page : &mut Vec<u64>,container : &mut Container,query :&mut  Query,containername : &str,conditions: &QueryConditions) -> Result<(),Error>{
        if bucket.len() >= 40{
            let mut better_bucket : Vec<u64> = bucket.iter().map(|f| IndexSizes::to_usize(*f) as u64).collect();
            let rows = container.get_spread_rows(&mut better_bucket).await?;
            
            let column_names = container.column_names();
            for i in rows.iter().enumerate(){
                if !condition_checker(i.1, &column_names, conditions)?{
                    continue;
                }
                if let Some(ind) = better_bucket.get(i.0){
                    page.push(*ind);
                }
                if page.len() >= PAGE_SIZE{
                    query.push((page.clone(),containername.to_string()));
                    page.clear();
                }
            }
            
        }
        Ok(())
    }

    async fn ancient_query(&mut self, col_names: &Vec<String>, containers: &[AlbaContainer], conditions: &QueryConditions) -> Result<Query, Error> {
        if let AlbaContainer::Real(container_name) = &containers[0] {
            let mut container = match self.container.get_mut(container_name) {
                Some(a) => a,
                None => return Err(gerr(&format!("No container named {}", container_name))),
            };
            let query_rows = container.get_query_candidates(conditions).await?;

            if query_rows.is_empty() {
                let mut final_query = Query::new(container.columns_owned());
                final_query.column_names = col_names.clone();
                return Ok(final_query);
            }
    
            let mut final_query = Query::new(container.columns_owned());
            final_query.column_names = col_names.clone();
            
            let mut page: Vec<u64> = Vec::with_capacity(PAGE_SIZE); 
            let mut iterator = query_rows.iter();
            let mut bucket: Vec<IndexSizes> = Vec::new();
            
            while let Some(i) = iterator.next() {
                bucket.push(i.clone());
                Database::interact_with_ancient_query_bucket(&mut bucket, &mut page, &mut container, &mut final_query, container_name, conditions).await?;
            }
            Database::interact_with_ancient_query_bucket(&mut bucket, &mut page, &mut container, &mut final_query, container_name, conditions).await?;
    
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
        loginfo!("{:?}",query);
        return Ok(query)
    }
    fn set_default_settings(&self) -> Result<(), Error> {
        let path = format!("{}/{}", self.location,SETTINGS_FILE);
        if fs::metadata(&path).is_err() {
            let mut file = fs::File::create_new(&path)?;
            let content = DEFAULT_SETTINGS.to_string();
            file.write_all(content.as_bytes())?
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
            let count = (wedfygt.len().await? - wedfygt.headers_offset) / wedfygt.element_size as u64;
            if count < 1{ continue;}
            for i in 0..count{
                let mut wb = vec![0u8;wedfygt.element_size];
                if let Err(e) = wedfygt.file.read_exact_at(&mut wb, wedfygt.headers_offset as u64 + (wedfygt.element_size as u64*i as u64) ){
                    logerr!("{}",e);
                    continue;
                };
                if wb == vec![0u8;wedfygt.element_size]{
                    wedfygt.graveyard.write().await.insert(i);
                }
            }
        }
        loginfo!("headers: {:?}",self.headers);
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
            c.load_indexing().await?;
            
        }
        Ok(())
    }
    pub async fn setup(&self) -> Result<(),Error>{
        if !std::fs::exists(&database_path())?{
            loginfo!("database folder created");
            std::fs::create_dir(&database_path())?;
        }
        return Ok(())
    }
    fn load_settings(&mut self) -> Result<(), Error> {
        let dir = PathBuf::from(&self.location);
        let path = dir.join(SETTINGS_FILE);
        fs::create_dir_all(&dir)?;
        if path.exists() && fs::metadata(&path)?.is_dir() {
            fs::remove_dir(&path)?;
        }
        if !path.is_file() {
            loginfo!("not file");
            self.set_default_settings()?; 
        }
        let mut rewrite = true;
        loginfo!("settings-file-path: {}",path.display());
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
                let alba_type = if i < types_headers_bytes.len() {
                    let tb = types_headers_bytes[i];
                    AlbaTypes::from_id(tb)?
            
                } else {
                    // no header byte → NULL
                    AlbaTypes::NONE
                };
            
                column_types.push(alba_type);
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
            loginfo!("Started to create container!");
              if !match fs::exists(format!("{}/{}",self.location,structure.name)) {Ok(a)=>a,Err(bruh)=>{return Err(bruh)}}{
                let cn_len = structure.col_nam.len();
                let cv_len = structure.col_val.len();
                loginfo!("No container with the entered name");

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
                loginfo!("Finished processing container headers!");
                
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
                    column_val_bytes[i] = item.get_id()
                }
                loginfo!("Finished computing the container header bytes!");

                if let Err(e) = check_for_reference_folder(&self.location){
                    return Err(e)
                }
                loginfo!("Creating container file");
                let mut file = match tokio::fs::File::create(format!("{}/{}",self.location,structure.name)).await{
                    Ok(f)=>{
                        f}
                    ,Err(e)=>{return Err(e)}
                };

                let mut flattened: Vec<u8> = Vec::new();
                for arr in &column_name_bytes {
                    flattened.extend_from_slice(arr);
                }
                for arr in &column_val_bytes {
                    flattened.push(*arr);
                }

                loginfo!("Writting container headers...");
                match file.write_all(&flattened).await {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(e)
                    }
                };
                loginfo!("Container data written!");
                loginfo!("Creating container reference...");
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
                loginfo!("Container reference has been created!");
                return Ok(Query::new_none(Vec::new()))
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
                    if !container.column_names().contains(&i) {
                        return Err(gerr(&format!("There is no column {} in the container {}", i, structure.container)))
                    }
                }
            
                let mut val: Vec<AlbaTypes> = container.columns_owned();
                for (index, col_name) in structure.col_nam.iter().enumerate() {
                    match container.column_names().iter().position(|c| c == col_name) {
                        Some(ri) => {
                            let input_val = structure.col_val.get(index).cloned().unwrap();
                            let expected_val = container.columns()[ri].clone();
                            match (&expected_val, &input_val) {
                                (AlbaTypes::Text(_), AlbaTypes::Text(s)) => {
                                    let mut code = generate_secure_code(MAX_STR_LEN);
                                    let txt_path = format!("{}/rf/{}", self.location, code);
                                    while fs::exists(&txt_path)? {
                                        let code_full = generate_secure_code(MAX_STR_LEN);
                                        code = code_full.chars().take(MAX_STR_LEN).collect::<String>();
                                    }
                                    val[ri] = AlbaTypes::Text(code.clone());
                                    let mut mvcc = container.mvcc.write().await;
                                    mvcc.1.insert(code, (false, s.to_string()));
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
                let container = match self.container.get_mut(&structure.container){
                    Some(i) => i,
                    None => {return Err(gerr(&format!("There is no container named {}",structure.container)))}
                };
                let mut wquery: BTreeMap<IndexSizes, Vec<AlbaTypes>> = container.heavy_get_spread_rows(&mut container.get_query_candidates(&structure.conditions).await?).await?;

                let mut col_val_rel : AHashMap<usize,&AlbaTypes> = AHashMap::new();
                for (index,column_name) in structure.col_nam.iter().enumerate(){
                    for i in container.column_names().iter().enumerate(){
                        if *column_name == *i.1{
                            if let Some(s) = structure.col_val.get(index){
                                let _ = col_val_rel.insert(i.0,s);
                            }
                        } 
                    }
                }
                for i in wquery.iter_mut(){
                    for j in i.1.iter_mut().enumerate(){
                        if let Some(a) = col_val_rel.get(&j.0){
                            let b = (**a).clone();
                            *j.1 = b;
                        }
                    }
                }
                let data: Vec<(usize, Vec<AlbaTypes>)> = wquery.into_iter().map(|(k, v)| (k.as_usize(), v)).collect();

                container.indexes.edit((container.column_names(),data))?;

                if self.settings.auto_commit{
                    self.commit().await?;
                }
            },
            AST::DeleteRow(structure) => {
                let container = match self.container.get(&structure.container){
                    Some(i) => i,
                    None => {return Err(gerr(&format!("There is no container named {}",structure.container)))}
                };
                if let Some(conditions) = structure.conditions{
                    let container = match self.container.get_mut(&structure.container){
                        Some(i) => i,
                        None => {return Err(gerr(&format!("There is no container named {}",structure.container)))}
                    };
                    let wquery: BTreeMap<IndexSizes, Vec<AlbaTypes>> = container.heavy_get_spread_rows(&mut container.get_query_candidates(&conditions).await?).await?;
                    let mut indexes = AHashSet::new();
                    let mut mvcc = container.mvcc.write().await;
                    for i in wquery{
                        indexes.insert(i.0.as_usize());
                        mvcc.0.insert(i.0.as_u64(),(true,i.1.clone()));
                    }
                    container.indexes.kaboom_indexes_out(indexes)?;

                }else{
                    let mut row_group =  self.settings.memory_limit / container.element_size as u64;
                    let mut mvcc = container.mvcc.write().await;
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
                            mvcc.0.insert(id,(true,j.1.clone()));
                            
                        }
                    }
                }
                if self.settings.auto_commit{
                    self.commit().await?;
                }
            },
            AST::DeleteContainer(structure) => {
                if self.containers.contains(&structure.container){
                    let container = match self.container.get(&structure.container){
                        Some(a) => a,
                        None => {return Err(gerr(&format!("There is no database with the name {}",structure.container)))}
                    };
                    for i in container.column_names().iter().enumerate(){
                        if structure.container == *i.1{
                            self.containers.remove(i.0);
                        }
                    }
                    self.container.remove(&structure.container);
                    tokio::fs::remove_file(format!("{}/{}",self.location,structure.container)).await?;
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
                    let mut guard = self.queries.write().await;
                    guard
                        .remove(&cmd.id)
                        .expect("query must exist")
                };
                q.next(self).await?;
                let q1 = q.duplicate();
                self.queries.write().await.insert(cmd.id, q);
                return Ok(q1)
            },
            AST::QueryControlPrevious(cmd) => {
                let mut q = {
                    let mut guard = self.queries.write().await;
                    guard
                        .remove(&cmd.id)
                        .expect("query must exist")
                };
            
                q.previous(self).await?;
                let q2 = q.duplicate();
                self.queries.write().await.insert(cmd.id, q);
                return Ok(q2)
            }
            AST::QueryControlExit(cmd) => {
                
                let mut guard = self.queries.write().await;
                guard
                .remove(&cmd.id)
                .expect("query must exist");
                
                
            }
            // _ =>{return Err(gerr("Failed to parse"));}
        }
    
        Ok(Query::new_none(Vec::new()))
    }
    pub async fn execute(&mut self,input : &str,arguments : Vec<String>) -> Result<Query, Error>{
        
        let start = Instant::now();
        let ast = parse(input.to_owned(),arguments)?;
        let result = self.run(ast).await?;
        loginfo!("Performance: {}",start.elapsed().as_millis());
        Ok(result)
    }  
}


pub async fn connect() -> Result<Database, Error>{
    let dbp = database_path();
    let path : &str = if dbp.ends_with('/') {
        &dbp[..dbp.len()-1]
    }else{
        &dbp
    };

    let db_path = PathBuf::from(path);
    loginfo!("{}",path);
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

    if let Some(strix) = STRIX.get(){
        start_strix(strix.clone()).await;
    }

    let mut db = Database{location:path.to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new(),queries:Arc::new(RwLock::new(HashMap::new())),secret_keys:Arc::new(RwLock::new(HashMap::new()))};
    db.setup().await?;
    if let Err(e) = db.load_settings(){
        logerr!("err: load_settings");
        return Err(e)
    };if let Err(e) = db.load_containers().await{
        logerr!("err: load_containers");
        return Err(e)
    };
    loginfo!("{:?}",db.settings);
    return Ok(db)
}

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn handle_connections_tcp_inner(payload : Vec<u8>,dbref: Arc<RwLock<Database>>) -> Vec<u8>{
    let mut secret_key_hash : [u8;32] = [0u8;32];
    secret_key_hash.clone_from_slice(payload.as_slice());

    let secret_key = match dbref.read().await.secret_keys.read().await.get(&secret_key_hash){
        Some(a) => a.clone(),
        None => {
            let buffer : [u8;1] = [false as u8];
            logerr!("the given secret key are not registred");
            return buffer.to_vec()
        }
    };

    let mut buffer : Vec<u8> = Vec::new();
    let session_id = secret_key.clone();
    let hash = blake3::hash(&session_id).as_bytes().clone();
    let key = Key::<Aes256Gcm>::from_slice(secret_key.as_slice());
    let _ = session_secret_rel.write().await.insert(hash.clone(), session_id.clone());
    cipher_map.write().await.insert(hash.clone(),Aes256Gcm::new(key));

    if let Ok(a) = encrypt(&session_id, &secret_key_hash).await{
        buffer.push(true as u8);
        buffer.extend_from_slice(&a);
    }else{
        buffer.push(false as u8);
    }
    

    loginfo!("authenticated successfully\nlen:{}",buffer.len());
    buffer
    // let _ = socket.shutdown().await;

}

// async fn handle_connections_tcp_sync(listener : &TcpListener,ardb : Arc<RwLock<Database>>){
//     let (mut socket, addr) = match listener.accept().await{
//         Ok(a) => a,
//         Err(e) => {
//             logerr!("{}",e);
//             return
//         }
//     };
//     loginfo!("Accepted connection from: {}", addr);
//     //handle_connections_tcp_inner(&mut socket, ardb).await;
//     // if let Err(e) = socket.shutdown().await{
//     //     logerr!("{}",e);
//     // }
// }
// async fn handle_connections_tcp_parallel(listener : &TcpListener,ardb : Arc<RwLock<Database>>){
//     let (mut socket, addr) = match listener.accept().await{
//         Ok(a) => a,
//         Err(e) => {
//             logerr!("{}",e);
//             return
//         }
//     };
//     loginfo!("Accepted connection from: {}", addr);
//     tokio::task::spawn(async move {
//         handle_connections_tcp_inner(&mut socket, ardb).await;
//     });
    
//     // if let Err(e) = socket.shutdown().await{
//     //     logerr!("{}",e);
//     // }
// }


use aes_gcm::{aead::{Aead, KeyInit, OsRng}, aes::cipher::{self}, AeadCore, Key};
use aes_gcm::Aes256Gcm;

lazy_static!{
    static ref cipher_map : Arc<RwLock<AHashMap<[u8;32],aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>>>>> = Arc::new(RwLock::new(AHashMap::new()));
    static ref session_secret_rel : Arc<RwLock<AHashMap<[u8;32],Vec<u8>>>> = Arc::new(RwLock::new(AHashMap::new())); 
}

async fn encrypt(content : &[u8],secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
    let cm = cipher_map.read().await;
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(secret_key){
        a
    } else{
        return Err(())
    };
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); 
    let mut result : Vec<u8> = Vec::new();
    loginfo!("nonce_len:{}",nonce.len());
    result.extend_from_slice(&nonce.to_vec());
    result.extend_from_slice(&cipher.encrypt(&nonce, content.as_ref()).unwrap());
    Ok(result)
}
async fn decrypt(cipher_text : &[u8],secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
    let nonce = &cipher_text[0..12];
    let cipher_b = &cipher_text[12..];
    let cm = cipher_map.read().await;
    let cipher: &aes_gcm::AesGcm<aes_gcm::aes::Aes256, cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UInt<cipher::typenum::UTerm, cipher::consts::B1>, cipher::consts::B1>, cipher::consts::B0>, cipher::consts::B0>> = 
    if let Some(a) = cm.get(secret_key){
        a
    } else{
        return Err(())
    };
    match cipher.decrypt(nonce.into(), cipher_b.as_ref()){
        Ok(a) => Ok(a),
        Err(e) => {
            logerr!("{}",e);
            Err(())
        }
    }
}


#[derive(Deserialize)]
struct DataConnection{
    command : String,
    arguments : Vec<String>
}


#[derive(Serialize,Default)]
struct TytoDBResponse{
    #[serde(rename = "?")]
    content : String,
    #[serde(rename = "!")]
    success : u8
}

impl TytoDBResponse {
    async fn to_bytes(self,secret_key : &[u8;32]) -> Result<Vec<u8>,()>{
        println!("{}",self.content);
        let bytes = serde_json::to_vec(&self).unwrap();
        return if let Ok(a) = encrypt(&bytes, secret_key).await{
            Ok(a)
        }else{
            Err(())
        };
    }
}


async fn handle_data_tcp_inner(dbref: Arc<RwLock<Database>>,rc_payload:Vec<u8>) -> Vec<u8>{
    let size = rc_payload.len();
    if size <= 0{
        logerr!("the payload is too short | size :{}",size);
        return (0 as u64).to_be_bytes().as_slice().to_vec()
    }
    loginfo!("hdti step 1");
    let mut session_id : [u8;32] = [0u8;32];
    session_id.clone_from_slice(&rc_payload[..32]);
    loginfo!("hdti step 2");


    let ssr = session_secret_rel.read().await;
    let mut db = dbref.write().await;
    let mut payload: Vec<u8> = Vec::with_capacity(512);
    payload.extend_from_slice(&rc_payload[32..]);
    if let Some(_) = ssr.get(&session_id) {
        loginfo!("hdti step 3");
        
        
        payload = match decrypt(&payload, &session_id).await{
            Ok(a) => a,
            Err(_) => {
                return (0 as u64).to_be_bytes().as_slice().to_vec()
            }
        };
        loginfo!("hdti step 4");
        
                 
    } else {
        logerr!("No session secret found for session_id");
        payload.clear();
        return (0 as u64).to_be_bytes().as_slice().to_vec();
    }
    loginfo!("hdti step 5");
    let mut response: Vec<u8> = Vec::with_capacity(510);
    match serde_json::from_slice::<DataConnection>(&payload) {
        Ok(v) => {
            loginfo!("hdti step 6");
            match db.execute(&v.command,v.arguments).await {
                Ok(query_result) => {
                    loginfo!("hdti step 7");
                    db.queries.write().await.insert(query_result.id.clone(), query_result.clone());
                    loginfo!("hdti step 8");
                    match serde_json::to_string(&query_result) {
                        
                        Ok(q) => {
                            loginfo!("hdti step 9");
                            if let Ok(b) = (TytoDBResponse{
                                content:q,
                                success:1
                            }).to_bytes(&session_id).await{
                                let size = b.len() as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                                response.extend_from_slice(&b);
                            }else{
                                let size = 0 as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                            };
                            loginfo!("hdti step 10");
                            
                        }
                        Err(e) => {
                            logerr!("Failed to serialize query result: {}", e);
                            if let Ok(b) = (TytoDBResponse{
                                content:format!("Failed to serialize query result: {}", e),
                                success:0
                            }.to_bytes(&session_id).await){
                                let size = b.len() as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                                response.extend_from_slice(&b);
                            }else{
                                let size = 0 as u64;
                                response.extend_from_slice(&size.to_be_bytes());
                            }
                            
                        }
                    }
                }
                Err(e) => {
                    logerr!("Failed to execute command '{}': {}", v.command, e);
                    if let Ok(b) = (TytoDBResponse{
                        content:format!("Failed to execute command '{}': {}", v.command, e),
                        success:0
                    }.to_bytes(&session_id).await){
                        let size = b.len() as u64;
                        response.extend_from_slice(&size.to_be_bytes());
                        response.extend_from_slice(&b);
                        loginfo!("payload: {:?}",b);
                    }else{
                        if let Ok(b) = (TytoDBResponse{
                            content:e.to_string(),
                            success:1
                        }).to_bytes(&session_id).await{
                            let size = b.len() as u64;
                            response.extend_from_slice(&size.to_be_bytes());
                            response.extend_from_slice(&b);
                        }else{
                            let size = 0 as u64;
                            response.extend_from_slice(&size.to_be_bytes());
                        };
                    }
                }
            }
        }
        Err(e) => {
            if let Ok(b) = (TytoDBResponse{
                content:format!("Failed to deserialize payload '{}'", e),
                success:0
            }.to_bytes(&session_id).await){
                let size = b.len() as u64;
                response.extend_from_slice(&size.to_be_bytes());
                response.extend_from_slice(&b)
            }else{
                let size = 0 as u64;
                response.extend_from_slice(&size.to_be_bytes());
            }
        }
    }
    loginfo!("hdti step 10");
    if response.len() < 1{
        logerr!("empty response");
        return (0 as u64).to_be_bytes().as_slice().to_vec()
    }
    loginfo!("hdti step 11");
    return response;
}


use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::{BodyExt, Full};
use hyper::{body::Bytes, Method, StatusCode};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;

async fn handle_data(req: Request<hyper::body::Incoming>,dbref: Arc<RwLock<Database>>) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().to_owned();
    let frame_stream = match req.collect().await{
        Ok(v)=> {v.to_bytes().to_vec()},
        Err(e) => {
            logerr!("{}",e);
            let r = Response::builder().status(StatusCode::BAD_REQUEST).body(Full::from(Bytes::from("Invalid input"))).unwrap();
            return Ok(r)
        }
    };
    if method == Method::POST{
        return Ok(Response::new(Full::new(Bytes::from(handle_data_tcp_inner(dbref, frame_stream).await))))
    }else{
        Ok(Response::new(Full::new(Bytes::from(handle_connections_tcp_inner(frame_stream, dbref).await))))
    }

}
impl Database{
    pub async fn run_database(self) -> Result<(), Error>{
        let crazy_config = engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_encode_padding(true)
        .with_decode_padding_mode(engine::DecodePaddingMode::Indifferent);
        let eng = base64::engine::GeneralPurpose::new(&alphabet::Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/").unwrap(), crazy_config);

        if fs::exists(secret_key_path()).unwrap(){
            let mut buffer : Vec<u8> = Vec::new();
            fs::File::open(secret_key_path()).unwrap().read_to_end(&mut buffer)?;
            let val = match serde_yaml::from_slice::<Vec<String>>(&buffer){Ok(a)=>a,Err(e)=>{return Err(gerr(&e.to_string()))}};
            // let bv : Vec<Vec<u8>> = val.iter().map(|s|{
            //     match eng.decode(s){
            //         Ok(a)=>a,
            //         Err(e)=>{
            //             logerr!("{}",e);
            //         }
            //     }
            // }).collect();
            let mut bv : Vec<Vec<u8>> = Vec::new();
            for i in val{
                match eng.decode(i) {
                    Ok(a)=>{bv.push(a);},
                    Err(e)=>{
                        return Err(gerr(&e.to_string()))
                    }
                };
            }

            let mut sk = self.secret_keys.write().await;
            for i in bv{
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i);
            }
        }else{
            let mut file = fs::File::create_new(secret_key_path()).unwrap();
            let mut keys : Vec<Vec<u8>> = Vec::new();
            for _ in 0..self.settings.secret_key_count{
                keys.push(Aes256Gcm::generate_key(OsRng).to_vec());
            }
            let mut sk = self.secret_keys.write().await;
            for i in keys.iter(){
                sk.insert(blake3::hash(&i).as_bytes().to_owned(), i.clone());
            }

            let mut b64_list : Vec<String> = Vec::new();
            for i in keys{
                b64_list.push(eng.encode(i))
            }
            if let Err(e) = serde_yaml::to_writer(&mut file, &b64_list){
                logerr!("{}",e);
                return Err(gerr(&e.to_string()))
            };
            file.flush()?;
            file.sync_all()?;
        }
        let settings = &self.settings;
        let connection_tcp_url = format!("{}:{}",settings.ip,settings.connections_port);
        let data_tcp_url = format!("{}:{}",settings.ip,settings.data_port);
        loginfo!("connections:\ndata:{}\nconn:{}",data_tcp_url,connection_tcp_url);
        loginfo!("\n\n\n\n\n\n\tTytoDB is now running!\n\n\n\n\n\n");
        
        let mtx_db = Arc::new(RwLock::new(self));
        // loop {
            
        //     handle_connections_tcp_sync(&connections_tcp,mtx_db.clone()).await;
        //     handle_data_tcp(&rrr,mtx_db.clone()).await;
        // }

        let addr = SocketAddr::from_str(&data_tcp_url).unwrap();
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let c = mtx_db.clone();
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| {
                        let b = c.to_owned();
                        async move {
                            handle_data(req, b).await
                        }
                    }))
                    .await
                {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }

    }
}