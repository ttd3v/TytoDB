use std::{collections::HashMap, fmt::format, fs::{self, File}, io::{Error, Read, Write}, mem::discriminant, os::unix::fs::FileExt, sync::{Arc, Mutex}};
use serde::{Serialize,Deserialize};
use size_of;
use serde_yaml;
use atomic_write_file::AtomicWriteFile;
use crate::{gerr, lexer_functions::AlbaTypes, parse, AST};
use rand::{Rng, distributions::Alphanumeric};

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
    headers_offset : u64,
    column_names : Vec<String>,
    path : String,
}

trait New {
    fn new(path : &str,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> where Self: Sized ;
}

impl New for Container {
    fn new(path : &str,element_size : usize, columns : Vec<AlbaTypes>,str_size : usize,headers_offset : u64,column_names : Vec<String>) -> Result<Self,Error> {
        Ok(Container{
            file : fs::File::open(path)?,
            element_size,
            columns,
            str_size,
            mvcc: Arc::new(Mutex::new(HashMap::new())),
            headers_offset ,
            column_names,
            path:path.to_string()
        })
    }
}

impl Container{
    fn len(&self) -> Result<u64,Error>{
        Ok(self.file.metadata()?.len())
    }
    fn arrlen(&self) -> Result<u64,Error>{
        let length = self.len()?;
        Ok((length-self.headers_offset)/self.element_size as u64)
    }
    pub fn get_next_addr(&self) -> Result<u64, Error> {
        let mut value = self.arrlen()? + 1;
        let mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        let mut max_key = 0;
        for (&key, (deleted, _)) in mvcc_guard.iter() {
            if *deleted {
                return Ok(key);
            }
            if key > max_key {
                max_key = key;
            }
        }
        if max_key >= value {
            value = max_key + 1;
        }
        Ok(value)
    }    
    pub fn push_row(&mut self, data : &Vec<AlbaTypes>) -> Result<(),Error>{
        let ind = self.get_next_addr()?;
        let mut mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        mvcc_guard.insert(ind, (false,data.clone()));
        Ok(())
    }
    fn commit(&mut self) -> Result<(),Error>{
        let mut mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        let mut insertions : Vec<(u64,Vec<AlbaTypes>)> = Vec::new();
        let mut deletes : Vec<(u64,Vec<AlbaTypes>)> = Vec::new();
        for (index,value) in mvcc_guard.iter(){
            let v = (*index,value.1.clone());
            if value.0{
                deletes.push(v);
            }else{
                insertions.push(v);
            }
        }
        mvcc_guard.clear();

        insertions.sort_by_key(|(index, _)| *index);
        deletes.sort_by_key(|(index, _)| *index);


        let mut file = AtomicWriteFile::open(self.path.clone())?;
        let hdr_off = self.headers_offset;
        let row_sz  = self.element_size as u64;
        let mut buf = vec![0u8; self.element_size];

        for item in insertions{
            file.write_all_at(&self.serialize_row(&item.1)?.as_slice(), item.0)?;
        }
        
        let mut total = self.arrlen()?; 
        for del_idx in &deletes {
            for i in (del_idx.0 + 1)..total {
                let from = hdr_off + i * row_sz;
                let to   = hdr_off + (i - 1) * row_sz;
                self.file.read_exact_at(&mut buf, from)?;
                file.write_all_at(&buf, to)?;
            }
            total -= 1;
        }
    
        let new_len = hdr_off + total * row_sz;
        file.set_len(new_len)?;
        file.flush()?;       
        file.sync_all()?;   
        file.commit()?;     


        Ok(())

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
    
    async fn get_element(&self, buf_index: u64) -> Result<Vec<AlbaTypes>,Error> {
        let mvcc_guard = self.mvcc.lock().map_err(|e| gerr(&e.to_string()))?;
        if let Some((deleted, value)) = mvcc_guard.get(&buf_index) {
            if !*deleted {
                return Ok(value.clone());
            }
        }

        let mut buf = vec![0u8;self.element_size];
        self.file.read_exact_at(&mut buf, (self.headers_offset as usize +self.element_size*(buf_index as usize)) as u64)?;
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
                        .take_while(|&&c| c != 0) // para parar no primeiro byte nulo
                        .cloned()
                        .collect::<Vec<u8>>();

                    let str_id = match String::from_utf8(trimmed) {
                        Ok(s) => s,
                        Err(e) => return Err(gerr(&format!("Erro ao converter bytes para String: {}", e))),
                    };
                    value.push(AlbaTypes::Text(str_id));
                },
                AlbaTypes::NONE => {
                    value.push(AlbaTypes::NONE);
                }
            }
        }
        Ok(value)
    }
}

fn calculate_header_size(max_str_len: usize, max_columns: usize) -> usize {
    let column_names_size = max_str_len * max_columns;
    let column_types_size = max_columns;
    column_names_size + column_types_size
}
impl Database {
    fn set_default_settings(&self) -> Result<(), Error> {
        let path = format!("{}/settings.yaml", self.location);
        if !match fs::metadata(&path) { Ok(_) => true, Err(_) => false } {
            let mut file = match fs::File::create(path) {
                Ok(f) => f,
                Err(e) => return Err(e),
            };
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
    fn load_containers(&mut self) -> Result<(), Error> {
        let path = std::path::PathBuf::from(&self.location).join("containers.yaml");

        // If not exist, write empty Vec<String>
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
            self.container.insert(contain.to_string(),Container::new(contain, element_size, he.1, self.settings.max_str_length,calculate_header_size(self.settings.max_str_length,self.settings.max_columns as usize) as u64,he.0.clone())?);
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
    fn load_settings(&mut self) -> Result<(), Error>{
        let path = format!("{}/settings.yaml",self.location);
        if match fs::exists(&path){Ok(a)=>a,Err(e)=>{return Err(e)}}{
            let mut file = match fs::File::open(path){
                Ok(a)=>a,
                Err(e)=>{
                    return Err(e)
                }
            };
            let mut raw_settings : String = String::new();
            match file.read_to_string(&mut raw_settings){
                Ok(_)=>{},
                Err(e) => {
                    return Err(e)
                }
            }
            let mut settings = match serde_yaml::from_str::<Settings>(&raw_settings){Ok(a)=>a,Err(e)=>{return Err(gerr(&e.to_string()))}};
            let mut rewrite = false;

            if settings.max_columns <= settings.min_columns{
                settings.min_columns = 1;
                rewrite = true
            }
            if settings.max_columns <= 1{
                settings.max_columns = 10;
                rewrite = true
            }

            if settings.min_columns > settings.max_columns{
                settings.min_columns = 1;
                rewrite = true
            }

            if settings.memory_limit < 1_048_576{
                settings.memory_limit = 1_048_576;
                rewrite = true
            }

            if settings.max_str_length < 1{
                settings.max_str_length = 1;
                rewrite = true
            }
            if rewrite{
                
                match file.write_all(format!(r#"
# WARNING: If you change 'max_columns' or 'max_str_length' after creating a container, it might not work until you revert the changes.
max_columns: {}
min_columns: {}
max_str_length: {}

auto_commit: {}
                
# Memory limit: defines how much memory the database can use during operations. Setting a higher value might improve performance, but exceeding hardware limits could have the opposite effect.
memory_limit: {}
                "#,settings.max_columns,settings.min_columns,settings.max_str_length,settings.auto_commit,settings.memory_limit).as_bytes()){
                    Ok(_)=>{},
                    Err(e)=>{return Err(e)}
                };
            }
            self.settings = settings;

        }else{
            if let Err(e) = self.set_default_settings(){
                return Err(e)
            }
            if let Err(e) = self.load_settings(){
                return Err(e)
            }
        }
        return Ok(())
    }
    
    fn get_container_headers(&self, container_name: &str) -> Result<(Vec<String>, Vec<AlbaTypes>), Error> {
        let path = format!("{}/{}",self.location,container_name);
        let max_columns : usize = self.settings.max_columns as usize;
        let max_str_len : usize = self.settings.max_str_length;
        let strhs = (max_str_len * max_columns);
        let exists = fs::exists(&path)?;
        if exists{
            let header_size = calculate_header_size(self.settings.max_str_length,self.settings.max_columns as usize);
            let file = fs::File::open(&path)?;
            let mut buffer : Vec<u8> = vec![0u8;header_size];
            file.read_exact_at(&mut buffer, 0)?;
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

    pub fn execute(&mut self,input : &str) -> Result<(), Error>{
        let ast = parse(input.to_owned())?;
        let min_column : usize = self.settings.min_columns as usize;
        let max_columns : usize = self.settings.max_columns as usize;
        let max_str_len : usize = self.settings.max_str_length;

        match ast{
            AST::CreateContainer(structure) => {
              if !match fs::exists(format!("{}/{}",self.location,structure.name)) {Ok(a)=>a,Err(bruh)=>{return Err(bruh)}}{
                let cn_len = structure.col_nam.len();
                let cv_len = structure.col_val.len();

                if cn_len != cv_len{
                    return Err(gerr("Mismatch between number of column names and column values"))
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
                        AlbaTypes::Text(_) => from_usize_to_u8(size_of::<i64>()),
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
                if let Err(e) = self.save_containers(){return Err(gerr(&e.to_string()))};
                let headers = self.get_container_headers(&structure.name)?;
                self.headers.push(headers);
                self.headers.shrink_to_fit();

                
            }else{
                return Err(gerr("A container with the specified name already exists"))
            }
            },
            AST::CreateRow(structure) => {
                // CREATE ROW [col_nam][col_val] ON <container:name>
                let container = match self.container.get_mut(&structure.container) {
                    None => return Err(gerr(&format!("Container '{}' does not exist.", structure.container))),
                    Some(a) => a
                };
                if structure.col_nam.len()!=structure.col_val.len(){
                    return Err(gerr(&format!(
                        "Column count mismatch: expected {} values for {} columns, but got {}.",
                        structure.col_nam.len(),
                        structure.col_nam.join(", "),
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
                
                            if discriminant(input_val) == discriminant(expected_val) {
                                if discriminant(expected_val) == discriminant(&AlbaTypes::Text(String::new())){
                                    let mut code = generate_secure_code(self.settings.max_str_length);
                                    let txt_path = format!("{}/rf/{}",self.location,code);
                                    while fs::exists(&txt_path)? {
                                        code = generate_secure_code(self.settings.max_str_length);
                                    }
                                    written_text.push(code.clone());
                                    let to_write = val[ri].clone();
                                    val[ri] = AlbaTypes::Text(code.clone());
                                    fs::File::create_new(&txt_path)?;
                                    let mut file = AtomicWriteFile::open(txt_path)?;
                                    match to_write{
                                        AlbaTypes::Text(a) => {
                                            file.write_all(a.as_bytes())?;
                                            file.flush()?;
                                            file.sync_all()?;
                                            file.commit()?;
                                        },
                                        _ => {}
                                    }
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
                    container.commit()?;
                }
            }
            _ =>{return Err(gerr("Failed to parse"));}
        }
        
        Ok(())
    }
    
}

pub fn connect(path : &str) -> Result<Database, Error>{
    if !match fs::exists(path){Ok(b)=>b,Err(e)=>{return Err(e)}}{
        if let Err(e ) = fs::create_dir(path){return Err(e)}
    }
    let mut db = Database{location:path.to_string(),settings:Default::default(),containers:Vec::new(),headers:Vec::new(),container:HashMap::new()};
    if let Err(e) = db.load_settings(){
        return Err(e)
    };if let Err(e) = db.load_containers(){
        return Err(e)
    };
    println!("{:?}",db.settings);
    return Ok(db)
}