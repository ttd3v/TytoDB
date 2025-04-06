use std::{fmt::format, fs, io::{Error, Read, Write}};


use concat_arrays::concat_arrays;
use serde::{Serialize,Deserialize};
use serde_yaml;

use crate::{gerr, lexer_functions::AlbaTypes, parse, AST};


pub struct Database{
    location : String,
    settings : Settings
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
    memory_limit : u64
}


fn from_usize_to_u8(i:usize) -> u8{
    return i as u8;
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
            
# Memory limit: defines how much memory the database can use during operations. Setting a higher value might improve performance, but exceeding hardware limits could have the opposite effect.
memory_limit: {}
            "#, 50, 1, 128, 104_857_600);
            match file.write_all(content.as_bytes()) {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
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
                
# Memory limit: defines how much memory the database can use during operations. Setting a higher value might improve performance, but exceeding hardware limits could have the opposite effect.
memory_limit: {}
                "#,settings.max_columns,settings.min_columns,settings.max_str_length,settings.memory_limit).as_bytes()){
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
    pub fn execute(self,input : &str) -> Result<(), Error>{
        let ast = match parse(input.to_owned()){Ok(a)=>a,Err(e)=>return Err(e)};
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
                        AlbaTypes::Int(_) => from_usize_to_u8(std::mem::size_of::<i32>()),
                        AlbaTypes::Bigint(_) => from_usize_to_u8(std::mem::size_of::<i64>()),
                        AlbaTypes::Float(_) => from_usize_to_u8(std::mem::size_of::<f64>()),
                        AlbaTypes::Bool(_) => from_usize_to_u8(std::mem::size_of::<bool>()),
                        AlbaTypes::Text(_) => from_usize_to_u8(std::mem::size_of::<i64>()),
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

                
            }else{
                return Err(gerr("A container with the specified name already exists"))
            }
            },
            _ =>{return Err(gerr("Failed to parse"));}
        }
        
        Ok(())
    }
    
}

pub fn connect(path : &str) -> Result<Database, Error>{
    if !match fs::exists(path){Ok(b)=>b,Err(e)=>{return Err(e)}}{
        if let Err(e ) = fs::create_dir(path){return Err(e)}
    }
    let mut db = Database{location:path.to_string(),settings:Default::default()};
    if let Err(e) = db.load_settings(){
        return Err(e)
    };
    println!("{:#?}",db.settings);
    return Ok(db)
}