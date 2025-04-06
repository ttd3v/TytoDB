use std::{fmt::format, fs, io::{Error, Write}};

use concat_arrays::concat_arrays;

use crate::{gerr, lexer_functions::AlbaTypes, parse, AST};

// SETTINGS
const MAX_COLUMNS : usize = 50;
const MIN_COLUMN : usize = 1;
const MAX_STR_LEN: usize = 128;

pub struct Database{
    location : String
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

fn from_usize_to_u8(i:usize) -> u8{
    return i as u8;
}
impl Database {
    pub fn execute(self,input : &str) -> Result<(), Error>{
        let ast = match parse(input.to_owned()){Ok(a)=>a,Err(e)=>return Err(e)};
        match ast{
            AST::CreateContainer(structure) => {
              if !match fs::exists(format!("{}/{}",self.location,structure.name)) {Ok(a)=>a,Err(bruh)=>{return Err(bruh)}}{
                let cn_len = structure.col_nam.len();
                let cv_len = structure.col_val.len();

                if cn_len != cv_len{
                    return Err(gerr("Mismatch between number of column names and column values"))
                }
                if cn_len < MIN_COLUMN || cv_len < MIN_COLUMN{
                    if cn_len < MIN_COLUMN {
                        return Err(gerr(&format!("Column count must be {} or more", MIN_COLUMN)))
                    }                    
                }
                if cn_len > MAX_COLUMNS || cv_len > MAX_COLUMNS{
                    return Err(gerr(format!("Exceeded maximum column count of {}",MAX_COLUMNS).as_str()))
                }

                let mut column_name_headers: [String; MAX_COLUMNS] = std::array::from_fn(|_| "".to_string());
                let mut column_val_headers: [AlbaTypes; MAX_COLUMNS] = std::array::from_fn(|_| AlbaTypes::NONE);


                for (num, str_val) in structure.col_nam.iter().enumerate().take(MAX_COLUMNS) {
                    column_name_headers[num] = str_val.to_string();
                }
                for (num, v) in structure.col_val.iter().enumerate().take(MAX_COLUMNS) {
                    column_val_headers[num] = v.clone();
                }
                
                let mut column_name_bytes: [[u8; MAX_STR_LEN]; MAX_COLUMNS] = [[0u8; MAX_STR_LEN]; MAX_COLUMNS];
                let mut column_val_bytes = [0u8; MAX_COLUMNS];

                for (i,item) in column_name_headers.iter().enumerate(){
                    let bytes = item.as_bytes();
                    if bytes.len() > MAX_STR_LEN {
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
    Ok(Database{location:path.to_string()})
}