use std::{fs, io::Error};

use crate::{parse, AST};


pub struct Database{

}
impl Database {
    pub fn execute(self,input : &str) -> Result<AST, Error>{
        parse(input.to_owned())
    }
}

pub fn connect(path : &str) -> Result<Database, Error>{
    if !match fs::exists(path){Ok(b)=>b,Err(e)=>{return Err(e)}}{
        if let Err(e ) = fs::create_dir(path){return Err(e)}
    }
    Ok(Database{

    })
}