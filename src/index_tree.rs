use std::{collections::{btree_set::BTreeSet, HashMap}, io::Error, sync::{Arc, RwLock}};
use std::cmp::Ordering;
use ahash::{AHashMap, AHashSet};

use crate::{gerr, index_sizes::IndexSizes, lexer_functions::{AlbaTypes, Token}};

#[derive(Default)]
#[derive(Debug)]
pub struct IndexTree{
    pub data : Arc<RwLock<HashMap<String,AHashMap<usize,BTreeSet<IndexSizes>>>>> ,
}

// impl IndexTree{
//     fn new() -> IndexTree{
//         return IndexTree{
//             data: HashMap::default(),
//         }
//     }
// }

fn text_magnitude(element : &char) -> usize {
    match element.to_digit(10){
        Some(a) => a as usize,
        None => 0
    }
}
fn index_text(input : &String) -> usize{
    let mut value : usize = 0;
    for i in input.chars(){
        value += text_magnitude(&i)
    }
    return value
}
fn index_number(input : usize) -> usize{
    return input / 1000
}
fn index_boolean(input : &bool) -> usize{
    return if *input{1}else{0}
}
fn index_bytes(input: &[u8]) -> usize {
    input.iter().map(|&byte| byte as usize).sum()
}


impl IndexTree{
    pub fn insert_gently(&mut self, column_names : Vec<String>, rows : Vec<(usize,Vec<AlbaTypes>)>)-> Result<(), Error>{
        let mut data = if let Ok(a) = self.data.write(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        for name in column_names.iter(){
            data.insert(name.clone(), AHashMap::new());
        }
        for row in rows{
            let row_id = row.0;
            for (id,cell) in row.1.iter().enumerate(){
                let column : &String = match column_names.get(id){
                    Some(a) => a,
                    None => {continue}
                };
                let group = self.get_group(cell);
                let column_map = data.get_mut(column).unwrap();
                column_map
                    .entry(group)
                    .or_insert_with(BTreeSet::new)
                    .insert(IndexSizes::proper(row_id));
            }
        }
        Ok(())
    }
    pub fn get_all_in_group(&self, column : &String,t : Token) -> Result<Option<BTreeSet<IndexSizes>>,Error>{
        let data = if let Ok(a) = self.data.read(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        if let Some(list) = data.get(column){
            match t{
                Token::String(str) => {
                    let g = self.get_group(&AlbaTypes::MediumString(str));
                    match list.get(&g){
                        Some(a) => {
                            return Ok(Some(a.clone()))
                        },
                        None => {
                            return Ok(None)
                        }
                    }
                },
                Token::Int(num) => {
                    let g = self.get_group(&&AlbaTypes::Bigint(num));
                    match list.get(&g){
                        Some(a) => {
                            return Ok(Some(a.clone()))
                        },
                        None => {
                            return Ok(None)
                        }
                    }
                },
                Token::Float(float) => {
                    let g = self.get_group(&&AlbaTypes::Float(float));
                    match list.get(&g){
                        Some(a) => {
                            return Ok(Some(a.clone()))
                        },
                        None => {
                            return Ok(None)
                        }
                    }
                },
                Token::Bool(eirdrghpirjgamjgoiuejg) => {
                    let g = self.get_group(&&AlbaTypes::Bool(eirdrghpirjgamjgoiuejg));
                    match list.get(&g){
                        Some(a) => {
                            return Ok(Some(a.clone()))
                        },
                        None => {
                            return Ok(None)
                        }
                    }
                },
                _ => {
                    return Err(gerr("Invalid token type, expected a type token, got another."))
                }
            }
        }
        Err(gerr("normalize"))
    }
    pub fn get_most_in_group_raising(&self, column : &String,t : Token) -> Result<Option<BTreeSet<BTreeSet<IndexSizes>>>,Error>{
        let data = if let Ok(a) = self.data.read(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        if let Some(list) = data.get(column){
            match t{
                Token::String(str) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::MediumString(str));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Int(num) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bigint(num));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Float(float) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Float(float));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Bool(eirdrghpirjgamjgoiuejg) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bool(eirdrghpirjgamjgoiuejg));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                _ => {
                    return Err(gerr("Invalid token type, expected a type token, got another."))
                }
            }
        }
        Err(gerr("normalize"))
    }
    pub fn get_most_in_group_lowering(&self, column : &String,t : Token) -> Result<Option<BTreeSet<BTreeSet<IndexSizes>>>,Error>{
        let data = if let Ok(a) = self.data.read(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        if let Some(list) = data.get(column){
            match t{
                Token::String(str) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::MediumString(str));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Int(num) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bigint(num));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Float(float) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Float(float));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Bool(eirdrghpirjgamjgoiuejg) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bool(eirdrghpirjgamjgoiuejg));
                    let mut g : BTreeSet<BTreeSet<IndexSizes>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                _ => {
                    return Err(gerr("Invalid token type, expected a type token, got another."))
                }
            }
        }
        Err(gerr("normalize"))
    }
    pub fn get_group(&self,cell : &AlbaTypes) -> usize{
        match cell{
            AlbaTypes::Int(n)  => index_number(*n as usize),
            AlbaTypes::Bigint(n) => index_number(*n as usize),
            AlbaTypes::Float(n) => index_number(*n as usize),
            AlbaTypes::Bool(b) => index_boolean(b),
            AlbaTypes::Char(c) => text_magnitude(c),
            AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)
            | AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => index_text(s),
            AlbaTypes::NanoBytes(blob) => index_bytes(blob),
            AlbaTypes::SmallBytes(blob) => index_bytes(blob),
            AlbaTypes::MediumBytes(blob) => index_bytes(blob),
            AlbaTypes::BigSBytes(blob) => index_bytes(blob),
            AlbaTypes::LargeBytes(blob) => index_bytes(blob),
            AlbaTypes::NONE => 0,
        }
    }
    pub fn kaboom_indexes_out(&mut self,indexes : AHashSet<usize>) -> Result<(), Error>{
        let mut data = if let Ok(a) = self.data.write(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        for j in data.iter_mut(){
            for i in j.1{
                for a in &indexes{
                    i.1.remove(&IndexSizes::proper(*a));
                }
            }
        }
        Ok(())
    }
    pub fn clear(&mut self) -> Result<(),Error>{
        let mut data = if let Ok(a) = self.data.write(){a}else{
            return Err(gerr("failed to get index tree"))
        };
        data.clear();
        Ok(())
    }
    pub fn edit(&mut self,data : (Vec<String>,Vec<(usize,Vec<AlbaTypes>)>)) -> Result<(), Error>{
        let mut ind = AHashSet::new();
        for i in data.1.iter(){
            ind.insert(i.0);
        }
        self.kaboom_indexes_out(ind)?;
        self.insert_gently(data.0, data.1);
        Ok(())
    }
}