use std::{collections::{btree_set::BTreeSet, HashMap}, io::Error, sync::{Arc, RwLock}};
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

impl IndexTree {
    pub fn insert_gently(&mut self, column_names: Vec<String>, rows: Vec<(usize, Vec<AlbaTypes>)>) -> Result<(), Error> {
        let mut data = if let Ok(a) = self.data.write() {
            a
        } else {
            return Err(gerr("Failed to acquire write lock on index tree"));
        };
        for name in column_names.iter() {
            data.insert(name.clone(), AHashMap::new());
        }
        for row in rows {
            let row_id = row.0;
            for (id, cell) in row.1.iter().enumerate() {
                let column: &String = match column_names.get(id) {
                    Some(a) => a,
                    None => continue,
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

    pub fn get_all_in_group(&self, column: &String, t: Token) -> Result<Option<BTreeSet<IndexSizes>>, Error> {
        let data = if let Ok(a) = self.data.read() {
            a
        } else {
            return Err(gerr("Failed to acquire read lock on index tree"));
        };
        if let Some(list) = data.get(column) {
            match t {
                Token::String(str) => {
                    let g = self.get_group(&AlbaTypes::MediumString(str));
                    Ok(list.get(&g).cloned())
                }
                Token::Int(num) => {
                    let g = self.get_group(&AlbaTypes::Bigint(num));
                    Ok(list.get(&g).cloned())
                }
                Token::Float(float) => {
                    let g = self.get_group(&AlbaTypes::Float(float));
                    Ok(list.get(&g).cloned())
                }
                Token::Bool(b) => {
                    let g = self.get_group(&AlbaTypes::Bool(b));
                    Ok(list.get(&g).cloned())
                }
                _ => Err(gerr("Invalid token type: expected String, Int, Float, or Bool")),
            }
        } else {
            return Ok(Some(BTreeSet::new()))
        }
    }

    pub fn get_most_in_group_raising(&self, column: &String, t: Token) -> Result<Option<BTreeSet<BTreeSet<IndexSizes>>>, Error> {
        let data = if let Ok(a) = self.data.read() {
            a
        } else {
            return Err(gerr("Failed to acquire read lock on index tree"));
        };
        if let Some(list) = data.get(column) {
            match t {
                Token::String(str) => {
                    let group_threshold = self.get_group(&AlbaTypes::MediumString(str));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k >= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Int(num) => {
                    let group_threshold = self.get_group(&AlbaTypes::Bigint(num));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k >= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Float(float) => {
                    let group_threshold = self.get_group(&AlbaTypes::Float(float));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k >= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Bool(b) => {
                    let group_threshold = self.get_group(&AlbaTypes::Bool(b));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k >= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                _ => Err(gerr("Invalid token type: expected String, Int, Float, or Bool")),
            }
        } else {
            return Ok(Some(BTreeSet::new()))
        }
    }

    pub fn get_most_in_group_lowering(&self, column: &String, t: Token) -> Result<Option<BTreeSet<BTreeSet<IndexSizes>>>, Error> {
        let data = if let Ok(a) = self.data.read() {
            a
        } else {
            return Err(gerr("Failed to acquire read lock on index tree"));
        };
        if let Some(list) = data.get(column) {
            match t {
                Token::String(str) => {
                    let group_threshold = self.get_group(&AlbaTypes::MediumString(str));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k <= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Int(num) => {
                    let group_threshold = self.get_group(&AlbaTypes::Bigint(num));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k <= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Float(float) => {
                    let group_threshold = self.get_group(&AlbaTypes::Float(float));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k <= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                Token::Bool(b) => {
                    let group_threshold = self.get_group(&AlbaTypes::Bool(b));
                    let result: BTreeSet<BTreeSet<IndexSizes>> = list
                        .iter()
                        .filter(|(k, _)| **k <= group_threshold)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(Some(result))
                }
                _ => Err(gerr("Invalid token type: expected String, Int, Float, or Bool")),
            }
        } else {
            Err(gerr(&format!("Column not found in index tree: {}", column)))
        }
    }

    pub fn get_group(&self, cell: &AlbaTypes) -> usize {
        match cell {
            AlbaTypes::Int(n) => index_number(*n as usize),
            AlbaTypes::Bigint(n) => index_number(*n as usize),
            AlbaTypes::Float(n) => index_number(*n as usize),
            AlbaTypes::Bool(b) => index_boolean(b),
            AlbaTypes::Char(c) => text_magnitude(c),
            AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) | AlbaTypes::MediumString(s) | AlbaTypes::BigString(s)
            | AlbaTypes::LargeString(s) | AlbaTypes::Text(s) => index_text(s),
            AlbaTypes::NanoBytes(blob) => index_bytes(blob),
            AlbaTypes::SmallBytes(blob) => index_bytes(blob),
            AlbaTypes::MediumBytes(blob) => index_bytes(blob),
            AlbaTypes::BigSBytes(blob) => index_bytes(blob),
            AlbaTypes::LargeBytes(blob) => index_bytes(blob),
            AlbaTypes::NONE => 0,
        }
    }

    pub fn kaboom_indexes_out(&mut self, indexes: AHashSet<usize>) -> Result<(), Error> {
        let mut data = if let Ok(a) = self.data.write() {
            a
        } else {
            return Err(gerr("Failed to acquire write lock on index tree"));
        };
        for j in data.iter_mut() {
            for i in j.1.iter_mut() {
                for a in &indexes {
                    i.1.remove(&IndexSizes::proper(*a));
                }
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), Error> {
        let mut data = if let Ok(a) = self.data.write() {
            a
        } else {
            return Err(gerr("Failed to acquire write lock on index tree"));
        };
        data.clear();
        Ok(())
    }

    pub fn edit(&mut self, data: (Vec<String>, Vec<(usize, Vec<AlbaTypes>)>)) -> Result<(), Error> {
        let mut ind = AHashSet::new();
        for i in data.1.iter() {
            ind.insert(i.0);
        }
        self.kaboom_indexes_out(ind)?;
        self.insert_gently(data.0, data.1)?;
        Ok(())
    }
}