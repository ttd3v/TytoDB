use std::{collections::{btree_set::BTreeSet, HashMap}, io::Error};
use std::cmp::Ordering;
use ahash::AHashMap;

use crate::{gerr, lexer_functions::{AlbaTypes, Token}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSizes{
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Usize(usize),
}
impl IndexSizes{
    pub fn to_usize(a : IndexSizes) -> usize{
        match a{
            IndexSizes::U8(a) => a as usize,
            IndexSizes::U16(a) => a as usize,
            IndexSizes::U32(a) => a as usize,
            IndexSizes::U64(a) => a as usize,
            IndexSizes::Usize(a) => a as usize,
        }
    }
}
impl PartialOrd for IndexSizes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexSizes {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_val = match self {
            IndexSizes::U8(val) => *val as u64,
            IndexSizes::U16(val) => *val as u64,
            IndexSizes::U32(val) => *val as u64,
            IndexSizes::U64(val) => *val,
            IndexSizes::Usize(val) => *val as u64, 
        };
        let other_val = match other {
            IndexSizes::U8(val) => *val as u64,
            IndexSizes::U16(val) => *val as u64,
            IndexSizes::U32(val) => *val as u64,
            IndexSizes::U64(val) => *val,
            IndexSizes::Usize(val) => *val as u64,
        };
        if self_val == other_val {
            let self_rank = match self {
                IndexSizes::U8(_) => 0,
                IndexSizes::U16(_) => 1,
                IndexSizes::U32(_) => 2,
                IndexSizes::U64(_) => 3,
                IndexSizes::Usize(_) => 4,
            };
            let other_rank = match other {
                IndexSizes::U8(_) => 0,
                IndexSizes::U16(_) => 1,
                IndexSizes::U32(_) => 2,
                IndexSizes::U64(_) => 3,
                IndexSizes::Usize(_) => 4,
            };
            self_rank.cmp(&other_rank)
        } else {
            self_val.cmp(&other_val)
        }
    }
}

impl IndexSizes {
    fn proper(r: usize) -> IndexSizes {
        // Use constants for clarity and correctness
        if r <= u8::MAX as usize {
            return IndexSizes::U8(r as u8);
        }
        if r <= u16::MAX as usize {
            return IndexSizes::U16(r as u16);
        }
        if r <= u32::MAX as usize {
            return IndexSizes::U32(r as u32);
        }
        if r <= u64::MAX as usize {
            return IndexSizes::U64(r as u64);
        }
        IndexSizes::Usize(r)
    }
    // fn as_usize(&self) -> usize {
    //     match self {
    //         IndexSizes::U8(val) => *val as usize,
    //         IndexSizes::U16(val) => *val as usize,
    //         IndexSizes::U32(val) => *val as usize,
    //         IndexSizes::U64(val) => *val as usize,
    //         IndexSizes::Usize(val) => *val,
    //     }
    // }
}

#[derive(Default)]
#[derive(Debug)]
pub struct IndexTree{
    pub data : HashMap<String,AHashMap<usize,BTreeSet<IndexSizes>>>,
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
fn index_number(input : impl Into<f64>) -> usize{
    return (input.into() as f64).log10() as usize
}
fn index_boolean(input : &bool) -> usize{
    return if *input{1}else{0}
}
fn index_bytes(input: &[u8]) -> usize {
    input.iter().map(|&byte| byte as usize).sum()
}


impl IndexTree{
    pub fn insert_gently(&mut self, column_names : Vec<String>, rows : Vec<(usize,Vec<AlbaTypes>)>){
        for name in column_names.iter(){
            self.data.insert(name.clone(), AHashMap::new());
        }
        for row in rows{
            let row_id = row.0;
            for (id,cell) in row.1.iter().enumerate(){
                let column : &String = match column_names.get(id){
                    Some(a) => a,
                    None => {continue}
                };
                let group = self.get_group(cell);
                let column_map = self.data.get_mut(column).unwrap();
                column_map
                    .entry(group)
                    .or_insert_with(BTreeSet::new)
                    .insert(IndexSizes::proper(row_id));
            }
        }
    }
    pub fn get_all_in_group(&self, column : &String,t : Token) -> Result<Option<&BTreeSet<IndexSizes>>,Error>{
        if let Some(list) = self.data.get(column){
            match t{
                Token::String(str) => {
                    let g = self.get_group(&AlbaTypes::MediumString(str));
                    match list.get(&g){
                        Some(a) => {
                            return Ok(Some(a))
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
                            return Ok(Some(a))
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
                            return Ok(Some(a))
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
                            return Ok(Some(a))
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
        if let Some(list) = self.data.get(column){
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
        if let Some(list) = self.data.get(column){
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
            AlbaTypes::Int(n)  => index_number(*n as f64),
            AlbaTypes::Bigint(n) => index_number(*n as f64),
            AlbaTypes::Float(n) => index_number(*n as f64),
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
}