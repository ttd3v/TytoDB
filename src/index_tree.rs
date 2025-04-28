use std::{collections::{btree_set::BTreeSet, HashMap}, error, io::Error, num};

use crate::{gerr, lexer_functions::{AlbaTypes, Token}};

#[derive(Default)]
pub struct IndexTree{
    pub data : HashMap<String,HashMap<usize,BTreeSet<usize>>>,
}

trait New{
    fn new() -> IndexTree;
}
impl New for IndexTree{
    fn new() -> IndexTree{
        return IndexTree{
            data: HashMap::default(),
        }
    }
}

fn text_magnitude(element : &char) -> usize {
    match element.to_ascii_lowercase(){
        'a' | 'b' | 'c' => 0,
        'd' | 'e' | 'f' => 1,
        'g' | 'h' | 'i' => 2,
        'j' | 'k' | 'l' => 3,
        'm' | 'n' | 'o' => 4,
        'p' | 'q' | 'r' => 5,
        's' | 't' | 'u' => 6,
        'v' | 'w' | 'x' => 7,
        'y' | 'z' => 8,
        _ => 9,
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
    return (input.into() / 128.0) as usize
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
            self.data.insert(name.clone(), HashMap::new());
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
                    .insert(row_id);
            }
        }
    }
    pub fn get_all_in_group(&self, column : &String,t : Token) -> Result<Option<&BTreeSet<usize>>,Error>{
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
    pub fn get_most_in_group_raising(&self, column : &String,t : Token) -> Result<Option<BTreeSet<BTreeSet<usize>>>,Error>{
        if let Some(list) = self.data.get(column){
            match t{
                Token::String(str) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::MediumString(str));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Int(num) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bigint(num));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Float(float) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Float(float));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 >= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Bool(eirdrghpirjgamjgoiuejg) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bool(eirdrghpirjgamjgoiuejg));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
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
    pub fn get_most_in_group_lowering(&self, column : &String,t : Token) -> Result<Option<BTreeSet<BTreeSet<usize>>>,Error>{
        if let Some(list) = self.data.get(column){
            match t{
                Token::String(str) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::MediumString(str));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Int(num) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bigint(num));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Float(float) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Float(float));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
                    for i in list.iter(){
                        if *i.0 <= bruhafjiahfasojf{
                            g.insert(i.1.clone());
                        }
                    }
                    return Ok(Some(g))
                },
                Token::Bool(eirdrghpirjgamjgoiuejg) => {
                    let bruhafjiahfasojf: usize = self.get_group(&AlbaTypes::Bool(eirdrghpirjgamjgoiuejg));
                    let mut g : BTreeSet<BTreeSet<usize>> = BTreeSet::new();
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