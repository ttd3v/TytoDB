use std::io::{Error, ErrorKind};
use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};

use crate::{database::MAX_STR_LEN, lexer};

#[derive(Debug, Clone, PartialEq)]
pub enum Token{
    Keyword(String),
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Operator(String),
    Group(Vec<Token>),
    SubCommand(Vec<Token>),
    Argument,
}
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum AlbaTypes{
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool),
    Char(char),
    NanoString(String),
    SmallString(String),
    MediumString(String),
    BigString(String),
    LargeString(String),
    NanoBytes(Vec<u8>),
    SmallBytes(Vec<u8>),
    MediumBytes(Vec<u8>),
    BigSBytes(Vec<u8>),
    LargeBytes(Vec<u8>),
    NONE
}
/*
char ~ 1
string~n ~ 10
string~s ~ 100
string-m ~ 500
string-b ~ 2,000
string-l ~ 3,000
bytes~n ~ 10
bytes~s ~ 1,000
bytes-m ~ 10,000
bytes-b ~ 100,000
bytes-l ~ 1,000,000

*/

impl AlbaTypes {
    pub fn try_from_existing(&self, i: AlbaTypes) -> Result<AlbaTypes, Error> {
        match self {
            AlbaTypes::Text(_) => {
                let text = match i {
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => s,
                    AlbaTypes::Int(n) => n.to_string(),
                    AlbaTypes::Bigint(n) => n.to_string(),
                    AlbaTypes::Float(f) => f.to_string(),
                    AlbaTypes::Bool(b) => b.to_string(),
                    AlbaTypes::Char(c) => c.to_string(),
                    AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
                    AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => {
                        general_purpose::STANDARD.encode(&b)
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Text")),
                };
                Ok(AlbaTypes::Text(text))
            }
            AlbaTypes::Int(_) => {
                let int_val = match i {
                    AlbaTypes::Int(n) => n,
                    AlbaTypes::Bigint(n) => {
                        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                            n as i32
                        } else {
                            return Err(Error::new(ErrorKind::InvalidData, "Bigint out of range for i32"));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NaN or infinite float to i32"));
                        }
                        f as i32
                    }
                    AlbaTypes::Bool(b) => if b { 1 } else { 0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<i32>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as i32"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Int")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Int")),
                };
                Ok(AlbaTypes::Int(int_val))
            }
            AlbaTypes::Bigint(_) => {
                let bigint_val = match i {
                    AlbaTypes::Bigint(n) => n,
                    AlbaTypes::Int(n) => n as i64,
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NaN or infinite float to i64"));
                        }
                        f as i64
                    }
                    AlbaTypes::Bool(b) => if b { 1 } else { 0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<i64>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as i64"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bigint")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Bigint")),
                };
                Ok(AlbaTypes::Bigint(bigint_val))
            }
            AlbaTypes::Float(_) => {
                let float_val = match i {
                    AlbaTypes::Float(f) => f,
                    AlbaTypes::Int(n) => n as f64,
                    AlbaTypes::Bigint(n) => n as f64,
                    AlbaTypes::Bool(b) => if b { 1.0 } else { 0.0 },
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        s.parse::<f64>().map_err(|_| Error::new(ErrorKind::InvalidData, "Failed to parse string as f64"))?
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Float")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Float")),
                };
                Ok(AlbaTypes::Float(float_val))
            }
            AlbaTypes::Bool(_) => {
                let bool_val = match i {
                    AlbaTypes::Bool(b) => b,
                    AlbaTypes::Int(n) => n != 0,
                    AlbaTypes::Bigint(n) => n != 0,
                    AlbaTypes::Float(f) => f != 0.0,
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        let trimmed = s.trim().to_lowercase();
                        match trimmed.as_str() {
                            "0" | "f" | "false" => false,
                            "1" | "t" | "true" => true,
                            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid boolean string")),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bool")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Bool")),
                };
                Ok(AlbaTypes::Bool(bool_val))
            }
            AlbaTypes::Char(_) => {
                let char_val = match i {
                    AlbaTypes::Char(c) => c,
                    AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
                    AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
                        if s.len() == 1 {
                            s.chars().next().unwrap()
                        } else {
                            return Err(Error::new(ErrorKind::InvalidData, "String must be a single character for Char"));
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Char")),
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to Char")),
                };
                Ok(AlbaTypes::Char(char_val))
            }
            AlbaTypes::NanoString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::NanoString(truncate_or_pad_string(s, 10)))
            }
            AlbaTypes::SmallString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::SmallString(truncate_or_pad_string(s, 100)))
            }
            AlbaTypes::MediumString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::MediumString(truncate_or_pad_string(s, 500)))
            }
            AlbaTypes::BigString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::BigString(truncate_or_pad_string(s, 2000)))
            }
            AlbaTypes::LargeString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::LargeString(truncate_or_pad_string(s, 3000)))
            }
            AlbaTypes::NanoBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::NanoBytes(truncate_or_pad_bytes(bytes, 10)))
            }
            AlbaTypes::SmallBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::SmallBytes(truncate_or_pad_bytes(bytes, 1000)))
            }
            AlbaTypes::MediumBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::MediumBytes(truncate_or_pad_bytes(bytes, 10_000)))
            }
            AlbaTypes::BigSBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::BigSBytes(truncate_or_pad_bytes(bytes, 100_000)))
            }
            AlbaTypes::LargeBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::LargeBytes(truncate_or_pad_bytes(bytes, 1_000_000)))
            }
            AlbaTypes::NONE => Ok(AlbaTypes::NONE),
        }
    }
   pub fn size(&self) -> usize{
        match self {
            AlbaTypes::Bigint(_) => size_of::<i64>(),
            AlbaTypes::Int(_) => size_of::<i32>(),
            AlbaTypes::Float(_) => size_of::<f64>(),
            AlbaTypes::Bool(_) => size_of::<bool>(),
            AlbaTypes::Text(_) => MAX_STR_LEN,
            AlbaTypes::NONE => 0,
            AlbaTypes::Char(_) => size_of::<char>(),
            AlbaTypes::NanoString(_) => 10 + size_of::<usize>(),
            AlbaTypes::SmallString(_) => 100 + size_of::<usize>(),
            AlbaTypes::MediumString(_) => 500 + size_of::<usize>(),
            AlbaTypes::BigString(_) => 2_000 + size_of::<usize>(),
            AlbaTypes::LargeString(_) => 3_000 + size_of::<usize>(),
            AlbaTypes::NanoBytes(_) => 10 + size_of::<usize>(),
            AlbaTypes::SmallBytes(_) => 1000 + size_of::<usize>(),
            AlbaTypes::MediumBytes(_) => 10_000 + size_of::<usize>(),
            AlbaTypes::BigSBytes(_) => 100_000 + size_of::<usize>(),
            AlbaTypes::LargeBytes(_) => 1_000_000 + size_of::<usize>(),
        }
    }
}

fn get_string_from_alba_type(i: AlbaTypes) -> Result<String, Error> {
    match i {
        AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
        AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => Ok(s),
        AlbaTypes::Int(n) => Ok(n.to_string()),
        AlbaTypes::Bigint(n) => Ok(n.to_string()),
        AlbaTypes::Float(f) => Ok(f.to_string()),
        AlbaTypes::Bool(b) => Ok(b.to_string()),
        AlbaTypes::Char(c) => Ok(c.to_string()),
        AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
        AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => {
            Ok(general_purpose::STANDARD.encode(&b))
        }
        AlbaTypes::NONE => Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to string")),
    }
}

fn truncate_or_pad_string(s: String, max_len: usize) -> String {
    if s.len() > max_len {
        s[..max_len].to_string()
    } else {
        format!("{: <width$}", s, width = max_len)
    }
}

fn get_bytes_from_alba_type(i: AlbaTypes) -> Result<Vec<u8>, Error> {
    match i {
        AlbaTypes::NanoBytes(b) | AlbaTypes::SmallBytes(b) | AlbaTypes::MediumBytes(b) |
        AlbaTypes::BigSBytes(b) | AlbaTypes::LargeBytes(b) => Ok(b),
        AlbaTypes::Text(s) | AlbaTypes::NanoString(s) | AlbaTypes::SmallString(s) |
        AlbaTypes::MediumString(s) | AlbaTypes::BigString(s) | AlbaTypes::LargeString(s) => {
            general_purpose::STANDARD
                .decode(s.as_bytes())
                .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid base64 string"))
        }
        AlbaTypes::NONE => Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to bytes")),
        _ => Err(Error::new(ErrorKind::InvalidData, "Unsupported conversion to bytes")),
    }
}

fn truncate_or_pad_bytes(b: Vec<u8>, max_len: usize) -> Vec<u8> {
    let mut bytes = b;
    if bytes.len() > max_len {
        bytes.truncate(max_len);
    } else {
        bytes.resize(max_len, 0);
    }
    bytes
}

impl TryFrom<Token> for AlbaTypes {
    type Error = &'static str;

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        match token {
            Token::String(s) =>
                Ok(AlbaTypes::Text(s)), // moved, no clone

            Token::Int(i) if (i32::MIN as i64) <= i && i <= (i32::MAX as i64) =>
                Ok(AlbaTypes::Int(i as i32)),

            Token::Int(i) =>
                Ok(AlbaTypes::Bigint(i)),

            Token::Float(f) =>
                Ok(AlbaTypes::Float(f)),

            Token::Bool(b) =>
                Ok(AlbaTypes::Bool(b)),
            Token::Keyword(s) => match s.to_uppercase().as_str().trim() {
                "INT" => Ok(AlbaTypes::Int(0)),        // default dummy values
                "BIGINT" => Ok(AlbaTypes::Bigint(0)),
                "FLOAT" => Ok(AlbaTypes::Float(0.0)),
                "BOOL" => Ok(AlbaTypes::Bool(false)),
                "TEXT" => Ok(AlbaTypes::Text(String::new())),
                "NANO-STRING" => Ok(AlbaTypes::NanoString(String::new())),
                "SMALL-STRING" => Ok(AlbaTypes::SmallString(String::new())),
                "MEDIUM-STRING" => Ok(AlbaTypes::MediumString(String::new())),
                "BIG-STRING" => Ok(AlbaTypes::BigString(String::new())),
                "LARGE-STRING" => Ok(AlbaTypes::LargeString(String::new())),
                "NANO-BYTES" => Ok(AlbaTypes::NanoBytes(Vec::new())),
                "SMALL-BYTES" => Ok(AlbaTypes::SmallBytes(Vec::new())),
                "MEDIUM-BYTES" => Ok(AlbaTypes::MediumBytes(Vec::new())),
                "BIG-BYTES" => Ok(AlbaTypes::BigSBytes(Vec::new())),
                "LARGE-BYTES" => Ok(AlbaTypes::LargeBytes(Vec::new())),
                _ => return Err(format!("Unknown type keyword: {}", s).leak()),
            },
            _ => {
                let va = format!("Cannot convert token to AlbaTypes: unsupported token type {:#?}. Expected one of: String, Int, Float, Bool, or Keyword (for type definitions).", token);
                return Err(va.leak());
            }
        }
    }
}
const KEYWORDS: &[&str] = &[
    "CREATE",
    "COMMIT",
    "ROLLBACK",
    "DELETE",
    "EDIT",
    "SEARCH",
    "WHERE",
    "ROW",
    "CONTAINER",
    "ON",
    "USING",
    "INT",
    "BIGINT",
    "TEXT",
    "BOOL",
    "FLOAT",
    "AND",
    "OR",
    "NANO-STRING",
    "SMALL-STRING",
    "MEDIUM-STRING",
    "BIG-STRING",
    "LARGE-STRING",
    "NANO-BYTES",
    "SMALL-BYTES",
    "MEDIUM-BYTES",
    "BIG-BYTES",
    "LARGE-BYTES",

    // weird looking because connection handlers that should use this, not users
    "QYCNPVS", // query control previous 
    "QYCNNXT", // query control next
    "QYCNEXT" // query control exit
];

pub fn lexer_keyword_match(result: &mut Vec<Token>, dough: &mut String) -> bool {
    let keyword = dough.to_uppercase(); 

    if KEYWORDS.contains(&keyword.as_str()) {
        result.push(Token::Keyword(keyword.to_uppercase())); 
        dough.clear(); 
        return true
    }
    false
}

pub fn lexer_string_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut T) -> bool{
    if dough.starts_with(' '){
        dough.drain(..1);
    }
    if dough.starts_with('\'') || dough.starts_with('"'){
        if let Some(quote_type) = dough.chars().nth(0){
            let mut escaped = false;
            while let Some(s) = itr.next() {
                if s == '\\'{
                    escaped = true
                }
                dough.push(s);
                
                if s == quote_type && !escaped {
                    break;
                }
            }
            if dough.starts_with(quote_type) && dough.ends_with(quote_type){
                result.push(Token::String(dough[1..dough.len()-1].to_string()));
                dough.clear();
                return true
            }
        }
    }
    false
}    

fn split_group_args(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::with_capacity(input.len());
    let (mut in_string, mut string_sort, mut parens, mut escape) = (false, '\0', 0, false);

    for c in input.chars() {
        if escape {
            current.push(c);
            escape = false;
            continue;
        }
        match c {
            '\\' => { escape = true; current.push(c); }
            '\'' | '"' => {
                if !in_string { in_string = true; string_sort = c; }
                else if c == string_sort { in_string = false; }
                current.push(c);
            }
            '(' if !in_string => { parens += 1; current.push(c); }
            ')' if !in_string => { if parens > 0 { parens -= 1; } current.push(c); }
            ',' if !in_string && parens == 0 => {
                let t = current.trim();
                if !t.is_empty() { result.push(t.to_string()); }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    let t = current.trim();
    if !t.is_empty() { result.push(t.to_string()); }

    result
}


pub fn lexer_group_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut T,
) -> bool {
    if dough.starts_with('[') {
        let mut in_string : bool = false;
        let mut string_sort : char = '\\';
        let mut i = 1;
        while let Some(c) = itr.next() {
            dough.push(c);
            if (c == '\'' || c == '"') && !in_string{
                string_sort = c;
                in_string = true;
                continue;
            }
            if in_string && c == string_sort{
                in_string = false;
                continue;
            }
            if c == '[' && !in_string{
                i+=1;
            }
            if c == ']' && !in_string {
                i -= 1;
                if i == 0{
                    break;
                }
            }
        }
        println!("{}",dough);

        if dough.ends_with(']') {
            let inner = &dough[1..dough.len() - 1];
            let mut abstract_tokens = Vec::with_capacity(16);

            for part in split_group_args(&inner) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                match lexer(part.to_string()) {
                    Ok(mut toks) if !toks.is_empty() => {
                        abstract_tokens.push(toks.remove(0));
                    }
                    _ => {
                        continue;
                    }
                }
            }

            dough.clear();
            result.push(Token::Group(abstract_tokens));
            return true;
        }
    }

    false
}
pub fn lexer_subcommand_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut T,
) -> Result<bool, Error> {
    if dough.starts_with('(') {
        let mut in_string : bool = false;
        let mut string_sort : char = '\\';
        while let Some(c) = itr.next() {
            dough.push(c);
            if (c == '\'' || c == '"') && !in_string{
                string_sort = c;
                in_string = true;
                continue;
            }
            if in_string && c == string_sort{
                in_string = false;
                continue;
            }
            if c == ')' && !in_string {
                break;
            }
        }
        println!("{}",dough);

        if dough.ends_with(')') {
            let inner = &dough.clone()[1..dough.len() - 1];
            dough.clear();
            result.push(Token::SubCommand(lexer(inner.to_string())?));
            return Ok(true);
        }
    }

    Ok(false)
}

const RADIX : u32= 10;
pub fn lexer_number_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
    if let Some(d) = dough.chars().nth(0){
        let mut had_dot = false;
        if d.is_digit(RADIX){
            while let Some(n) = itr.next(){
                if n.is_digit(RADIX){
                    dough.push(n.clone());
                }else{
                    if n == '.' && !had_dot{
                        dough.push(n.clone());
                        had_dot = true
                    }else{
                        break;
                    }
                }
            }
            if had_dot{
                if let Ok(float) = dough.parse::<f64>(){
                    result.push(Token::Float(float));
                    dough.clear();
                    return true
                }
            }else{
                if let Ok(int) = dough.parse::<i64>(){
                    result.push(Token::Int(int));
                    dough.clear();
                    return true
                }
            }
        }
    }
    false
}  
pub fn lexer_ignore_comments_match<T:Iterator<Item = char>>(dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
    if dough.starts_with('/'){
        while let Some(c) = itr.next(){
            dough.push(c);
            let n = dough.len();
            if &dough[n-2..n] == "*/"{
                break;
            }
        }
    }
    false
}
pub fn lexer_operator_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut std::iter::Peekable<T>
) -> bool {
    if dough.is_empty() {
        return false;
    }
    let first = dough.chars().next().unwrap();
    let op_abstract_token = match first {
        '>' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    ">="
                } else {
                    ">"
                }
            } else {
                ">"
            }
        },
        '<' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "<="
                } else {
                    "<"
                }
            } else {
                "<"
            }
        },
        '&' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '&' {
                    itr.next();
                    if let Some(&third_char) = itr.peek() {
                        if third_char == '&' {
                            itr.next();
                            if let Some(&fourth_char) = itr.peek() {
                                if fourth_char == '>' {
                                    itr.next();
                                    "&&&>"
                                } else {
                                    "&&&"
                                }
                            } else {
                                "&&&"
                            }
                        } else if third_char == '>' {
                            itr.next();
                            "&&>"
                        } else {
                            "&&"
                        }
                    } else {
                        "&&"
                    }
                } else if next_char == '>' {
                    itr.next();
                    "&>"
                } else {
                    "&"
                }
            } else {
                "&"
            }
        },
        '=' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "=="
                } else {
                    "=" 
                }
            } else {
                "="
            }
        },
        '+' => "+",
        '*' => "*",
        '-' => "-",
        '/' => "/",
        '%' => "%",
        '!' => {
            if let Some(&next_char) = itr.peek() {
                if next_char == '=' {
                    itr.next(); 
                    "!="
                } else {
                    "!" 
                }
            } else {
                "!"
            }
        },
        _ => return false, 
    };

    result.push(Token::Operator(op_abstract_token.to_string()));
    dough.clear();
    true
}

pub fn lexer_boolean_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    _itr: &mut std::iter::Peekable<T>
) -> bool{
    let trimmed = dough.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        result.push(Token::Bool(true));
        dough.clear();
        return true
    } else if trimmed.eq_ignore_ascii_case("false") {
        result.push(Token::Bool(false));
        dough.clear();
        return true
    }
    false
}

