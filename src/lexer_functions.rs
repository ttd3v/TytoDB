use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};

use crate::lexer;

#[derive(Debug, Clone, PartialEq)]
pub enum Token{
    Keyword(String),
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Operator(String),
    Group(Vec<Token>),
    SubCommand(Vec<Token>)
}
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum AlbaTypes{
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool),
    NONE
}

impl AlbaTypes {
    pub fn try_from_existing(&self, i: AlbaTypes) -> Result<AlbaTypes, Error> {
        match self {
            AlbaTypes::Text(_) => {
                let text = match i {
                    AlbaTypes::Text(s) => s,
                    AlbaTypes::Int(n) => n.to_string(),
                    AlbaTypes::Bigint(n) => n.to_string(),
                    AlbaTypes::Float(f) => f.to_string(),
                    AlbaTypes::Bool(b) => b.to_string(),
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
                            return Err(Error::new(ErrorKind::InvalidData, "Bigint value out of range for i32"));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NaN or infinite float to i32"));
                        }
                        f as i32
                    }
                    AlbaTypes::Bool(b) => if b { 1 } else { 0 },
                    AlbaTypes::Text(s) => {
                        match s.parse::<i32>() {
                            Ok(n) => n,
                            Err(_) => return Err(Error::new(ErrorKind::InvalidData, format!("Failed to parse '{}' as i32", s))),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Int")),
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
                    AlbaTypes::Text(s) => {
                        match s.parse::<i64>() {
                            Ok(n) => n,
                            Err(_) => return Err(Error::new(ErrorKind::InvalidData, format!("Failed to parse '{}' as i64", s))),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bigint")),
                };
                Ok(AlbaTypes::Bigint(bigint_val))
            }
            AlbaTypes::Float(_) => {
                let float_val = match i {
                    AlbaTypes::Float(f) => f,
                    AlbaTypes::Int(n) => n as f64,
                    AlbaTypes::Bigint(n) => n as f64,
                    AlbaTypes::Bool(b) => if b { 1.0 } else { 0.0 },
                    AlbaTypes::Text(s) => {
                        match s.parse::<f64>() {
                            Ok(f) => f,
                            Err(_) => return Err(Error::new(ErrorKind::InvalidData, format!("Failed to parse '{}' as f64", s))),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Float")),
                };
                Ok(AlbaTypes::Float(float_val))
            }
            AlbaTypes::Bool(_) => {
                let bool_val = match i {
                    AlbaTypes::Bool(b) => b,
                    AlbaTypes::Int(n) => n != 0,
                    AlbaTypes::Bigint(n) => n != 0,
                    AlbaTypes::Float(f) => f != 0.0,
                    AlbaTypes::Text(s) => {
                        let trimmed = s.trim().to_lowercase();
                        match trimmed.as_str() {
                            "0" => false,
                            "1" => true,
                            "f" => false,
                            "t" => true,
                            "false" => false,
                            "true" => true,
                            "" => return Err(Error::new(ErrorKind::InvalidData, "Empty string cannot be converted to boolean")),
                            _ => return Err(Error::new(ErrorKind::InvalidData, format!("Invalid boolean string: '{}'", s))),
                        }
                    }
                    AlbaTypes::NONE => return Err(Error::new(ErrorKind::InvalidData, "Cannot convert NONE to Bool")),
                };
                Ok(AlbaTypes::Bool(bool_val))
            }
            AlbaTypes::NONE => {
                Ok(AlbaTypes::NONE)
            }
        }
    }
}

/*
Types and it's size
+-----------+---------------+
| TYPE NAME |   BYTE SIZE   |
+-----------+---------------+
| CHAR      |   1           |
| STRING-SS |   5           |
| STRING-SM |   20          |
| STRING-SB |   50          |
| STRING-SL |   75          |
| STRING-MS |   100         |
| STRING-MM |   135         |
| STRING-MB |   150         |
| STRING-ML |   175         |
| STRING-BS |   200         |
| STRING-BM |   250         |
| STRING-BB |   350         |
| STRING-BL |   400         |
| STRING-LS |   500         |
| STRING-LM |   600         |
| STRING-LB |   700         |
| STRING-LL |   10000       |
| BLOB-SS   |   250         |
| BLOB-SM   |   500         |
| BLOB-SB   |   750         |
| BLOB-SL   |   1000        |
| BLOB-MS   |   25000       |
| BLOB-MM   |   50000       |
| BLOB-MB   |   75000       |
| BLOB-ML   |   100000      |
| BLOB-BS   |   250000      |
| BLOB-BM   |   500000      |
| BLOB-BB   |   750000      |
| BLOB-BL   |   1000000     |
| BLOB-LS   |   250000000   |
| BLOB-LM   |   500000000   |
| BLOB-LB   |   750000000   |
| BLOB-LL   |   1000000000  |
+-----------+---------------+
*/



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
                _ => return Err(format!("Unknown type keyword: {}", s).leak()),
            },
            _ => {
                let va = format!("Cannot convert token to AlbaTypes: unsupported token type {:#?}. Expected one of: String, Int, Float, Bool, or Keyword (for type definitions).", token);
                return Err(va.leak());
            }
        }
    }
}
const KEYWORDS: &[&str] = &["CREATE","COMMIT","ROLLBACK","DELETE","EDIT","SEARCH","WHERE","ROW","CONTAINER","ON","USING","INT","BIGINT","TEXT","BOOL","FLOAT","AND","OR"];

pub fn lexer_keyword_match(result: &mut Vec<Token>, dough: &mut String) -> bool {
    let keyword = dough.to_uppercase(); // Remove spaces and normalize

    if KEYWORDS.contains(&keyword.as_str()) {
        result.push(Token::Keyword(keyword.to_uppercase())); // Store the normalized keyword
        dough.clear(); // Clear after matching
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
            // strip the brackets
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

