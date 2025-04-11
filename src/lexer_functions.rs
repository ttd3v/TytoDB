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
}
#[derive(Debug, Clone, PartialEq)]
pub enum AlbaTypes{
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool),
    NONE
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
                _ => return Err(format!("Unknown type keyword: {}", s).leak()),
            },
            _ =>{
                let va = format!("unsupported token type: {:#?}",token);
                return Err(va.leak());
            }
        }
    }
}
const KEYWORDS: &[&str] = &["CREATE","DELETE","EDIT","SEARCH","WHERE","ROW","CONTAINER","ON","USING","INT","BIGINT","TEXT","BOOL","FLOAT","AND","OR"];

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
            while let Some(s) = itr.next() {
                dough.push(s);
                if s == quote_type {
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
pub fn lexer_group_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    itr: &mut T,
) -> bool {
    if dough.starts_with('[') {
        // consume until ']' inclusive
        while let Some(c) = itr.next() {
            dough.push(c);
            if c == ']' {
                break;
            }
        }

        if dough.ends_with(']') {
            // strip the brackets
            let inner = &dough[1..dough.len() - 1];
            let mut abstract_tokens = Vec::with_capacity(16);

            for part in inner.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                match lexer(part.to_string()) {
                    Ok(mut toks) if !toks.is_empty() => {
                        abstract_tokens.push(toks.remove(0));
                    }
                    _ => {
                        // you could log or return Err here
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

