use std::io::{Error,ErrorKind};

#[derive(Debug)]
pub enum Token{
    Keyword(String),
    String(String),
    Number(i64),
    Logic(String),
    Operator(String)
}

pub fn lexer(input : String) -> Result<Vec<Token>,Error> {
    let mut char_list = input.chars();
    let mut result : Vec<Token> = Vec::new();
    static KEYWORDS: &'static [&str] = &["PICK","SEARCH","ON","IN","IF","AND","OR","CONTAINER","INSERT","DELETE"];


    let mut dough = String::new();
    let mut unmounted : Vec<String> = Vec::new();
    
    while let Some(s) = char_list.next(){
        dough.push(s);
        if s.is_whitespace(){
            unmounted.push(dough.clone());
            dough.clear();
            continue;
        }
        if s == '\'' || s == '"'{
            let quote_type = s;
            while let Some(sv) = char_list.next(){
            dough.push(sv);
            if sv == quote_type {
                break;
            }
            }
        }
    }
    if !dough.is_empty(){
        unmounted.push(dough.clone());
        dough.clear();
    }
    println!("{:#?}",unmounted);
    for item in unmounted {
        let l = item.len();
        if l == 0{
            continue;
        }
        // operator check
        if l <= 2{
            let mut push_as_operator = ||{
                result.push(Token::Operator(item.clone()));
            };
            match item.as_str(){
                "+" => {push_as_operator();continue;},
                "-" => {push_as_operator();continue;},
                "*" => {push_as_operator();continue;},
                "/" => {push_as_operator();continue;},
                "^" => {push_as_operator();continue;},
                "#" => {push_as_operator();continue;},
                _ => {}
            };
        }
        // logical operator check
        if l <= 2{
            let mut push_as_operator = ||{
                result.push(Token::Logic(item.clone()));
            };
            match item.as_str(){
                "=" => {push_as_operator();continue;},
                "==" => {push_as_operator();continue;},
                ">=" => {push_as_operator();continue;},
                "<=" => {push_as_operator();continue;},
                "!=" => {push_as_operator();continue;},
                "<" => {push_as_operator();continue;},
                ">" => {push_as_operator();continue;},
                "<>" => {push_as_operator();continue;},
                _ => {}
            };
        }

        // number check
        if l>=1{
            let tmp = i64::from_str_radix(item.as_str(), 10);
            if let Ok(num) = tmp{
                result.push(Token::Number(num));
                continue;
            }
        }

        // String check
        if l>=2 && ((item.starts_with('\'') && item.ends_with('\''))||(item.starts_with('"') && item.ends_with('"'))) {
            let inner_string = item[1..item.len()-1].to_string();
            result.push(Token::String(inner_string));
            continue;
        }

        // Keyword check
        if l > 0{
            let item_upper = item.trim().to_uppercase();
            if KEYWORDS.contains(&item_upper.as_str()) {
                result.push(Token::Keyword(item_upper));
                continue;
            }
        }

        return Err(Error::new(ErrorKind::Unsupported, "INVALID INPUT"))
    }

    return Ok(result)
}
fn main (){
    println!("{:#?}",lexer("PICK 'my nice friend'".to_string()))
}