mod create_inputs_for_testing;
use std::io::{Error,ErrorKind};
use std::time::Instant;

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
enum AlbaTypes{
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool)
}

impl TryFrom<&Token> for AlbaTypes {
    type Error = String;
    fn try_from(token: &Token) -> Result<Self, Self::Error> {
        match token {
            Token::String(s) => Ok(AlbaTypes::Text(s.clone())),
            Token::Int(i) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Ok(AlbaTypes::Int(*i as i32))
                } else {
                    Ok(AlbaTypes::Bigint(*i))
                }
            },
            Token::Float(f) => Ok(AlbaTypes::Float(*f)),
            Token::Bool(b) => Ok(AlbaTypes::Bool(*b)),
            _ => Err(format!("Token {:?} cannot be converted to AlbaTypes", token)),
        }
    }
}


const KEYWORDS: &[&str] = &["CREATE","DELETE","EDIT","SEARCH","WHERE","ROW","CONTAINER","ON","USING","INT","BIGINT","STRING","BOOLEAN","FLOAT"];

fn lexer_keyword_match(result: &mut Vec<Token>, dough: &mut String) -> bool {
    let keyword = dough.to_uppercase(); // Remove spaces and normalize

    if KEYWORDS.contains(&keyword.as_str()) {
        result.push(Token::Keyword(keyword.to_uppercase())); // Store the normalized keyword
        dough.clear(); // Clear after matching
        return true
    }
    false
}

fn lexer_string_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut T) -> bool{
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
fn lexer_group_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut T) -> bool{
    if dough.starts_with('['){
        
            while let Some(s) = itr.next() {
                dough.push(s);
                if s == ']' {
                    break;
                }
            }
            if dough.starts_with('[') && dough.ends_with(']'){
                let st = &dough[1..dough.len()-1];
                let mut abstract_tokens : Vec<Token> = Vec::with_capacity(20);
                for i in st.split(','){
                    if let Ok(v) = lexer(i.to_string()){
                        if let Some(f) = v.first(){
                            abstract_tokens.push(f.clone())
                        }
                    }
                }
                dough.clear();
                result.push(Token::Group(abstract_tokens));
                return true
            }
        
    }
    false
}  
const RADIX : u32= 10;
fn lexer_number_match<T:Iterator<Item = char>>(result : &mut Vec<Token>,dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
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
fn lexer_ignore_comments_match<T:Iterator<Item = char>>(dough : &mut String, itr : &mut std::iter::Peekable<T>) -> bool{
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
fn lexer_operator_match<T: Iterator<Item = char>>(
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

fn lexer_boolean_match<T: Iterator<Item = char>>(
    result: &mut Vec<Token>,
    dough: &mut String,
    _itr: &mut std::iter::Peekable<T>
) -> bool{
    // Trim any whitespace from the dough
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




fn lexer(input : String)->Result<Vec<Token>,Error>{
    if input.len() == 0{
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()))
    }
    // print!("{}\n",input);
    let mut characters = input.trim().chars().peekable();
    let mut result:Vec<Token> = Vec::with_capacity(20);
    let mut dough : String = String::new();

    while let Some(c) = characters.next(){
        dough.push(c);
        lexer_ignore_comments_match(&mut dough, &mut characters) ;
        lexer_keyword_match(&mut result, &mut dough);
        lexer_boolean_match(&mut result, &mut dough, &mut characters) ;
        lexer_operator_match(&mut result, &mut dough, &mut characters) ;
        lexer_number_match(&mut result, &mut dough, &mut characters) ;
        lexer_string_match(&mut result, &mut dough, &mut characters) ;
        lexer_group_match(&mut result, &mut dough, &mut characters);
    }
    
    if result.len() > 0{
        println!("{:?}",result);
        return Ok(result)
    }
    drop(result);

    Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()))
}

/*



- CREATE <Instance> ...
| CREATE CONTAINER <name> [col_nam][col_typ] 
| CREATE ROW [col_nam][col_val] ON <container:name>

- EDIT <Instance> ...
| EDIT ROW [col_name][col_val] ON <container:name> WHERE <conditions>

- DELETE <instance> ...
| DELETE ROW ON <container> WHERE <conditions>
| DELETE ROW ON <container>
| DELETE CONTAINER <container>

- SEARCH <col_nam> ON <container> ... 
| SEARCH <col_nam> ON <container>
| SEARCH <col_nam> ON <container> WHERE <conditions>

*/
#[derive(Debug, Clone, PartialEq)]
enum AST{
    CreateContainer(AST_CREATE_CONTAINER),
    CreateRow(AST_CREATE_ROW),
    EditRow(AST_EDIT_ROW),
    DeleteRow(AST_DELETE_ROW),
    DeleteContainer(AST_DELETE_CONTAINER),
}



#[derive(Debug, Clone, PartialEq)]
struct AST_CREATE_CONTAINER{
    name : String,
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
}
#[derive(Debug, Clone, PartialEq)]
struct AST_CREATE_ROW{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String
}
#[derive(Debug, Clone, PartialEq)]
struct AST_EDIT_ROW{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String,
    conditions : Vec<Token>
}
#[derive(Debug, Clone, PartialEq)]
struct AST_DELETE_ROW{
    container : String,
    conditions : Option<Vec<Token>>
}
#[derive(Debug, Clone, PartialEq)]
struct AST_DELETE_CONTAINER{
    container : String,
}

fn gerr(msg : &str) -> Error{
    return Error::new(ErrorKind::Other, msg.to_string())
}


fn parser_debugger_extract_string(output : &mut String,list : &Vec<Token>,index : usize) -> Option<Error>{
    if let Some(cn) = list.get(index){
        match cn{
            Token::String(s) => {
                output.push_str(s.as_str());
                return None
            },
            _ => {return Some(gerr("Invalid type, must be a string"));}
        }
    }else{
        return Some(gerr("Missing a string"));
    }
}

fn parser_debugger_extract_group_elstr(output : &mut Vec<String>,list : &Vec<Token>,index : usize) -> Option<Error>{
    if let Some(group) = list.get(index) {
        match group{
            Token::Group(ggggg) => {
                for i in ggggg{
                    match i{
                        Token::String(s) => {output.push(s.clone());},
                        _=>{return Some(gerr("Invalid type, must be a group with only strings inside"));}
                    }
                }
            }
            _ => {return Some(gerr("Invalid type, must be a group with only strings inside"));} 
        }
    }
    None
}
fn parser_debugger_extract_group_albatype(output: &mut Vec<AlbaTypes>, list: &[Token], index: usize) -> Option<Error> {
    if let Some(token) = list.get(index) {
        match token {
            Token::Group(g) => {
                for item in g {
                    match AlbaTypes::try_from(item) {
                        Ok(value) => output.push(value),
                        Err(_) => return Some(gerr("Invalid type")),
                    }
                }
                None
            }
            _ => Some(gerr("Missing column types")),
        }
    } else {
        Some(gerr("Missing token for column types"))
    }
}

// MAIN FUNCTIONS
fn debug_create_command(tokens: &Vec<Token>) -> Result<AST,Error>{
    if let Some(instance) = tokens.get(1){
        match instance{
            Token::Keyword(ist) => {
                match ist.as_str(){
                    "CONTAINER" => {
                        let mut cname : String = String::new();
                        let mut col_name : Vec<String> = Vec::with_capacity(5);
                        let mut col_types : Vec<AlbaTypes> = Vec::with_capacity(5);
                        if let Some(err) = parser_debugger_extract_string(&mut cname,tokens,2){
                            return Err(err)
                        }
                        if let Some(cva) = parser_debugger_extract_group_elstr(&mut col_name, tokens, 3){
                            return Err(cva)
                        }
                        if let Some(bruh) = parser_debugger_extract_group_albatype(&mut col_types, tokens, 4){
                            return Err(bruh)
                        }
                        if col_name.len() != col_types.len(){
                            return Err(gerr("All column names and column types are not matching"))
                        }
                        
                        return Ok(AST::CreateContainer(AST_CREATE_CONTAINER { name: cname, col_nam: col_name, col_val: col_types }))
                    }
                    "ROW" => {
                        let mut col_names : Vec<String> = Vec::with_capacity(5);
                        let mut col_values : Vec<AlbaTypes> = Vec::with_capacity(5);
                        let mut container = String::new();

                        if let Some(cva) = parser_debugger_extract_group_elstr(&mut col_names, tokens, 2){
                            return Err(cva)
                        }
                        if let Some(bruh) = parser_debugger_extract_group_albatype(&mut col_values, tokens, 3){
                            return Err(bruh)
                        }
                        if let Some(cn) = tokens.get(4){
                            match cn{
                                Token::String(s) => {
                                    container = s.clone()
                                },
                                _ => {return Err(gerr("Invalid type, the container name must be a string"));}
                            }
                        }
                        return Ok(AST::CreateRow(AST_CREATE_ROW { col_nam: col_names, col_val: col_values, container: container }))
                        
                    },
                    _ => {return Err(gerr("Invalid instance type"))}
                }
            },
            _ => {
                return Err(gerr("Invalid command"));
            }
            
        }
    }
    return Err(gerr("Missing the instance to be created"));
}

fn debug_edit_command(tokens : &Vec<Token>) -> Result<AST,Error> {
    if let Some(instance) = tokens.get(1){
        match instance{
            Token::Keyword(st) => {
                match st.as_str(){
                    "ROW" => {
                        let mut ed_col_name : Vec<String> = Vec::with_capacity(20);
                        let mut ed_col_type : Vec<AlbaTypes> = Vec::with_capacity(20);
                        let mut ed_container : String = String::new();
                        let mut cond : Option<Vec<Token>> = None;

                        if let Some(errrrrr) = parser_debugger_extract_group_elstr(&mut ed_col_name, &tokens, 2){
                            return Err(errrrrr)
                        }
                        if let Some(errrrrr) = parser_debugger_extract_group_albatype(&mut ed_col_type, &tokens, 3){
                            return Err(errrrrr)
                        }
                        if let Some(t) = tokens.get(4){
                            if let Token::Keyword(kw) = t{
                                if kw != "ON"{
                                    return Err(gerr("Missing container"))
                                }
                            }
                        }
                        if let Some(errrrrr) = parser_debugger_extract_string(&mut ed_container, &tokens, 5){
                            return Err(errrrrr)
                        }
                        if tokens.len() > 7{
                            cond = Some(tokens[6..].to_vec())
                        }
                        return Ok(AST::EditRow(AST_EDIT_ROW{
                            col_nam:ed_col_name,
                            col_val: ed_col_type,
                            container: ed_container,
                            conditions: cond.unwrap_or_default()
                        }))
                    },
                    _ => {
                        return Err(gerr("Invalid instance type"));
                    }
                }
            },
            _ => {
                return Err(gerr("Invalid command"));
            }
        }
    }
    return Err(gerr("Missing the instance to be editted"));
}

fn debug_tokens(tokens: &Vec<Token>) -> Result<AST, Error> {
    let first = tokens.first().ok_or_else(|| gerr("Token list is empty"))?;
    if let Token::Keyword(command) = first {
        match command.as_str() {
            "CREATE" => debug_create_command(tokens),
            "EDIT" => debug_edit_command(tokens),
            _ => Err(gerr("Invalid command keyword")),
        }
    } else {
        Err(gerr("First token is not a keyword"))
    }
}


fn parse(input : String) -> Result<AST, Error>{
    let tokens = match lexer(input){
        Ok(a) => {println!("{:#?}",a);a},
        Err(e) => {return Err(e)}
    };
    return debug_tokens(&tokens)
}


    let mut total_time: u128 = 0;
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        total_time += start.elapsed().as_nanos();
    }
    
    print!("\n\n\nBenchmark:\n|  Iterations: {}\n|  Total time: {}ns\n|  Task time: {}ns\n|  Total time: {}ms\n|  Task time: {}ms\n\n\n",iterations,total_time as u128,(total_time as f64 / iterations as f64),total_time as u128/1_000_000,(total_time as f64 / iterations as f64)/(1_000_000.0 as f64))
}
fn main(){
    println!("{:?}",parse("EDIT ROW ['myfavorite']['abc'] ON 'nicenice'".to_string()))
    
}
