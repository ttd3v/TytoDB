mod create_inputs_for_testing;
mod lexer_functions;
mod database;
use std::{io::{Error,ErrorKind}, sync::Arc};
use tokio::sync::Mutex;
use tokio;
use database::connect;
use lexer_functions::{
    lexer_boolean_match,
    lexer_group_match,
    lexer_ignore_comments_match,
    lexer_string_match,
    lexer_keyword_match,
    lexer_operator_match,
    lexer_number_match,
    Token,
    AlbaTypes
};


fn lexer(input : String)->Result<Vec<Token>,Error>{
    if input.len() == 0{
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()))
    }
    let mut characters = input.trim().chars().peekable();
    let mut result:Vec<Token> = Vec::with_capacity(20);
    let mut dough : String = String::new();

    while let Some(c) = characters.next(){
        dough.push(c);
        lexer_ignore_comments_match(&mut dough, &mut characters) ;
        lexer_keyword_match(&mut result, &mut dough);
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters) ;
        lexer_operator_match(&mut result, &mut dough, &mut characters) ;
        lexer_number_match(&mut result, &mut dough, &mut characters) ;
        lexer_string_match(&mut result, &mut dough, &mut characters) ;
    }
    
    if result.len() > 0{
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
            _ => {print!("{:?}",output);return Some(gerr("Invalid type, must be a string"));}
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
                    match AlbaTypes::try_from(item.clone()) {
                        Ok(value) => output.push(value),
                        Err(e) => return Some(gerr(&format!("{}",e))),
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
                        if let Some(cn) = tokens.get(5){
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
                        return Ok(AST::CreateRow(AST_CREATE_ROW{
                            col_nam:ed_col_name,
                            col_val: ed_col_type,
                            container: ed_container
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
        Ok(a) => {a},
        Err(e) => {return Err(e)}
    };
    return debug_tokens(&tokens)
}

#[tokio::main]
async fn main() {

    let steps = 100_000;

    match connect("/home/theo/Desktop/tytodb").await {
        Ok(mut c) => {
            for _ in 1..steps {
                if let Err(e) = c.execute("
                CREATE ROW ['text']['Lorem ipsum dolor sit amet'] ON 'abcd'").await{
                    panic!("{}",e);
                }
            }
            if let Err(e) = c.commit().await{
                panic!("err: {}",e);
            }
        }
        Err(e) => eprintln!("Error connecting to database: {}", e),
    }


}
