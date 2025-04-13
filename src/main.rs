mod lexer_functions;
mod database;
use std::{fs::File, io::{Error,ErrorKind}, sync::Arc, time::Instant};
use tokio::sync::Mutex;
use tokio;
use database::connect;
use lexer_functions::{
    lexer_boolean_match, lexer_group_match, lexer_ignore_comments_match, lexer_keyword_match, lexer_number_match, lexer_operator_match, lexer_string_match, lexer_subcommand_match, AlbaTypes, Token
};


fn lexer(input: String) -> Result<Vec<Token>, Error> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()));
    }

    let mut characters = input.trim().chars().peekable();
    let mut result = Vec::with_capacity(20);
    let mut dough = String::new();

    while let Some(c) = characters.next() {
        dough.push(c);
        lexer_ignore_comments_match(&mut dough, &mut characters);
        lexer_keyword_match(&mut result, &mut dough);
        lexer_subcommand_match(&mut result, &mut dough, &mut characters)?;
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters);
        lexer_operator_match(&mut result, &mut dough, &mut characters);
        lexer_number_match(&mut result, &mut dough, &mut characters);
        lexer_string_match(&mut result, &mut dough, &mut characters);
    }

    if !dough.trim().is_empty() {
        lexer_keyword_match(&mut result, &mut dough);
        lexer_subcommand_match(&mut result, &mut dough, &mut characters)?;
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters);
        lexer_operator_match(&mut result, &mut dough, &mut characters);
        lexer_number_match(&mut result, &mut dough, &mut characters);
        lexer_string_match(&mut result, &mut dough, &mut characters);
    }

    if !dough.trim().is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, format!("Unexpected token '{}' at end of input. Expected a valid keyword, string, number, boolean, group, or operator.", dough)));
    }

    if result.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "The given input did not produced tokens".to_string()));
    }

    Ok(result)
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
    CreateContainer(AstCreateContainer),
    CreateRow(AstCreateRow),
    EditRow(AstEditRow),
    DeleteRow(AstDeleteRow),
    DeleteContainer(AstDeleteContainer),
    Search(AstSearch)
}



#[derive(Debug, Clone, PartialEq)]
struct AstCreateContainer{
    name : String,
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstCreateRow{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String
}
#[derive(Debug, Clone, PartialEq)]
struct AstEditRow{
    col_nam : Vec<String>,
    col_val : Vec<AlbaTypes>,
    container : String,
    conditions : (Vec<(Token,Token,Token)>,Vec<(usize,char)>)
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteRow{
    container : String,
    conditions : Option<Vec<Token>>
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteContainer{
    container : String,
}

#[derive(Debug, Clone, PartialEq)]
enum AlbaContainer {
    Real(String),
    Virtual(Vec<Token>)
}

#[derive(Debug, Clone, PartialEq)]
struct AstSearch{
    container : Vec<AlbaContainer>,
    conditions : (Vec<(Token,Token,Token)>,Vec<(usize,char)>),
    col_nam : Vec<String>,
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
            _ => { return Some(gerr(&format!("Expected a string token at position {}, but found {:?}", index, cn))); }
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
                        
                        return Ok(AST::CreateContainer(AstCreateContainer { name: cname, col_nam: col_name, col_val: col_types }))
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
                        return Ok(AST::CreateRow(AstCreateRow { col_nam: col_names, col_val: col_values, container: container }))
                        
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
                        let mut conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>) = (Vec::with_capacity(10), Vec::with_capacity(10));   

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
                        
                        if let Some(tok) = tokens.get(6) {
                            if match tok {
                                Token::Keyword(a) if a.to_uppercase() == "WHERE" => false,
                                _ => true,
                            } {
                                return Err(gerr(&format!(r#"In EDIT ROW command, expected keyword 'WHERE' at position 6, but found {:?}"#, tok)));
                            }

                            if let Some(iterator_of_tokens) = tokens.get(7..) {
                                let mut bushes: Vec<Token> = Vec::new();

                                for i in iterator_of_tokens {
                                    if bushes.len() == 3 {
                                        conditions.0.push((
                                            get_from_bushes_with_safety(&bushes, 0)?,
                                            get_from_bushes_with_safety(&bushes, 1)?,
                                            get_from_bushes_with_safety(&bushes, 2)?,
                                        ));
                                        bushes.clear();

                                        conditions.1.push((
                                            conditions.0.len() - 1,
                                            match i {
                                                Token::Keyword(a) => match a.to_uppercase().as_str() {
                                                    "OR" => 'o',
                                                    "AND" => 'a',
                                                    _ => {
                                                        return Err(gerr(
                                                            "Expected logical operator 'AND' or 'OR' after a condition",
                                                        ))
                                                    }
                                                },
                                                _ => return Err(gerr("Expected logical operator after condition")),
                                            },
                                        ));
                                        continue;
                                    }

                                    match i {
                                        Token::String(_) => match bushes.len() {
                                            0 | 2 => bushes.push(i.clone()),
                                            _ => return Err(gerr("Unexpected string: operator might be missing")),
                                        },
                                        Token::Bool(_) | Token::Int(_) | Token::Float(_) => {
                                            if bushes.len() == 2 {
                                                bushes.push(i.clone())
                                            } else {
                                                return Err(gerr("Unexpected value: condition must follow 'column OP value' pattern"));
                                            }
                                        }
                                        Token::Operator(_) => {
                                            if bushes.len() == 1 {
                                                bushes.push(i.clone());
                                            } else {
                                                return Err(gerr("Unexpected operator: check condition structure"));
                                            }
                                        }
                                        _ => return Err(gerr("Unexpected token in WHERE clause")),
                                    }
                                }

                                if bushes.len() == 3 {
                                    conditions.0.push((
                                        get_from_bushes_with_safety(&bushes, 0)?,
                                        get_from_bushes_with_safety(&bushes, 1)?,
                                        get_from_bushes_with_safety(&bushes, 2)?,
                                    ));
                                }
                            }
                        }

                        return Ok(AST::EditRow(AstEditRow{
                            col_nam:ed_col_name,
                            col_val: ed_col_type,
                            container: ed_container,
                            conditions
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

fn get_from_bushes_with_safety(bushes : &Vec<Token>,index : usize) -> Result<Token,Error>{
    match bushes.get(index){ Some(a) => Ok(a.clone()), None => return Err(gerr("Missing"))}
}

fn debug_search(tokens: &Vec<Token>) -> Result<AST, Error> {
    let container: Vec<AlbaContainer> = match tokens.get(3) {
        Some(s) => match s {
            Token::Group(a) => {
                let mut containers : Vec<AlbaContainer> = Vec::new();
                for i in a{
                    match i{
                        Token::String(str) => {containers.push(AlbaContainer::Real(str.clone()));},
                        Token::SubCommand(a) => {containers.push(AlbaContainer::Virtual(a.clone()))},
                        _ => {
                            return Err(gerr("..."));
                        }
                    }
                }
                containers
            },
            _ => return Err(gerr("Expected container group as a group at position 3")),
        },
        None => return Err(gerr("Missing container group (expected at position 3)")),
    };

    let mut conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>) =
        (Vec::with_capacity(10), Vec::with_capacity(10));

    let columns: Vec<String> = match tokens.get(1) {
        Some(a) => match a {
            Token::Group(g) => {
                g.iter()
                    .map(|a| match a {
                        Token::String(s) => s.to_string(),
                        _ => "".to_string(),
                    })
                    .collect()
            }
            _ => return Err(gerr("Expected a group of column names (strings) at position 1")),
        },
        None => return Err(gerr("Missing column group (expected at position 1)")),
    };

    if let Some(tok) = tokens.get(2) {
        if match tok {
            Token::Keyword(a) if a.to_uppercase() == "ON" => false,
            _ => true,
        } {
            return Err(gerr(&format!(r#"In SEARCH command, expected keyword 'ON' at position 2, but found {:?}"#, tok)));
        }
    }

    if let Some(tok) = tokens.get(4) {
        if match tok {
            Token::Keyword(a) if a.to_uppercase() == "WHERE" => false,
            _ => true,
        } {
            return Err(gerr(r#"Expected keyword "WHERE" at position 4"#));
        }

        if let Some(iterator_of_tokens) = tokens.get(5..) {
            let mut bushes: Vec<Token> = Vec::new();

            for i in iterator_of_tokens {
                if bushes.len() == 3 {
                    conditions.0.push((
                        get_from_bushes_with_safety(&bushes, 0)?,
                        get_from_bushes_with_safety(&bushes, 1)?,
                        get_from_bushes_with_safety(&bushes, 2)?,
                    ));
                    bushes.clear();

                    conditions.1.push((
                        conditions.0.len() - 1,
                        match i {
                            Token::Keyword(a) => match a.to_uppercase().as_str() {
                                "OR" => 'o',
                                "AND" => 'a',
                                _ => {
                                    return Err(gerr(
                                        "Expected logical operator 'AND' or 'OR' after a condition",
                                    ))
                                }
                            },
                            _ => return Err(gerr("Expected logical operator after condition")),
                        },
                    ));
                    continue;
                }

                match i {
                    Token::String(_) => match bushes.len() {
                        0 | 2 => bushes.push(i.clone()),
                        _ => return Err(gerr("Unexpected string: operator might be missing")),
                    },
                    Token::Bool(_) | Token::Int(_) | Token::Float(_) => {
                        if bushes.len() == 2 {
                            bushes.push(i.clone())
                        } else {
                            return Err(gerr("Unexpected value: condition must follow 'column OP value' pattern"));
                        }
                    }
                    Token::Operator(_) => {
                        if bushes.len() == 1 {
                            bushes.push(i.clone());
                        } else {
                            return Err(gerr("Unexpected operator: check condition structure"));
                        }
                    }
                    _ => return Err(gerr("Unexpected token in WHERE clause")),
                }
            }

            if bushes.len() == 3 {
                conditions.0.push((
                    get_from_bushes_with_safety(&bushes, 0)?,
                    get_from_bushes_with_safety(&bushes, 1)?,
                    get_from_bushes_with_safety(&bushes, 2)?,
                ));
            }
        }
    }

    Ok(AST::Search(AstSearch {
        container,
        conditions,
        col_nam: columns,
    }))
}

fn debug_tokens(tokens: &Vec<Token>) -> Result<AST, Error> {
    let first = tokens.first().ok_or_else(|| gerr("Token list is empty"))?;
    if let Token::Keyword(command) = first {
        return match command.to_uppercase().as_str() {
            "CREATE" => debug_create_command(tokens),
            "EDIT" => debug_edit_command(tokens),
            "SEARCH" => debug_search(tokens),
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

/* Test Functions */
async fn test_container_creation(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Container Creation");

    let query = "CREATE CONTAINER 'test_container' [ 'id' ] [ INT ]";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to create container with one column");

    let result = db.execute(query).await;
    assert!(result.is_err(), "Should not be able to create duplicate container");

    let mut col_names: Vec<String> = (0..50).map(|i| format!("col{}", i)).collect();
    let col_types = vec!["INT"; 50];
    let col_names_str = col_names.iter().map(|s| format!("'{}'", s)).collect::<Vec<_>>().join(", ");
    let col_types_str = col_types.join(", ");
    let query = format!("CREATE CONTAINER 'max_container' [ {} ] [ {} ]", col_names_str, col_types_str);
    let result = db.execute(&query).await;
    assert!(result.is_ok(), "Failed to create container with maximum columns");

    col_names.push("col50".to_string());
    let col_names_str = col_names.iter().map(|s| format!("'{}'", s)).collect::<Vec<_>>().join(", ");
    let col_types_str = vec!["INT"; 51].join(", ");
    let query = format!("CREATE CONTAINER 'over_max_container' [ {} ] [ {} ]", col_names_str, col_types_str);
    let result = db.execute(&query).await;
    assert!(result.is_err(), "Should not create container with more than 50 columns");

    let query = "CREATE CONTAINER 'mixed_container' [ 'id', 'name', 'age', 'active', 'salary' ] [ INT, TEXT, BIGINT, BOOL, FLOAT ]";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to create container with mixed data types");

    println!("Test passed: Container Creation");
    Ok(())
}

async fn test_row_insertion(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Row Insertion");

    let query = "CREATE ROW [ 'id' ] [ 1 ] ON 'test_container'";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to insert row");

    let query = "CREATE ROW [ 'id' ] [ 2 ] ON 'test_container'";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to insert another row");

    let query = "CREATE ROW [ 'id' ] [ 'not_an_int' ] ON 'test_container'";
    let result = db.execute(query).await;
    assert!(result.is_err(), "Should not insert string into INT column");

    let query = "CREATE ROW [ 'id', 'name', 'age', 'active', 'salary' ] [ 1, 'Alice', 30, true, 50000.0 ] ON 'mixed_container'";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to insert row into mixed_container");

    println!("Test passed: Row Insertion");
    Ok(())
}

async fn test_searching(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Searching");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ]";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 2, "Should have 2 rows");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' > 1";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 1, "Should have 1 row where id > 1");

    let query = "SEARCH [ 'id', 'name', 'age' ] ON [ 'mixed_container' ] WHERE 'age' > 20 AND 'name' = 'Alice'";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 1, "Should have 1 row matching complex conditions");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' <= 2 OR 'id' = 1";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 2, "Should match rows with id <= 2 or id = 1");

    db.execute("CREATE ROW [ 'id', 'name', 'age', 'active', 'salary' ] [ 2, 'Alicia', 25, true, 60000.0 ] ON 'mixed_container'").await?;
    let query = "SEARCH [ 'name' ] ON [ 'mixed_container' ] WHERE 'name' &> 'Ali'";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 2, "Should match names containing 'Ali'");

    let query = "SEARCH [ 'name' ] ON [ 'mixed_container' ] WHERE 'name' &&> 'ali'";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 2, "Should match case-insensitive 'alice'");

    db.execute("CREATE CONTAINER 'test_container2' [ 'id' ] [ INT ]").await?;
    db.execute("CREATE ROW [ 'id' ] [ 3 ] ON 'test_container2'").await?;
    let query = "SEARCH [ 'id' ] ON [ 'test_container', 'test_container2' ] WHERE 'id' > 1";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 2, "Should aggregate rows from both containers");

    for i in 3..10 {
        db.execute(&format!("CREATE ROW [ 'id' ] [ {} ] ON 'test_container'", i)).await?;
    }
    let query = "SEARCH [ 'id' ] ON [ 'test_container' ]";
    let mut result = db.execute(query).await?;
    assert!(result.pages.len() >= 1, "Should have at least one page");
    let initial_rows = result.rows.1.len();
    result.next(db)?;
    assert_ne!(result.rows.1.len(), initial_rows, "Pagination should change row set");

    db.execute("CREATE CONTAINER 'null_container' [ 'id', 'value' ] [ INT, TEXT ]").await?;
    db.execute("CREATE ROW [ 'id', 'value' ] [ 1, '' ] ON 'null_container'").await?;
    let query = "SEARCH [ 'id' ] ON [ 'null_container' ] WHERE 'value' = 'test'";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 0, "Should exclude rows with empty/NULL-like values");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'invalid_col' = 1";
    let result = db.execute(query).await;
    assert!(result.is_err(), "Should error on invalid column name");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' > 100";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.1.len(), 0, "Should return 0 rows when no match");

    println!("Test passed: Searching");
    Ok(())
}

async fn test_row_editing(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Row Editing");

    let query = "EDIT ROW [ 'id' ] [ 3 ] ON 'test_container' WHERE 'id' = 1";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to update row");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' = 3";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.0.len(), 1, "Should have 1 row with id = 3");

    println!("Test passed: Row Editing");
    Ok(())
}

async fn test_row_deletion(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Row Deletion");

    let query = "DELETE ROW ON 'test_container' WHERE 'id' = 2";
    let result = db.execute(query).await;
    assert!(result.is_ok(), "Failed to delete row");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ]";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.0.len(), 1, "Should have 1 row left");

    println!("Test passed: Row Deletion");
    Ok(())
}

async fn test_transaction_handling(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Transaction Handling");

    db.execute("CREATE ROW [ 'id' ] [ 4 ] ON 'test_container'").await?;
    let result = db.execute("SEARCH [ 'id' ] ON [ 'test_container' ]").await?;
    assert_eq!(result.rows.0.len(), 2, "Should see uncommitted row in mvcc");

    db.rollback().await?;
    let result = db.execute("SEARCH [ 'id' ] ON [ 'test_container' ]").await?;
    assert_eq!(result.rows.0.len(), 1, "Should not see rolled back row");

    db.execute("CREATE ROW [ 'id' ] [ 4 ] ON 'test_container'").await?;
    db.commit().await?;
    let result = db.execute("SEARCH [ 'id' ] ON [ 'test_container' ]").await?;
    assert_eq!(result.rows.0.len(), 2, "Should see committed row");

    println!("Test passed: Transaction Handling");
    Ok(())
}

async fn test_error_handling(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Error Handling");

    let query = "CREATE CONTAINER invalid_container [ 'id' ] [ UNKNOWN ]";
    let result = db.execute(query).await;
    assert!(result.is_err(), "Should not create container with invalid type");

    let query = "CREATE ROW [ 'id' ] [ 1 ] ON 'non_existent'";
    let result = db.execute(query).await;
    assert!(result.is_err(), "Should not insert into non-existent container");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' = ";
    let result = db.execute(query).await;
    assert!(result.is_err(), "Should not execute invalid search query");

    println!("Test passed: Error Handling");
    Ok(())
}

async fn test_performance(db: &mut database::Database) -> Result<(), Error> {
    println!("Running test: Performance");

    for i in 0..10000 {
        let query = format!("CREATE ROW [ 'id' ] [ {} ] ON 'test_container'", i);
        db.execute(&query).await?;
    }
    db.commit().await?;
    let query = "SEARCH [ 'id' ] ON [ 'test_container' ]";
    let result = db.execute(query).await?;
    assert_eq!(result.rows.0.len(), 10000, "Should have 10000 rows");

    let start = Instant::now();
    let result = db.execute(query).await?;
    let duration = start.elapsed();
    println!("Time to search 10,000 rows: {:?}", duration);
    assert_eq!(result.rows.0.len(), 10000, "Should retrieve 10000 rows");

    let query = "SEARCH [ 'id' ] ON [ 'test_container' ] WHERE 'id' > 5000";
    let start = Instant::now();
    let result = db.execute(query).await?;
    let duration = start.elapsed();
    println!("Time to search with condition on 10,000 rows: {:?}", duration);
    assert_eq!(result.rows.0.len(), 4999, "Should have 4999 rows where id > 5000");

    println!("Test passed: Performance");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/home/theo/Desktop/%test%/";
    let mut db = connect(db_path).await?;
    test_container_creation(&mut db).await?;
    test_row_insertion(&mut db).await?;
    test_searching(&mut db).await?;
    test_row_editing(&mut db).await?;
    test_row_deletion(&mut db).await?;
    test_transaction_handling(&mut db).await?;
    test_error_handling(&mut db).await?;
    test_performance(&mut db).await?;

    println!("All tests completed successfully!");
    Ok(())
}
