mod lexer_functions;
mod database;
mod container;
mod parser;
mod index_tree;
mod index_sizes;
mod strix;
use std::{io::{Error,ErrorKind}, iter, sync::Arc};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio;
use database::connect;
use lexer_functions::{
    lexer_boolean_match, lexer_group_match, lexer_ignore_comments_match, lexer_keyword_match, lexer_number_match, lexer_operator_match, lexer_string_match, lexer_subcommand_match, AlbaTypes, Token
};
use crate::database::Database;
pub mod better_logs;

fn lexer(input: String) -> Result<Vec<Token>, Error> {
    if input.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "Input cannot be blank".to_string()));
    }

    let mut characters = input.trim().chars().peekable();
    let mut result = Vec::with_capacity(20);
    let mut dough = String::new();

    while let Some(c) = characters.next() {
        if c == '?'{
            result.push(Token::Argument);
            continue;
        }
        dough.push(c);
        lexer_ignore_comments_match(&mut dough, &mut characters);
        lexer_keyword_match(&mut result, &mut dough);
        lexer_subcommand_match(&mut result, &mut dough, &mut characters)?;
        lexer_group_match(&mut result, &mut dough, &mut characters);
        lexer_boolean_match(&mut result, &mut dough, &mut characters);
        lexer_number_match(&mut result, &mut dough, &mut characters);
        lexer_operator_match(&mut result, &mut dough, &mut characters);
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
        result.push(Token::String(dough))
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
    Search(AstSearch),
    Commit(AstCommit),
    Rollback(AstRollback),
    QueryControlNext(AstQueryControlNext),
    QueryControlPrevious(AstQueryControlPrevious),
    QueryControlExit(AstQueryControlExit),
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
    conditions : Option<(Vec<(Token,Token,Token)>,Vec<(usize,char)>)>
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
#[derive(Debug, Clone, PartialEq)]
struct AstCommit{
    container : Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct AstRollback{
    container : Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlNext{
    id : String,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlPrevious{
    id : String,
}
#[derive(Debug, Clone, PartialEq)]
struct AstQueryControlExit{
    id : String,
}

fn gerr(msg : &str) -> Error{
    return Error::new(ErrorKind::Other, msg.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = match connect().await{
        Ok(database) => database,
        Err(e) => panic!("{}",e.to_string())
    };
    if let Err(e) = db.run_database().await{
        logerr!("{}",e);
    };
    Ok(())
}



#[cfg(test)]
mod tests {

    use super::*;

    async fn run_tests(mut db : Database){
        println!("===== Starting tests =====");
        db.execute(
            "CREATE CONTAINER 'test_container' ['col0','col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11'][INT,BIGINT,FLOAT,BOOL,SMALL-STRING,MEDIUM-STRING,BIG-STRING,LARGE-STRING,SMALL-BYTES,MEDIUM-BYTES,BIG-BYTES,LARGE-BYTES]",
            Vec::new(),
        )
        .await
        .unwrap();        let mut rng = thread_rng();
        for i in 0..1_000_000{
            let small_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2) 
                .collect();
            let medium_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2)
                .collect();
            let big_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2) 
                .collect();
            let large_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2)
                .collect();
    
            let small_bytes: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect(); 
            let medium_bytes: Vec<u8> = (0..256).map(|_| rng.r#gen()).collect(); 
            let big_bytes: Vec<u8> = (0..1024).map(|_| rng.r#gen()).collect(); 
            let large_bytes: Vec<u8> = (0..10_240).map(|_| rng.r#gen()).collect(); 
            let small_bytes_b64 = STANDARD.encode(small_bytes);
            let medium_bytes_b64 = STANDARD.encode(medium_bytes);
            let big_bytes_b64 = STANDARD.encode(big_bytes);
            let large_bytes_b64 = STANDARD.encode(large_bytes);
            let arguments = vec![
                i.to_string(),
                i.to_string(),
                i.to_string(),
                if i > 50000{"TRUE".to_string()}else{"FALSE".to_string()},
                format!("'{}'", small_string),
                format!("'{}'", medium_string),
                format!("'{}'", big_string),
                format!("'{}'", large_string),
                format!("'{}'", small_bytes_b64),
                format!("'{}'", medium_bytes_b64),
                format!("'{}'", big_bytes_b64),
                format!("'{}'", large_bytes_b64)
            ];
            db.execute(
                "CREATE ROW ['col0','col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11'][?,?,?,?,?,?,?,?,?,?,?,?] ON 'test_container'",
                arguments,
            )
            .await
            .unwrap();
            db.execute("COMMIT", Vec::new()).await.unwrap();
        }
        for i in 0..1_000_000{
            let small_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2) 
                .collect();
            let medium_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2)
                .collect();
            let big_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2) 
                .collect();
            let large_string: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(2)
                .collect();
    
            let small_bytes: Vec<u8> = (0..32).map(|_| rng.r#gen()).collect(); 
            let medium_bytes: Vec<u8> = (0..256).map(|_| rng.r#gen()).collect(); 
            let big_bytes: Vec<u8> = (0..1024).map(|_| rng.r#gen()).collect(); 
            let large_bytes: Vec<u8> = (0..10_240).map(|_| rng.r#gen()).collect(); 
            let small_bytes_b64 = STANDARD.encode(small_bytes);
            let medium_bytes_b64 = STANDARD.encode(medium_bytes);
            let big_bytes_b64 = STANDARD.encode(big_bytes);
            let large_bytes_b64 = STANDARD.encode(large_bytes);
            let random_int = rng.gen_range(1..1_000_000);
            let arguments = vec![
                i.to_string(),
                i.to_string(),
                i.to_string(),
                if i > 50000{"TRUE".to_string()}else{"FALSE".to_string()},
                format!("'{}'", small_string),
                format!("'{}'", medium_string),
                format!("'{}'", big_string),
                format!("'{}'", large_string),
                format!("'{}'", small_bytes_b64),
                format!("'{}'", medium_bytes_b64),
                format!("'{}'", big_bytes_b64),
                format!("'{}'", large_bytes_b64),
                random_int.to_string()
            ];
            db.execute("EDIT ROW ['col0','col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11'][?,?,?,?,?,?,?,?,?,?,?,?] ON 'test_container' WHERE 'col0' <= ?", arguments).await.unwrap();
            db.execute("COMMIT", Vec::new()).await.unwrap();
        }
        for _ in 0..10_000{
            let random_int = rng.gen_range(1..10_000);
            db.execute("DELETE ROW ['col0','col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11'] ON 'test_container' WHERE 'col0' = ?", vec![random_int.to_string()]).await.unwrap();
            db.execute("COMMIT", Vec::new()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn main() {

        let db: Database = match connect().await{
            Ok(database) => database,
            Err(e) => panic!("{}",e.to_string())
        };

        run_tests(db).await;
    }
}