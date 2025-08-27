mod alba_types;
mod burning_map;
mod container;
mod database;
mod indexing;
mod query;
mod query_conditions;
mod row;
use alba_types::AlbaTypes;
use database::connect;
use std::io::{Error, ErrorKind};

pub mod better_logs;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(String),
    String(String),
    Bytes(Vec<u8>),
    Int(i64),
    Geo((f64, f64)),
    Huge(i128),
    UHuge(u128),
    Float(f64),
    Bool(bool),
    Operator(String),
    Group(Vec<Token>),
    SubCommand(Vec<Token>),
    Argument,
}
#[derive(Debug, Clone, PartialEq)]
enum AST {
    CreateContainer(AstCreateContainer),
    CreateRow(AstCreateRow),
    EditRow(AstEditRow),
    DeleteRow(AstDeleteRow),
    DeleteContainer(AstDeleteContainer),
    Search(AstSearch),
    Commit(AstCommit),
    Rollback(AstRollback),
}

#[derive(Debug, Clone, PartialEq)]
struct AstCreateContainer {
    name: String,
    col_nam: Vec<String>,
    col_val: Vec<AlbaTypes>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstCreateRow {
    col_nam: Vec<String>,
    col_val: Vec<AlbaTypes>,
    container: String,
}
#[derive(Debug, Clone, PartialEq)]
struct AstEditRow {
    col_nam: Vec<String>,
    col_val: Vec<AlbaTypes>,
    container: String,
    conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>),
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteRow {
    container: String,
    conditions: Option<(Vec<(Token, Token, Token)>, Vec<(usize, char)>)>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstDeleteContainer {
    container: String,
}

type AlbaContainer = String;

#[derive(Debug, Clone, PartialEq)]
struct AstSearch {
    container: AlbaContainer,
    conditions: (Vec<(Token, Token, Token)>, Vec<(usize, char)>),
    col_nam: Vec<String>,
}
#[derive(Debug, Clone, PartialEq)]
struct AstCommit {
    container: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct AstRollback {
    container: Option<String>,
}

fn gerr(msg: &str) -> Error {
    Error::new(ErrorKind::Other, msg.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = match connect() {
        Ok(database) => {
            println!("connected");
            database
        }
        Err(e) => panic!("{}", e.to_string()),
    };
    if let Err(e) = db.run_database() {
        logerr!("{}", e);
    };
    Ok(())
}
