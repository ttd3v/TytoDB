use std::io::Error;

use crate::{gerr, lexer, lexer_functions::{AlbaTypes, Token}, AlbaContainer, AstCommit, AstCreateContainer, AstCreateRow, AstEditRow, AstQueryControlExit, AstQueryControlNext, AstQueryControlPrevious, AstRollback, AstSearch, AST};



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
                        let mut col_name_holder : Vec<String> = Vec::with_capacity(5);
                        for i in col_name.iter(){
                            if col_name_holder.contains(&i){
                                return Err(gerr("Repeated column names"))
                            }
                            col_name_holder.push(i.clone());
                        }
                        drop(col_name_holder);
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

fn debug_qycnpvs(tokens : &Vec<Token>) -> Result<AST, Error> {
    if let Some(ii) = tokens.get(1) {
        if let Token::String(a) = ii{
            return Ok(AST::QueryControlPrevious(AstQueryControlPrevious{id:a.to_string()}))
        }
    }
    Err(gerr("Missing container"))
}

fn debug_qycnnxt(tokens : &Vec<Token>) -> Result<AST, Error> {
    if let Some(ii) = tokens.get(1) {
        if let Token::String(a) = ii{
            return Ok(AST::QueryControlNext(AstQueryControlNext{id:a.to_string()}))
        }
    }
    Err(gerr("Missing container"))
}
fn debug_qycnext(tokens : &Vec<Token>) -> Result<AST, Error> {
    if let Some(ii) = tokens.get(1) {
        if let Token::String(a) = ii{
            return Ok(AST::QueryControlExit(AstQueryControlExit{id:a.to_string()}))
        }
    }
    Err(gerr("Missing container"))
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
            "COMMIT"|"ROLLBACK" => debug_finishers_command(tokens),
            "QYCNPVS" => debug_qycnpvs(tokens),
            "QYCNNXT" => debug_qycnnxt(tokens),
            "QYCNEXT" => debug_qycnext(tokens),
            _ => Err(gerr("Invalid command keyword")),
        }
    } else {
        Err(gerr("First token is not a keyword"))
    }
}


fn debug_finishers_command(tokens : &Vec<Token>) -> Result<AST,Error> {
    if let Some(kw) = tokens.get(0){
        if let Token::Keyword(st) = kw {
            match st.to_uppercase().as_str(){
                "COMMIT" => {
                    let con : Option<String> = match tokens.get(1){
                        Some(ttt) => match ttt{
                            Token::String(a) => Some(a.to_string()),
                            _ => None
                        },
                        None => None
                    };
                    return Ok(AST::Commit(AstCommit{
                        container:con
                    }))
                },
                "ROLLBACK" => {
                    let con : Option<String> = match tokens.get(1){
                        Some(ttt) => match ttt{
                            Token::String(a) => Some(a.to_string()),
                            _ => None
                        },
                        None => None
                    };
                    return Ok(AST::Rollback(AstRollback{
                        container:con
                    }))
                },
                _ => {
                    return Err(gerr("Failed to process the entered finisher"));
                }
            }
        }
    }
    return Err(gerr("Missing the instance to be editted"));
}


pub fn parse(input : String,arguments_input : Vec<String>) -> Result<AST, Error>{
    let mut arguments : Vec<Token> = Vec::with_capacity(arguments_input.len());
    for i in arguments_input{
        let toks = lexer(i)?;
        if toks.len() > 1{
            return Err(gerr("Invalid argument"))
        }
        if let Some(tttoks) = toks.first(){
            match tttoks{
                Token::Bool(a) => arguments.push(Token::Bool(*a)),
                Token::Int(a) => arguments.push(Token::Int(*a)),
                Token::Float(a) => arguments.push(Token::Float(*a)),
                Token::String(a) => arguments.push(Token::String(a.to_string())),
                _ => return Err(gerr("Invalid argument"))
            }
        }
    }

    let mut tokens: Vec<Token> = match lexer(input){
        Ok(a) => {a},
        Err(e) => {return Err(e)}
    };
    let mut arg_iter = arguments.iter();
    for token in tokens.iter_mut() {
        if *token == Token::Argument {
            *token = match arg_iter.next(){
                Some(arg) => arg.to_owned(),
                None => {
                    return Err(gerr("Not enough arguments"))
                }
            }
        }
    }

    if arg_iter.next().is_some() {
        return Err(gerr("Too many arguments"));
    }
    drop(arguments);
    return debug_tokens(&tokens)
}
