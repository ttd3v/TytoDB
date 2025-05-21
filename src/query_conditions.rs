use std::{collections::HashMap, io::{self, Error, ErrorKind}};

use ahash::AHashMap;
use regex::Regex;

use crate::{gerr, lexer_functions::{AlbaTypes, Token}, query::PrimitiveQueryConditions, row::Row};


fn string_to_char(s: String) -> Result<char, io::Error> {
    let mut chars = s.chars();

    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => Err(Error::new(ErrorKind::InvalidInput, "Input must be exactly one character")),
    }
}

enum Operator{
    Equal,
    StrictEqual,
    Greater,
    Lower,
    GreaterEquality,
    LowerEquality,
    Different,
    StringContains,
    StringCaseInsensitiveContains,
    StringRegularExpression
}

#[derive(Clone, Copy)]
enum LogicalGate{
    And,
    Or,
}

pub struct QueryConditonAtom{
    column : String,
    operator : Operator,
    value : AlbaTypes,
}

pub struct QueryConditions{
    chain : Vec<(QueryConditonAtom,Option<LogicalGate>)>
}

fn gather_regex<'a>(regex_map: &'a mut HashMap<String, Regex>, key: String) -> Result<&'a Regex, Error> {
    if regex_map.contains_key(&key) {
        return Ok(regex_map.get(&key).unwrap());
    }
    let reg = match Regex::new(&key) {
        Ok(a) => a,
        Err(e) => return Err(gerr(&e.to_string())),
    };
    regex_map.insert(key.clone(), reg);
    Ok(regex_map.get(&key).unwrap())
}

impl QueryConditions{
    pub fn from_primitive_conditions(&self,primitive_conditions : PrimitiveQueryConditions, column_properties : &HashMap<String,AlbaTypes>) -> Result<Self,Error>{
        let mut chain : Vec<(QueryConditonAtom,Option<LogicalGate>)> = Vec::new();
        let condition_chunk = primitive_conditions.0;
        let condition_logical_gates_vec = primitive_conditions.1;
        let mut condition_logical_gates = AHashMap::new();
        for i in condition_logical_gates_vec{
            condition_logical_gates.insert(i.0, match i.1{
                'a'|'A' => LogicalGate::And,
                'o'|'O' => LogicalGate::Or,
                _ => return  Err(gerr("Failed to load LogicalGate, invalid token."))
            });
        }
        for (index,value) in condition_chunk.iter().enumerate(){
            let value = value.to_owned();
            
            let column = if let Token::String(name) = value.0{
                name
            }else{
                return Err(gerr("Failed to get QueryConditions, but failed to gather the column_name."))
            };
            
            let operator = if let Token::Operator(operator_name) = value.1{
                match operator_name.as_str(){
                    "=" => Operator::Equal,
                    "==" => Operator::StrictEqual,
                    ">=" => Operator::GreaterEquality,
                    "<=" => Operator::LowerEquality,
                    ">" => Operator::Greater,
                    "<" => Operator::Lower,
                    "!=" => Operator::Different,
                    "&>" => Operator::StringContains,
                    "&&>" => Operator::StringCaseInsensitiveContains,
                    "&&&>" => Operator::StringRegularExpression,
                    _ => {
                        return Err(gerr("Failed to get operator, invalid token contant."))
                    }
                }
            }else{
                return Err(gerr("Failed to get operator, invalid token,"))
            };

            let column_value = if let Some(column_type) = column_properties.get(&column){
                match column_type{
                    AlbaTypes::Text(_) => {
                        if let Token::String(string) = value.2{
                            AlbaTypes::Text(string)
                        }else {
                            return Err(gerr("No string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Int(_) => {
                        if let Token::Int(number) = value.2{
                            AlbaTypes::Int(number as i32)
                        }else {
                            return Err(gerr("No integer found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Bigint(_) => {
                        if let Token::Int(number) = value.2{
                            AlbaTypes::Bigint(number)
                        }else {
                            return Err(gerr("No integer found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Float(_) => {
                        if let Token::Float(number) = value.2{
                            AlbaTypes::Float(number)
                        }else {
                            return Err(gerr("No float found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Bool(_) => {
                        if let Token::Bool(bool) = value.2{
                            AlbaTypes::Bool(bool)
                        }else {
                            return Err(gerr("No bool found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::Char(_) => {
                        if let Token::String(char) = value.2{
                            AlbaTypes::Char(string_to_char(char)?)
                        }else {
                            return Err(gerr("No char found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NanoString(_) => {
                        if let Token::String(mut nano_string) = value.2{
                            nano_string.truncate(10);
                            AlbaTypes::NanoString(nano_string)
                        }else {
                            return Err(gerr("No nano_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::SmallString(_) => {
                        if let Token::String(mut small_string) = value.2{
                            small_string.truncate(100);
                            AlbaTypes::SmallString(small_string)
                        }else {
                            return Err(gerr("No small_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::MediumString(_) => {
                        if let Token::String(mut medium_string) = value.2{
                            medium_string.truncate(500);
                            AlbaTypes::SmallString(medium_string)
                        }else {
                            return Err(gerr("No medium_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::BigString(_) => {
                        if let Token::String(mut big_string) = value.2{
                            big_string.truncate(2000);
                            AlbaTypes::SmallString(big_string)
                        }else {
                            return Err(gerr("No big_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::LargeString(_) => {
                        if let Token::String(mut large_string) = value.2{
                            large_string.truncate(3000);
                            AlbaTypes::SmallString(large_string)
                        }else {
                            return Err(gerr("No large_string found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NanoBytes(_) => {
                        if let Token::Bytes(mut nano_bytes) = value.2{
                            nano_bytes.truncate(10);
                            AlbaTypes::NanoBytes(nano_bytes)
                        }else {
                            return Err(gerr("No nano_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::SmallBytes(_) => {
                        if let Token::Bytes(mut small_bytes) = value.2{
                            small_bytes.truncate(1000);
                            AlbaTypes::SmallBytes(small_bytes)
                        }else {
                            return Err(gerr("No small_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::MediumBytes(_) => {
                        if let Token::Bytes(mut medium_bytes) = value.2{
                            medium_bytes.truncate(10000);
                            AlbaTypes::MediumBytes(medium_bytes)
                        }else {
                            return Err(gerr("No medium_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::BigSBytes(_) => {
                        if let Token::Bytes(mut big_bytes) = value.2{
                            big_bytes.truncate(100000);
                            AlbaTypes::BigSBytes(big_bytes)
                        }else {
                            return Err(gerr("No big_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::LargeBytes(_) => {
                        if let Token::Bytes(mut large_bytes) = value.2{
                            large_bytes.truncate(1000000);
                            AlbaTypes::BigSBytes(large_bytes)
                        }else {
                            return Err(gerr("No large_bytes found in the ComparisionToken"))
                        }
                    },
                    AlbaTypes::NONE => {
                        return Err(gerr("Failed to extract the value from the column_properties"))
                    },
                } 
            }else{
                return Err(gerr("Failed to generate QueryConditions, that happened because no column_property has been found with the given column-names"))
            };

            let gate = condition_logical_gates
                .get(&index)
                .map(|a| a.clone());

            chain.push((QueryConditonAtom{column,operator,value:column_value},gate));
        }
        return Ok(QueryConditions { chain })
    }
    pub fn row_match(&self,row : Row) -> Result<bool,Error>{
        if self.chain.is_empty(){
            return Ok(true)
        }
        let mut regex_cache : HashMap<String,Regex> = HashMap::new();
        let mut result = false;
        for (query_atom,logicgate) in self.chain.iter(){
            let value = if let Some(val) = row.data.get(&query_atom.column){
                val
            }else{
                return Err(gerr("No value found to that column name"))
            };
            let check = match query_atom.operator{
                Operator::Equal|Operator::StrictEqual => {
                    query_atom.value == *value
                },
                Operator::Greater => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n > match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::Lower => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n < match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::GreaterEquality => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n >= match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::LowerEquality => {
                    match query_atom.value {
                        AlbaTypes::Int(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as i32,
                                AlbaTypes::Float(n) => n as i32,
                                AlbaTypes::Bigint(n) => n as i32,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Bigint(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as i64,
                                AlbaTypes::Float(n) => n as i64,
                                AlbaTypes::Bigint(n) => n as i64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        AlbaTypes::Float(n) => {
                            n <= match *value {
                                AlbaTypes::Int(n) => n as f64,
                                AlbaTypes::Float(n) => n as f64,
                                AlbaTypes::Bigint(n) => n as f64,
                                _ => {
                                    return Err(gerr("Invalid row type"))
                                }
                            }
                        },
                        _ => {
                            return Err(gerr("Invalid query atom type"))
                        }
                    }
                },
                Operator::Different => {
                    query_atom.value != *value
                },
                Operator::StringContains => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            s.contains(val)
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
                Operator::StringCaseInsensitiveContains => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            s.to_lowercase().contains(&val.to_lowercase())
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
                Operator::StringRegularExpression => {
                    match &query_atom.value{
                        AlbaTypes::NanoString(s)|AlbaTypes::SmallString(s)|AlbaTypes::MediumString(s)|AlbaTypes::BigString(s)|AlbaTypes::LargeString(s)|AlbaTypes::Text(s) => {
                            let val = match value{
                                AlbaTypes::NanoString(s) => s,
                                AlbaTypes::SmallString(s) => s,
                                AlbaTypes::MediumString(s) => s,
                                AlbaTypes::BigString(s) => s,
                                AlbaTypes::LargeString(s) => s,
                                AlbaTypes::Text(s) => s,
                                _ => return Err(gerr("Invalid row type"))
                            };
                            let reg = gather_regex(&mut regex_cache, val.clone())?;
                            reg.is_match(&s)
                        },
                        _ => {
                            return Err(gerr("Invalid row type"))
                        }
                    }
                },
            };
            result = check;
            if let Some(logical_gate) = logicgate{
                match logical_gate {
                    LogicalGate::And => if !check{return Ok(false)},
                    LogicalGate::Or => if check{return Ok(true)} 
                }
            }
        }

        Ok(result)
    }

}