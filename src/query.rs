use std::io::{self, Error, ErrorKind};

use ahash::{AHashMap, HashMap};
use serde::{Deserialize, Serialize};

use crate::{database::{generate_secure_code, Database}, gerr, lexer_functions::{AlbaTypes, Token}};


const PAGE_SIZE: usize = 100;

type QueryPage = (Vec<u64>, String);
pub type PrimitiveQueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>, Vec<Vec<AlbaTypes>>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub rows: Rows,
    pub pages: Vec<QueryPage>,
    pub current_page: usize,
    pub column_names: Vec<String>,
    pub column_types: Vec<AlbaTypes>,
    pub id: String,
}

impl Query {
    pub fn duplicate(&self) -> Self {
        
        Query {
            rows: self.rows.clone(),
            pages: self.pages.clone(),
            current_page: self.current_page, 
            column_names: self.column_names.clone(),
            column_types: self.column_types.clone(),
            id: self.id.clone(),
        }
    }

    pub fn trim(&mut self) {
        
        self.column_types = self
            .column_types
            .iter()
            .filter(|p| !matches!(p, AlbaTypes::NONE))
            .cloned()
            .collect();
        self.column_names = self
            .column_names
            .iter()
            .filter(|p| !p.is_empty())
            .cloned()
            .collect();
        
    }

    pub fn new(column_types: Vec<AlbaTypes>) -> Self {
        
        let mut n = Query {
            rows: (Vec::new(), Vec::new()), 
            pages: Vec::new(), 
            current_page: 0, 
            column_names: Vec::new(), 
            column_types,
            id: generate_secure_code(100),
        };
        n.trim();
        
        n
    }

    pub fn new_none(column_types: Vec<AlbaTypes>) -> Self {
        
        let mut a = Query {
            rows: (Vec::new(), Vec::new()), 
            pages: Vec::new(), 
            current_page: 0, 
            column_names: Vec::new(), 
            column_types,
            id: "".to_string(),
        };
        a.trim();
        
        a
    }

    pub fn join(&mut self, foreign: Query) {
        if foreign.column_types != self.column_types {
            return;
        }
        
        for (idx, (foreign_ids, container_name)) in foreign.pages.into_iter().enumerate() {
            if idx < self.pages.len() {
                let (self_ids, _) = &mut self.pages[idx];
                
                for foreign_id in foreign_ids {
                    if self_ids.len() < PAGE_SIZE {
                        self_ids.push(foreign_id);
                    } else {
                        break;
                    }
                }
            } else {
                self.pages.push((foreign_ids, container_name));
            }
        }
        
        self.trim();
    }

    pub async fn load_rows(&mut self, database: &mut Database) -> Result<(), Error> {
        
        
        if self.pages.is_empty() {
            
            return Ok(());
        }
        
        
        let page = match self.pages.get(self.current_page) {
            Some(a) => {
                
                a
            },
            None => {
                
                return Err(gerr("There is no page"))
            }
        };
        
        let container_name = &page.1;
        
        let container = match database.container.get(container_name) {
            Some(a) => {
                
                a
            },
            None => {
                
                return Err(gerr(&format!("There is no container in the given database named {}", container_name)))
            }
        };
        
        let mut rows = Vec::new();
        
        for i in page.0.iter() {
            
            let indexes = (*i, *i + 1);
            println!("load_rows: {:?}",indexes);
            match container.get_rows(indexes).await?.get(0) {
                Some(a) => {
                    rows.push(a.clone());
                },
                None => {
                    
                    continue;
                }
            }
        }
        
        
        self.rows = (container.column_names(), rows);
        self.trim();
        Ok(())
    }

    pub async fn next(&mut self, database: &mut Database) -> Result<(), Error> {
        
        
        if self.pages.is_empty() {
            
            return Ok(());
        }
        
        
        if self.current_page + 1 >= self.pages.len() {
            
            return Ok(());
        }
        
        self.current_page += 1;
        
        
        
        self.load_rows(database).await?;
        self.trim();
        
        
        Ok(())
    }

    pub async fn previous(&mut self, database: &mut Database) -> Result<(), Error> {
        
        
        if self.pages.is_empty() {
            
            return Ok(());
        }
        
        
        if self.current_page == 0 {
            
            return Ok(());
        }
        
        self.current_page -= 1;
        
        
        
        self.load_rows(database).await?;
        self.trim();
        
        
        Ok(())
    }

    pub fn push(&mut self, subject: (Vec<u64>, String)) {
        
        self.pages.push(subject);
        
    }
}

