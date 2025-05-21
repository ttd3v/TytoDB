use std::sync::Weak;
use std::{collections::HashMap, fs::File, os::unix::fs::FileExt, sync::Arc};
use futures::channel::mpsc::UnboundedSender;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::RwLock;
use std::io::Error;
use crate::{container::Container, gerr, lexer_functions::AlbaTypes, query_conditions::QueryConditions, row::Row};

fn drain_all<T>(receiver: &mut UnboundedReceiver<T>) -> Vec<T> {
    let mut items = Vec::new();
    while let Ok(item) = receiver.try_recv() {
        items.push(item);
    }
    items
}

const CHUNK_MATRIZ : usize = 4096 * 10;
type ContainerName = String;
type SearchRequestTuple = Vec<Row>;

pub struct SearchRequest {
    pub query: QueryConditions,
    pub respond_to: tokio::sync::oneshot::Sender<SearchRequestTuple>,
}

#[derive(Debug)]
pub struct SearchRunner{
    pub file : RwLock<Arc<File>>,
    pub metadata : (usize,usize,HashMap<String,AlbaTypes>),
    pub memo : RwLock<HashMap<usize,Row>>,
    pub running : RwLock<bool>,
    pub window : mpsc::UnboundedReceiver<SearchRequest>,
    pub sender : mpsc::UnboundedSender<SearchRequest>,
    
}

impl SearchRunner{
    pub async fn run(&mut self, container : Arc<&Container>) -> Result<(),Error>{
        if self.window.len() == 0{
            return Ok(())
        }
        let mut running_value = self.running.write().await;
        *running_value = true; drop(running_value);

        let stop = async ||{
            let mut running_value = self.running.write().await;
            *running_value = false; drop(running_value);
        };

        let mut clients_list: Vec<(SearchRequest,Vec<Row>)> = drain_all(&mut self.window).into_iter().map(|f|{(f,Vec::new())}).collect();
        let element_size = self.metadata.0;
        let header_offset = self.metadata.1;

        let file = self.file.read().await;
        let file_size = match file.metadata(){
            Ok(mtd) => mtd.len() as usize,
            Err(e) => {
                stop().await;
                return Err(e)
            }
        };
        let total_rows = (file_size-header_offset)/element_size;
        let mut readen_rows = 0;
        let rows_per_iteration = std::cmp::max(1, CHUNK_MATRIZ / element_size).min(total_rows);

        

        while readen_rows < total_rows{
            let to_read = rows_per_iteration.min(total_rows-readen_rows);
            let read_size = to_read * element_size;
            let mut buffer = vec![0u8;read_size];
            if let Err(e) = file.read_exact_at(&mut buffer, (header_offset + (readen_rows * element_size)) as u64){
                stop().await;
                return Err(e)
            }
            let mut rows : Vec<Row> = Vec::new();
            for i in 0..to_read{
                let buff = &buffer[(i*element_size)..((i+1)*element_size)];
                let row = match container.deserialize_row(buff).await{
                    Ok(row_content) => {
                        let mut data : HashMap<String,AlbaTypes> = HashMap::new();
                        for (index,value) in container.headers.iter().enumerate(){
                            let column_value = match row_content.get(index){
                                Some(a) => {
                                    let cv = a.to_owned();
                                    if std::mem::discriminant(&cv) != std::mem::discriminant(&value.1){
                                        return Err(gerr("Invalid alba type row order, unmatching stuff"))
                                    }
                                    cv
                                },
                                None => {
                                    stop().await;
                                    return Err(gerr("Invalid alba type row order, missing stuff"));
                                }
                            };
                            data.insert(value.0.clone(),column_value);
                        }
                        Row{
                            data,
                            metadata:self.metadata.2.clone()
                        }
                    },
                    Err(e) => {
                        stop().await;
                        return Err(e)
                    }
                };
                rows.push(row);

            }
            for row in rows{
                for client in clients_list.iter_mut(){
                    let is_match = match client.0.query.row_match(&row){
                        Ok(bool) => bool,
                        Err(e) => {
                            stop().await;
                            return Err(e)
                        }
                    };
                    if is_match{
                        client.1.push(row.clone());
                    }
                }
            }
            readen_rows += to_read;
        }

        for client in clients_list{
            let _ = client.0.respond_to.send(client.1);
        }

        return Ok(())
    }
}