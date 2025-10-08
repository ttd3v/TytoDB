use crate::alba_types::AlbaTypes;
use crate::container::MvccState;
use crate::indexing::{Request, RequestMethods};
use crate::{
    Token,
    container::Container,
    query_conditions::{QueryConditions, QueryIndexType, QueryType},
    row::Row,
};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::{io::Error, os::unix::fs::FileExt, sync::Arc, vec};

pub type PrimitiveQueryConditions = (Vec<(Token, Token, Token)>, Vec<(usize, char)>);

type Rows = (Vec<String>, Vec<Row>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Query {
    pub rows: Rows,
}

#[derive(Clone, Debug)]
pub struct SearchArguments {
    pub element_size: usize,
    pub header_offset: usize,
    pub conditions: QueryConditions,
}
const CHUNK_SIZE_BYTES: usize = 4096 * 12;

pub fn search(
    container: Arc<Mutex<Container>>,
    args: SearchArguments,
) -> Result<(Vec<Row>, Vec<u64>), Error> {
    let mut lck = container.lock().unwrap();
    let size = lck.file.metadata().unwrap().len() as usize;
    if size == args.header_offset {
        return Ok((Vec::new(), Vec::new()));
    }
    let empty = vec![255u8; args.element_size];
    let column_names = &lck.column_names();
    let qt = args.conditions.query_type().unwrap();
    if let QueryType::Indexed(QueryIndexType::Strict(u)) = qt {
        let mut res = (Vec::new(), Vec::new());
        let mut req = u
            .iter()
            .map(|i| Request {
                method: RequestMethods::Read,
                key: i.clone(),
                value: u64::MAX,
            })
            .collect();
        lck.index_map.request(&mut req)?;

        let mut g = Vec::with_capacity(req.len());
        for i in req {
            //println!("--- {:?}", i);
            if i.value != u64::MAX {
                g.push(i.value);
            }
        }
        for offset in g {
            let mut buff = vec![0u8; args.element_size];
            if !lck.ds_cache.get(offset, &mut buff) {
                lck.file.read_exact_at(&mut buff, offset)?;
            }
            //println!("{}", offset);

            if buff == empty {
                continue;
            }

            let b = Row {
                data: lck.deserialize_row(&buff)?,
            };

            if args.conditions.row_match(&b, column_names)? {
                res.0.push(b);
                res.1.push(offset);
            }
        }

        return Ok(res);
    }

    let total_rows = (lck.file.metadata()?.len() as usize - args.header_offset) / args.element_size;
    let rows_per_it = (CHUNK_SIZE_BYTES / args.element_size).max(1);
    let chunk_size = (rows_per_it * args.element_size).min(total_rows * args.element_size);
    let count_its = (total_rows / rows_per_it).max(1);
    let remainder = total_rows % rows_per_it;
    let mut rows = Vec::new();
    let mut offsets = Vec::new();
    for i in 0..count_its {
        let mut buffer = vec![0u8; chunk_size];
        let file_offset = args.header_offset as u64 + (i * chunk_size) as u64;

        lck.file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file = args.header_offset + i * chunk_size + j * args.element_size;
            if row_bin == empty {
                continue;
            }

            let bare_row = lck.deserialize_row(row_bin)?;
            let row = Row { data: bare_row };

            if args.conditions.row_match(&row, &column_names)? {
                offsets.push(offset_in_file as u64);
                rows.push(row);
            }
        }
    }
    if remainder > 0 {
        let mut buffer = vec![0u8; args.element_size * remainder];
        let file_offset = args.header_offset as u64 + (count_its * chunk_size) as u64;

        lck.file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file =
                args.header_offset + count_its * chunk_size + j * args.element_size;
            if row_bin == empty {
                continue;
            }

            let bare_row = lck.deserialize_row(row_bin)?;
            let row = Row { data: bare_row };

            if args.conditions.row_match(&row, &column_names)? {
                offsets.push(offset_in_file as u64);
                rows.push(row);
            }
        }
    }

    Ok((rows, offsets))
}

type ActionTypeEdit = (Vec<String>, Vec<AlbaTypes>);
#[derive(Clone, Debug)]
pub enum ActionType {
    Edit(ActionTypeEdit),
    Delete,
}

pub fn search_with_action(
    container: Arc<Mutex<Container>>,
    args: SearchArguments,
    action_type: ActionType,
) -> Result<(), Error> {
    let mut lck = container.lock().unwrap();
    let size = lck.file.metadata().unwrap().len() as usize;

    if size == args.header_offset {
        return Ok(());
    }

    let empty = vec![255u8; args.element_size];
    let column_names = &lck.column_names();

    let qt = args.conditions.query_type()?;

    let mut indexes = Vec::new();
    if let ActionType::Edit((col_nam, col_val)) = action_type.clone() {
        for i in col_nam.iter().enumerate() {
            for j in lck.headers.iter().enumerate() {
                if *j.1.0 == *i.1 {
                    indexes.push((j.0, col_val[i.0].clone()));
                }
            }
        }
    }

    if let QueryType::Indexed(QueryIndexType::Strict(u)) = qt {
        let mut res = (Vec::new(), Vec::new());

        let mut req = u
            .iter()
            .map(|i| Request {
                method: RequestMethods::Read,
                key: i.clone(),
                value: u64::MAX,
            })
            .collect();
        lck.index_map.request(&mut req)?;

        let mut g = Vec::with_capacity(req.len());
        for i in req {
            if i.value != u64::MAX {
                g.push(i.value);
            }
        }

        for offset in g {
            let mut buff = vec![0u8; args.element_size];
            if !lck.ds_cache.get(offset, &mut buff) {
                lck.file.read_exact_at(&mut buff, offset)?;
            }
            if buff == empty {
                continue;
            }

            let b = Row {
                data: lck.deserialize_row(&buff)?,
            };
            //println!("b{:?}", b.data);

            if args.conditions.row_match(&b, column_names)? {
                res.0.push(b.clone());
                res.1.push(offset);
                match action_type {
                    ActionType::Edit((_, _)) => {
                        let xxx = b.data.clone();
                        let mut yyy = xxx.clone();
                        for j in indexes.iter() {
                            yyy[j.0] = j.1.clone();
                        }
                        //println!("be:{:?}", b.data);
                        if xxx[0] != yyy[0] {
                            lck.mvcc.0.push((offset as u64, (MvccState::Delete, xxx)));
                            lck.push_row(yyy)?;
                        } else {
                            lck.mvcc.0.push((offset as u64, (MvccState::Edit, yyy)));
                        }
                        //lck.push_row(yyy)?;
                    }
                    ActionType::Delete => {
                        lck.mvcc
                            .0
                            .push((offset as u64, (MvccState::Delete, b.data.clone())));
                    }
                }
            }
        }

        return Ok(());
    }

    let total_rows = (lck.file.metadata()?.len() as usize - args.header_offset) / args.element_size;
    let rows_per_it = (CHUNK_SIZE_BYTES / args.element_size).max(1);
    let chunk_size = (rows_per_it * args.element_size).min(total_rows * args.element_size);
    let count_its = (total_rows / rows_per_it).max(1);
    let remainder = total_rows % rows_per_it;
    for i in 0..count_its {
        let mut buffer = vec![0u8; chunk_size];
        let file_offset = args.header_offset as u64 + (i * chunk_size) as u64;

        lck.file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file = args.header_offset + i * chunk_size + j * args.element_size;
            if row_bin == empty {
                continue;
            }

            let bare_row = lck.deserialize_row(row_bin)?;
            let row = Row { data: bare_row };

            //println!("b1{:?}", row.data);
            if args.conditions.row_match(&row, &column_names)? {
                match action_type {
                    ActionType::Edit((_, _)) => {}
                    ActionType::Delete => {
                        lck.mvcc.0.push((
                            offset_in_file as u64,
                            (MvccState::Delete, vec![row.data[0].clone()]),
                        ));
                    }
                }
            } else {
            }
        }
    }

    if remainder > 0 {
        let mut buffer = vec![0u8; args.element_size * remainder];
        let file_offset = args.header_offset as u64 + (count_its * chunk_size) as u64;

        lck.file.read_exact_at(&mut buffer, file_offset)?;
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file =
                args.header_offset + count_its * chunk_size + j * args.element_size;
            if row_bin == empty {
                continue;
            }

            let bare_row = lck.deserialize_row(row_bin)?;
            let row = Row { data: bare_row };

            if args.conditions.row_match(&row, &column_names)? {
                match action_type {
                    ActionType::Edit((_, _)) => {
                        let xxx = row.data.clone();
                        let mut yyy = xxx.clone();
                        for j in indexes.iter() {
                            yyy[j.0] = j.1.clone();
                        }
                        //println!("be:{:?}", b.data);
                        if xxx[0] != yyy[0] {
                            lck.mvcc
                                .0
                                .push((offset_in_file as u64, (MvccState::Delete, xxx)));
                            lck.push_row(yyy)?;
                        } else {
                            lck.mvcc
                                .0
                                .push((offset_in_file as u64, (MvccState::Edit, yyy)));
                        }
                    }
                    ActionType::Delete => {
                        lck.mvcc.0.push((
                            offset_in_file as u64,
                            (MvccState::Delete, vec![row.data[0].clone()]),
                        ));
                    }
                }
            } else {
            }
        }
    }

    Ok(())
}
