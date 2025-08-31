use crate::alba_types::AlbaTypes;
use crate::container::MvccState;
use std::sync::Mutex;
use std::{fs::File, io::Error, os::unix::fs::FileExt, sync::Arc, vec};

use crate::container::MAX_GRAVEYARD_LENGTH_IN_MEMORY;
use crate::{
    Token,
    container::Container,
    query_conditions::{QueryConditions, QueryIndexType, QueryType},
    row::Row,
};
use serde::{Deserialize, Serialize};

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
    pub file: Arc<Mutex<File>>,
    pub conditions: QueryConditions,
}
const CHUNK_SIZE_BYTES: usize = 4096 * 12;

pub fn search(
    container: Arc<Mutex<Container>>,
    args: SearchArguments,
) -> Result<(Vec<Row>, Vec<u64>), Error> {
    let file = args.file.lock().unwrap();
    let lck = container.lock().unwrap();
    let size = file.metadata().unwrap().len() as usize;
    if size == args.header_offset {
        return Ok((Vec::new(), Vec::new()));
    }
    let empty = vec![255u8; args.element_size];
    let column_names = &lck.column_names();
    let qt = args.conditions.query_type().unwrap();
    let mut gy = lck.graveyard.lock().unwrap();
    let mut ds_cache = lck.ds_cache.lock().unwrap();
    if let QueryType::Indexed(QueryIndexType::Strict(u)) = qt {
        let mut res = (Vec::new(), Vec::new());
        let v = lck.index_map.lock().unwrap().get(u)?;

        for offset in v {
            if gy.contains(&offset) {
                continue;
            }
            let mut buff = vec![0u8; args.element_size];
            if !ds_cache.get(offset, &mut buff) {
                file.read_exact_at(&mut buff, offset)?;
            }
            if buff == empty || gy.contains(&offset) {
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

    let total_rows = (file.metadata()?.len() as usize - args.header_offset) / args.element_size;
    let rows_per_it = (CHUNK_SIZE_BYTES / args.element_size).max(1);
    let chunk_size = (rows_per_it * args.element_size).min(total_rows * args.element_size);
    let count_its = (total_rows / rows_per_it).max(1);
    let remainder = total_rows % rows_per_it;

    let mut space_gy = gy.len();
    let mut rows = Vec::new();
    let mut offsets = Vec::new();
    for i in 0..count_its {
        let mut buffer = vec![0u8; chunk_size];
        let file_offset = args.header_offset as u64 + (i * chunk_size) as u64;

        file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file = args.header_offset + i * chunk_size + j * args.element_size;

            if gy.get(&(offset_in_file as u64)).is_some() {
                continue;
            }
            if row_bin == empty {
                if space_gy < MAX_GRAVEYARD_LENGTH_IN_MEMORY {
                    space_gy += 1;
                    gy.insert(offset_in_file as u64);
                }
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

        file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file =
                args.header_offset + count_its * chunk_size + j * args.element_size;

            if gy.get(&(offset_in_file as u64)).is_some() {
                continue;
            }
            if row_bin == empty {
                if space_gy < MAX_GRAVEYARD_LENGTH_IN_MEMORY {
                    space_gy += 1;
                    gy.insert(offset_in_file as u64);
                }
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
    println!(
        "Starting search_with_action: element_size={}, header_offset={}, action_type={:?}",
        args.element_size, args.header_offset, action_type
    );
    let file = args.file.lock().unwrap();
    println!("Acquired file lock");
    let lck = container.lock().unwrap();
    println!("Acquired container lock");
    let mut ds_cache = lck.ds_cache.lock().unwrap();
    println!("Acquired ds_cache lock");
    let size = file.metadata().unwrap().len() as usize;
    println!("File size: {}", size);
    if size == args.header_offset {
        println!("File size equals header_offset, returning");
        return Ok(());
    }

    let empty = vec![255u8; args.element_size];
    let column_names = &lck.column_names();
    println!("Column names: {:?}", column_names);
    let qt = args.conditions.query_type()?;
    println!("Query type: {:?}", qt);
    let mut gy = lck.graveyard.lock().unwrap();
    println!("Acquired graveyard lock, size: {}", gy.len());

    let mut indexes = Vec::new();
    if let ActionType::Edit((col_nam, col_val)) = action_type.clone() {
        println!(
            "Processing edit action: col_names={:?}, col_values={:?}",
            col_nam, col_val
        );
        for i in col_nam.iter().enumerate() {
            for j in lck.headers.iter().enumerate() {
                if *j.1.0 == *i.1 {
                    println!("Matched column {} at index {}", i.1, j.0);
                    indexes.push((j.0, col_val[i.0].clone()));
                }
            }
        }
        println!("Edit indexes: {:?}", indexes);
    }

    let mut mvcc = lck.mvcc.lock().unwrap();
    println!("Acquired mvcc lock");

    if let QueryType::Indexed(QueryIndexType::Strict(u)) = qt {
        println!("Using indexed query with indices: {:?}", u);
        let mut res = (Vec::new(), Vec::new());
        let mut vi = lck.index_map.lock().unwrap();
        println!("Index map locked");
        let v = vi.get(u)?;
        println!("Index map retrieved: {} offsets", v.len());
        for offset in v {
            println!("Processing offset: {}", offset);
            if gy.contains(&offset) {
                println!("Offset {} in graveyard, skipping", offset);
                continue;
            }
            let mut buff = vec![0u8; args.element_size];
            if !ds_cache.get(offset, &mut buff) {
                println!("Cache miss for offset {}, reading from file", offset);
                file.read_exact_at(&mut buff, offset)?;
            }
            if buff == empty || gy.contains(&offset) {
                println!("Offset {} is empty or in graveyard, skipping", offset);
                continue;
            }
            println!("Deserializing row at offset {}", offset);
            let mut b = Row {
                data: lck.deserialize_row(&buff)?,
            };
            println!("Row deserialized: {:?}", b);
            if args.conditions.row_match(&b, column_names)? {
                println!("Row at offset {} matches conditions", offset);
                res.0.push(b.clone());
                res.1.push(offset);
                match action_type {
                    ActionType::Edit((_, _)) => {
                        println!("Applying edit to row at offset {}", offset);
                        for j in indexes.iter() {
                            println!("Updating column index {} with value {:?}", j.0, j.1);
                            b.data[j.0] = j.1.clone();
                        }
                        println!("Inserting edit into MVCC at offset {}", offset);
                        mvcc.0
                            .insert(offset as u64, (MvccState::Edit, b.data.clone()));
                    }
                    ActionType::Delete => {
                        println!("Inserting delete into MVCC at offset {}", offset);
                        mvcc.0
                            .insert(offset as u64, (MvccState::Delete, b.data.clone()));
                    }
                }
            } else {
                println!("Row at offset {} does not match conditions", offset);
            }
        }
        println!("Indexed query complete, processed {} rows", res.0.len());
        return Ok(());
    }

    let total_rows = (file.metadata()?.len() as usize - args.header_offset) / args.element_size;
    let rows_per_it = (CHUNK_SIZE_BYTES / args.element_size).max(1);
    let chunk_size = (rows_per_it * args.element_size).min(total_rows * args.element_size);
    let count_its = (total_rows / rows_per_it).max(1);
    let remainder = total_rows % rows_per_it;
    println!(
        "Scan query: total_rows={}, rows_per_it={}, chunk_size={}, count_its={}, remainder={}",
        total_rows, rows_per_it, chunk_size, count_its, remainder
    );
    let mut space_gy = gy.len();
    for i in 0..count_its {
        println!("Processing chunk {}", i);
        let mut buffer = vec![0u8; chunk_size];
        let file_offset = args.header_offset as u64 + (i * chunk_size) as u64;
        println!("Reading chunk at file_offset {}", file_offset);
        file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file = args.header_offset + i * chunk_size + j * args.element_size;
            println!("Processing row at offset_in_file {}", offset_in_file);
            if gy.get(&(offset_in_file as u64)).is_some() {
                println!("Offset {} in graveyard, skipping", offset_in_file);
                continue;
            }
            if row_bin == empty {
                println!("Row at offset {} is empty", offset_in_file);
                if space_gy < MAX_GRAVEYARD_LENGTH_IN_MEMORY {
                    space_gy += 1;
                    gy.insert(offset_in_file as u64);
                    println!(
                        "Added offset {} to graveyard, new size: {}",
                        offset_in_file, space_gy
                    );
                }
                continue;
            }
            println!("Deserializing row at offset {}", offset_in_file);
            let bare_row = lck.deserialize_row(row_bin)?;
            let mut row = Row { data: bare_row };
            println!("Row deserialized: {:?}", row);
            if args.conditions.row_match(&row, &column_names)? {
                println!("Row at offset {} matches conditions", offset_in_file);
                match action_type {
                    ActionType::Edit((_, _)) => {
                        println!("Applying edit to row at offset {}", offset_in_file);
                        for j in indexes.iter() {
                            println!("Updating column index {} with value {:?}", j.0, j.1);
                            row.data[j.0] = j.1.clone();
                        }
                        println!("Inserting edit into MVCC at offset {}", offset_in_file);
                        mvcc.0
                            .insert(offset_in_file as u64, (MvccState::Edit, row.data.clone()));
                    }
                    ActionType::Delete => {
                        println!("Inserting delete into MVCC at offset {}", offset_in_file);
                        mvcc.0.insert(
                            offset_in_file as u64,
                            (MvccState::Delete, vec![row.data[0].clone()]),
                        );
                    }
                }
            } else {
                println!("Row at offset {} does not match conditions", offset_in_file);
            }
        }
    }

    if remainder > 0 {
        println!("Processing remainder: {} rows", remainder);
        let mut buffer = vec![0u8; args.element_size * remainder];
        let file_offset = args.header_offset as u64 + (count_its * chunk_size) as u64;
        println!("Reading remainder at file_offset {}", file_offset);
        file.read_exact_at(&mut buffer, file_offset).unwrap();
        for (j, row_bin) in buffer.chunks_exact(args.element_size).enumerate() {
            let offset_in_file =
                args.header_offset + count_its * chunk_size + j * args.element_size;
            println!(
                "Processing remainder row at offset_in_file {}",
                offset_in_file
            );
            if gy.get(&(offset_in_file as u64)).is_some() {
                println!("Offset {} in graveyard, skipping", offset_in_file);
                continue;
            }
            if row_bin == empty {
                println!("Remainder row at offset {} is empty", offset_in_file);
                if space_gy < MAX_GRAVEYARD_LENGTH_IN_MEMORY {
                    space_gy += 1;
                    gy.insert(offset_in_file as u64);
                    println!(
                        "Added remainder offset {} to graveyard, new size: {}",
                        offset_in_file, space_gy
                    );
                }
                continue;
            }
            println!("Deserializing remainder row at offset {}", offset_in_file);
            let bare_row = lck.deserialize_row(row_bin)?;
            let mut row = Row { data: bare_row };
            println!("Remainder row deserialized: {:?}", row);
            if args.conditions.row_match(&row, &column_names)? {
                println!(
                    "Remainder row at offset {} matches conditions",
                    offset_in_file
                );
                match action_type {
                    ActionType::Edit((_, _)) => {
                        println!("Applying edit to row at offset {}", offset_in_file);
                        for j in indexes.iter() {
                            println!("Updating column index {} with value {:?}", j.0, j.1);
                            row.data[j.0] = j.1.clone();
                        }
                        println!("Inserting edit into MVCC at offset {}", offset_in_file);
                        mvcc.0
                            .insert(offset_in_file as u64, (MvccState::Edit, row.data.clone()));
                    }
                    ActionType::Delete => {
                        println!("Inserting delete into MVCC at offset {}", offset_in_file);
                        mvcc.0.insert(
                            offset_in_file as u64,
                            (MvccState::Delete, vec![row.data[0].clone()]),
                        );
                    }
                }
            } else {
                println!(
                    "Remainder row at offset {} does not match conditions",
                    offset_in_file
                );
            }
        }
    }

    println!("search_with_action complete, MVCC size: {}", mvcc.0.len());
    Ok(())
}
