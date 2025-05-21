use std::fs::File;
use tokio::sync::mpsc;
use ahash::HashMap;
use tokio::sync::RwLock;

use crate::lexer_functions::AlbaTypes;


type row = Vec<AlbaTypes>;
pub struct SearchRunner{
    file : RwLock<File>,
    gathered : RwLock<HashMap<usize,row>>
}