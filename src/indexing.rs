use tokio::sync::RwLock;

use crate::{alba_types::AlbaTypes, container::Container, gerr};
use std::{collections::BTreeSet, fs::{self, File}, hash::{DefaultHasher, Hash, Hasher}, io::{Error, Read, Write}, os::unix::fs::{FileExt, MetadataExt}, sync::Arc, time::Duration};

const INDEX_CHUNK_SIZE : u64 = GERAL_DISK_CHUNK as u64;
const GERAL_DISK_CHUNK : usize = 4096;


type IndexElement = (u64,u64); // index value , offset value
type MetadataElement = (u64,u64,u16); // minimum index value, maximum index value , items in chunk


trait Add{
    /// Insert a index value into indexes
    async fn add(&self, arg: u64,arg_offset : u64) -> Result<(),Error>; // direct index value
}
trait Remove{
    /// Remove a index value from indexes
    async fn remove(&self, arg: u64) -> Result<(),Error>;
}
/// Types that can be used as search inputs.
pub trait SearchQuery {}
impl SearchQuery for std::ops::Range<u64> {}
impl SearchQuery for std::ops::RangeInclusive<u64> {}
trait Search <T:SearchQuery>{
    /// Look for offset values from a range of indexes, index or IncludeRange of indexes
    async fn search(&self, arg:T) -> Result<BTreeSet<u64>,Error>;
}

pub struct Indexing{
    indexes_file : Arc<RwLock<File>>,
    indexes_metadata_file : Arc<RwLock<File>>,
    metadata : Arc<RwLock<Vec<(u64,u64,u16)>>>,
    changes : Arc<RwLock<bool>>,
    destroyed : Arc<RwLock<bool>>
}
impl Indexing{
    pub async fn create_index(container : &Container) -> Result<(),Error>{
        let container_name : &String = &container.container_name;

        let ifp = format!("./{}.cindex",container_name);
        let mtp = format!("./{}.cimeta",container_name);
        if fs::exists(&ifp)? || fs::exists(&mtp)?{
            return Ok(())
        }

        File::create_new(ifp)?;
        File::create_new(mtp)?;
        Ok(())
    }
    pub async fn load_index(container : &Container) -> Result<Arc<Self>,Error>{
        Indexing::create_index(container).await?;
        let container_name : &String = &container.container_name;

        let ifp = format!("./{}.cindex",container_name);
        let mtp = format!("./{}.cimeta",container_name);
        if !fs::exists(&ifp)? || !fs::exists(&mtp)?{
            return Err(gerr("One of the indexing files are missing"))
        }

        let indexes_file = File::create_new(&ifp)?;
        let mut metadata_file = File::create_new(&mtp)?;

        let index_metadata  = {
            let size = metadata_file.metadata()?.size() as usize;
            let mut buffer = vec![0u8;size];
            metadata_file.read_to_end(&mut buffer)?;
            let mut elements : Vec<MetadataElement> = Vec::with_capacity(size/18);
            for i in buffer.chunks_exact(18){
                let minimum_index_value = u64::from_be_bytes(i[0..8].try_into().unwrap());
                let maximum_index_value = u64::from_be_bytes(i[8..16].try_into().unwrap());
                let length_of_chunk     = u16::from_be_bytes(i[16..18].try_into().unwrap());
                elements.push((minimum_index_value,maximum_index_value,length_of_chunk));
            }
            elements
        };
        let me = Arc::new(Indexing { indexes_file: Arc::new(RwLock::new(indexes_file)), indexes_metadata_file: Arc::new(RwLock::new(metadata_file)), metadata: Arc::new(RwLock::new(index_metadata)), changes: Arc::new(RwLock::new(false)), destroyed:Arc::new(RwLock::new(false)) });
        let virt_me = me.clone();
        tokio::spawn(async move{
            let me = virt_me;
            loop{
                tokio::time::sleep(Duration::from_secs(10)).await;
                let c = me.changes.read().await;
                if *c{
                    drop(c);
                    let mut file = me.indexes_file.read().await;
                    let _ = file.sync_all();
                    drop(file);
                    file = me.indexes_metadata_file.read().await;
                    let _ = file.sync_all();
                    *me.changes.write().await = false;
                }
                if *me.destroyed.read().await{
                    break;
                }
            }
        });
        Ok(me)
    }
    pub async fn create_index_chunk(&self,arg : u64,arg_offset : u64) -> Result<(),Error>{
        let mut metadata = self.metadata.write().await;
        let index_file = self.indexes_file.write().await;
        metadata.push((arg.clone(),arg+INDEX_CHUNK_SIZE,1));
        
        let metadata_file = self.indexes_metadata_file.write().await;
        let metadata_size = metadata_file.metadata()?.size();
        metadata_file.set_len(metadata_size + 18)?;
        let value = (arg.clone().to_be_bytes(),(arg+INDEX_CHUNK_SIZE as u64).to_be_bytes(),(1 as u16).to_be_bytes());
        let mut buffer = [0u8; 18];
        buffer[0..8].copy_from_slice(&value.0);
        buffer[8..16].copy_from_slice(&value.1);
        buffer[16..18].copy_from_slice(&value.2);
        metadata_file.write_all_at(&mut buffer, metadata_size)?;
        let _ = buffer;
        let _ = metadata_size;
        
        let index_file_size = index_file.metadata()?.size();
        index_file.set_len(index_file_size+INDEX_CHUNK_SIZE * 16)?;
        let mut buffer = [0u8;(INDEX_CHUNK_SIZE*16) as usize];
        let mut ib = [0u8;16];
        ib[..8].copy_from_slice(&arg.to_be_bytes());
        ib[8..].copy_from_slice(&arg_offset.to_be_bytes());
        buffer[..16].copy_from_slice(&ib);
        index_file.write_all_at(&mut buffer,index_file_size)?;
        *self.changes.write().await = true;
        Ok(())
    }
    pub async fn insert_index(&self,arg : u64, arg_offset : u64,meta : (usize, &(u64, u64, u16))) -> Result<(),Error>{
        let index_file = self.indexes_file.write().await;
        let meta_file = self.indexes_file.write().await;
        
        let mut index_buff = [0u8;16];
        let mut meta_buff = [0u8;18];
        index_buff[..8].copy_from_slice(&arg.to_be_bytes());
        index_buff[8..].copy_from_slice(&arg_offset.to_be_bytes());
        meta_buff[..8].copy_from_slice(&meta.1.0.to_be_bytes());
        meta_buff[8..16].copy_from_slice(&meta.1.1.to_be_bytes());
        meta_buff[16..].copy_from_slice(&meta.1.2.to_be_bytes());
        index_file.write_all_at(&index_buff, (((meta.0*INDEX_CHUNK_SIZE as usize) as usize + (meta.1.2) as usize) as usize).try_into().unwrap())?;
        meta_file.write_all_at(&meta_buff, meta.0 as u64*18)?;
        *self.changes.write().await = true;
        Ok(())
    }
}

impl Add for Indexing {
    async fn add(&self, arg: u64,arg_offset : u64) -> Result<(),Error> {
        let metadata = self.metadata.read().await;
        let meta: (usize, &(u64, u64, u16)) = {
            let mut index : (usize,&(u64,u64,u16)) = (0,&(0,0,0));
            let mut alloc = false;
            for (i,v) in metadata.iter().enumerate(){
                if v.2 < u16::MAX && arg <= v.1 && arg >= v.0{
                    index = (i,v);
                    alloc = true;
                    break;
                }
            }
            if alloc{index}else{return self.create_index_chunk(arg, arg_offset).await}
        };
        self.insert_index(arg, arg_offset,meta).await
    }
}



pub trait getIndex{
    fn get_index(&self) -> u64;
}

impl getIndex for i32{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for i64{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for i16{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for i128{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for u128{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for u64{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for u32{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for u16{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for u8{
    fn get_index(&self) -> u64{
        *self as u64/INDEX_CHUNK_SIZE
    }
}
impl getIndex for f64{
    fn get_index(&self) -> u64{
        if self.is_nan(){
            return 0
        }
        (self.abs() as u64) / INDEX_CHUNK_SIZE
    }
}
impl getIndex for bool{
    fn get_index(&self) -> u64{
        if *self{
            return 1
        }
        0
    }
}
impl getIndex for String{
    fn get_index(&self) -> u64{
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()/INDEX_CHUNK_SIZE
    }
}


impl getIndex for AlbaTypes {
    fn get_index(&self) -> u64 {
        match self {
            AlbaTypes::Text(s) => s.get_index(),
            AlbaTypes::Int(i) => i.get_index(),
            AlbaTypes::Bigint(i) => i.get_index(),
            AlbaTypes::Float(f) => f.get_index(),
            AlbaTypes::Bool(b) => b.get_index(),
            AlbaTypes::Char(c) => (*c as u64).get_index(),
            AlbaTypes::NanoString(s) => s.get_index(),
            AlbaTypes::SmallString(s) => s.get_index(),
            AlbaTypes::MediumString(s) => s.get_index(),
            AlbaTypes::BigString(s) => s.get_index(),
            AlbaTypes::LargeString(s) => s.get_index(),
            AlbaTypes::NanoBytes(bytes) => {
                // For Vec<u8>, hash it and then get index
                use std::hash::{Hash, Hasher};
                use std::collections::hash_map::DefaultHasher;

                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::SmallBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::MediumBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::BigSBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::LargeBytes(bytes) => {
                let mut hasher = DefaultHasher::new();
                bytes.hash(&mut hasher);
                let h = hasher.finish();
                h / INDEX_CHUNK_SIZE
            },
            AlbaTypes::NONE => 0,
        }
    }
}
