use std::{
    ffi::CString,
    io::Error,
    os::raw::{c_char, c_int},
    ptr::null_mut,
};

use crate::gerr;

#[repr(C)]
pub struct Hashmap {
    pub file: c_int,
    pub bucket_size: u64,
    pub len: u64,
    pub path: *mut c_char,
    pub temp_path: *mut c_char,
    cache: *mut BurningMap,
}

#[repr(C)]
struct Paper {
    health: u8,
    key: u64,
    value: u64,
}

#[repr(C)]
struct BurningMap {
    capacity: u64,
    paper_vector: *mut Paper,
}

#[repr(C)]
pub struct OptionUINT64 {
    pub some: i8,
    pub value: u64,
}

#[repr(C)]
pub struct GetInput {
    pub count: u32,
    pub key: *mut u64,
}

#[repr(C)]
#[derive(Default)]
pub struct GetOutput {
    pub success: i8,
    pub count: u32,
    pub value: *mut OptionUINT64,
}

#[repr(C)]
pub struct WriteInput {
    pub count: u32,
    pub key: *mut u64,
    pub value: *mut u64,
    pub exists: *mut u8,
}

#[link(name = "hashmap", kind = "static")]
unsafe extern "C" {
    fn hashmap_new(hashmap: *mut Hashmap, KiB: u64) -> c_int;
    fn hashmap_get(
        hashmap: *mut Hashmap,
        entry: *mut GetInput,
        foreign_output: *mut GetOutput,
    ) -> c_int;
    fn hashmap_write(hashmap: *mut Hashmap, entry: *mut WriteInput) -> c_int;
    fn hashmap_destroy(hashmap: *mut Hashmap) -> c_int;
}

fn error_execution_product(i: i32) -> Error {
    match i {
        0 => gerr("Hashmap Error(0): Unexpected success code in error handler"),
        -1 => gerr("Hashmap Error(-1): Something went wrong"),
        -2 => gerr("Hashmap Error(-2): Failed to allocate memory"),
        -3 => gerr("Hashmap Error(-3): Disk write failure"),
        -4 => gerr("Hashmap Error(-4): Disk read failure"),
        -5 => gerr("Hashmap Error(-5): Failed to open file"),
        -6 => gerr("Hashmap Error(-6): IoUring queue start failure"),
        -7 => gerr("Hashmap Error(-7): IoUring SQE failure"),
        -8 => gerr("Hashmap Error(-8): IoUring CQE file write failure"),
        -10 => gerr("Hashmap Error(-10): IoUring CQE file read failure"),
        -11 => gerr("Hashmap Error(-11): Fsync failure"),
        -12 => gerr("Hashmap Error(-12): Out of disk space"),
        -13 => gerr("Hashmap Error(-13): IoUring submit failure"),
        _ => gerr(&format!("Hashmap Error({}): Unknown error code", i)),
    }
}

pub struct IndexingHashmap {
    inner: Hashmap,
}

impl Drop for IndexingHashmap {
    fn drop(&mut self) {
        unsafe { hashmap_destroy(&mut self.inner as *mut Hashmap) };
    }
}

unsafe impl Send for IndexingHashmap {}
unsafe impl Sync for IndexingHashmap {}
impl IndexingHashmap {
    pub fn new(path: String, kib: u64) -> Result<Self, Error> {
        let p = format!("{}.hashmap", path);
        let ptemp = format!("{}.temp", path);

        let c_p = match CString::new(p.clone()) {
            Ok(a) => a,
            Err(e) => return Err(gerr(&e.to_string())),
        };

        let c_tp = match CString::new(ptemp) {
            Ok(a) => a,
            Err(e) => return Err(gerr(&e.to_string())),
        };

        let mut h: Hashmap = Hashmap {
            path: c_p.into_raw(),
            temp_path: c_tp.into_raw(),
            file: -1,
            len: 0,
            bucket_size: 0,
            cache: null_mut(),
        };
        let execution_product = unsafe { hashmap_new((&mut h) as *mut Hashmap, kib) };
        match execution_product {
            x if x >= 0 => Ok(IndexingHashmap { inner: h }),
            _ => {
                eprintln!("Failed to create hashmap-file");
                Err(error_execution_product(execution_product))
            }
        }
    }
    pub fn get(&mut self, keys: Vec<u64>) -> Result<Vec<u64>, Error> {
        let mut keys = keys;
        let mut input = GetInput {
            count: keys.len() as u32,
            key: keys.as_mut_ptr(),
        };
        let mut output = GetOutput::default();

        let execution_product = unsafe {
            hashmap_get(
                &mut self.inner as *mut Hashmap,
                &mut input as *mut GetInput,
                &mut output as *mut GetOutput,
            )
        };

        if execution_product < 0 {
            eprintln!("Failed to run hashmap-get");
            return Err(error_execution_product(execution_product));
        }
        let v = output.value;
        Ok(unsafe {
            let a = Vec::from_raw_parts(v, output.count as usize, keys.len());
            let mut b = Vec::new();
            for i in a {
                if i.some > 0 {
                    b.push(i.value);
                }
            }
            b
        })
    }
    pub fn write(&mut self, entries: Vec<(bool, u64, u64)>) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<u64> = Vec::with_capacity(entries.len());
        let mut values: Vec<u64> = Vec::with_capacity(entries.len());
        let mut exists: Vec<u8> = Vec::with_capacity(entries.len());

        for (exist, key, value) in entries {
            exists.push(if exist { 1 } else { 0 });
            keys.push(key);
            values.push(value);
        }

        let mut input = WriteInput {
            count: keys.len() as u32,
            key: keys.as_mut_ptr(),
            value: values.as_mut_ptr(),
            exists: exists.as_mut_ptr(),
        };

        let execution_product = unsafe {
            hashmap_write(
                &mut self.inner as *mut Hashmap,
                &mut input as *mut WriteInput,
            )
        };

        if execution_product < 0 {
            eprintln!("Failed to run hashmap-write");
            return Err(error_execution_product(execution_product));
        }
        Ok(())
    }
}
