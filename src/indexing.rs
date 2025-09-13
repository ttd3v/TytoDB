use std::{
    ffi::CString,
    io::Error,
    os::raw::{c_char, c_int, c_uchar},
    ptr::null_mut,
};
use libc;
use crate::gerr;

#[repr(C)]
pub struct Hashmap {
    pub file: c_int,
    pub array_len: *mut c_uchar,
    pub bucket_size: u64,
    pub len: u64,
    pub path: *mut c_char,
    pub temp: *mut c_char,
}

#[derive(Clone,Copy,Debug)]
#[repr(C)]
pub struct Cell {
    pub hk: u64,
    pub v: u64,
}

#[derive(Clone,Copy,Debug)]
#[repr(C)]
pub struct SomeU64 {
    pub exists: u8,
    pub value: u64,
}

#[link(name = "hashmap", kind = "static")]
unsafe extern "C" {
    fn hm_new(hashmap: *mut Hashmap, path: *const c_char) -> c_int;
    fn hm_get(
        hashmap: *mut Hashmap,
        inputs: *const u64,
        outputs: *mut SomeU64,
        length: u64,
    ) -> c_int;
    fn hm_write(hashmap: *mut Hashmap, inputs: *const Cell, length: u64) -> c_int;
}

fn error_execution_product(i: i32) -> Error {
    match i {
        0 => gerr("Hashmap Error(0): Unexpected success code in error handler"),
        -1 => gerr("Hashmap Error(-1): General error"),
        -2 => gerr("Hashmap Error(-2): Memory allocation failed"),
        -3 => gerr("Hashmap Error(-3): No disk space"),
        -4 => gerr("Hashmap Error(-4): io_uring queue failed"),
        -5 => gerr("Hashmap Error(-5): File open failed"),
        -6 => gerr("Hashmap Error(-6): File create failed"),
        -7 => gerr("Hashmap Error(-7): File read failed"),
        -8 => gerr("Hashmap Error(-8): File write failed"),
        -9 => gerr("Hashmap Error(-9): File sync failed"),
        -10 => gerr("Hashmap Error(-10): Vector allocation failed"),
        -11 => gerr("Hashmap Error(-11): iovec allocation failed"),
        -12 => gerr("Hashmap Error(-12): Fetch hashset allocation failed"),
        -13 => gerr("Hashmap Error(-13): Fetch to read allocation failed"),
        -14 => gerr("Hashmap Error(-14): Fetch cell buffer allocation failed"),
        -15 => gerr("Hashmap Error(-15): Fetch buffer hashmap failed"),
        -16 => gerr("Hashmap Error(-16): Fetch vector cells failed"),
        -17 => gerr("Hashmap Error(-17): Fetch allocation failed"),
        -18 => gerr("Hashmap Error(-18): Rebucket buffer allocation failed"),
        -19 => gerr("Hashmap Error(-19): Rebucket cells allocation failed"),
        -20 => gerr("Hashmap Error(-20): Write slot full"),
        _ => gerr(&format!("Hashmap Error({}): Unknown error code", i)),
    }
}

pub struct IndexingHashmap {
    inner: Hashmap,
}

impl Drop for IndexingHashmap {
    fn drop(&mut self) {
        unsafe {
            if !self.inner.array_len.is_null() {
                libc::free(self.inner.array_len as *mut libc::c_void);
            }
            if self.inner.file != -1 {
                libc::close(self.inner.file);
            }
            if !self.inner.path.is_null() {
                libc::free(self.inner.path as *mut libc::c_void);
            }
            if !self.inner.temp.is_null() {
                libc::free(self.inner.temp as *mut libc::c_void);
            }
        }
    }
}

unsafe impl Send for IndexingHashmap {}
unsafe impl Sync for IndexingHashmap {}

impl IndexingHashmap {
    pub fn new(path: String, _kib: u64) -> Result<Self, Error> {
        let p = format!("{}.hashmap", path);
        let ptemp = format!("{}.temp", path);

        let c_p = CString::new(p).map_err(|e| gerr(&e.to_string()))?;
        let c_tp = CString::new(ptemp).map_err(|e| gerr(&e.to_string()))?;

        let path_ptr = c_p.as_ptr();
        let mut h = Hashmap {
            file: -1,
            array_len: null_mut(),
            bucket_size: 0,
            len: 0,
            path: c_p.into_raw(),
            temp: c_tp.into_raw(),
        };
        let execution_product = unsafe { hm_new(&mut h, path_ptr) };
        match execution_product {
            x if x >= 0 => Ok(IndexingHashmap { inner: h }),
            _ => Err(error_execution_product(execution_product)),
        }
    }

    pub fn get(&mut self, keys: Vec<u64>) -> Result<Vec<u64>, Error> {
        let length = keys.len() as u64;
        if length == 0 {
            return Ok(vec![]);
        }
        let mut outputs: Vec<SomeU64> = vec![SomeU64 { exists: 0, value: 0 }; keys.len()];
        let inputs_ptr = keys.as_ptr();
        let outputs_ptr = outputs.as_mut_ptr();
        let execution_product = unsafe {
            hm_get(
                &mut self.inner,
                inputs_ptr,
                outputs_ptr,
                length,
            )
        };
        if execution_product < 0 {
            return Err(error_execution_product(execution_product));
        }
        let out_slice = unsafe { std::slice::from_raw_parts(outputs_ptr, keys.len()) };
        let mut b = Vec::new();
        for i in out_slice {
            if i.exists != 0 {
                b.push(i.value);
            }
        }
        Ok(b)
    }

    pub fn write(&mut self, entries: Vec<(bool, u64, u64)>) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let length = entries.len() as u64;
        let mut cells: Vec<Cell> = Vec::with_capacity(entries.len());
        for (exists, key, value) in entries {
            cells.push(Cell { hk: key, v: if exists{value}else{u64::MAX} });
        }
        let cells_ptr = cells.as_ptr();
        let execution_product = unsafe { hm_write(&mut self.inner, cells_ptr, length) };
        if execution_product < 0 {
            return Err(error_execution_product(execution_product));
        }
        Ok(())
    }
}