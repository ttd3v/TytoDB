use std::{
    ffi::CString,
    fs::File,
    io::{self, Error, ErrorKind},
    os::{fd::FromRawFd, raw::c_char},
    ptr::null_mut,
    str::FromStr,
};

#[repr(C)]
pub struct BTree {
    a: i32,
    b: *mut Meta,
    c: u32,
    d: u64,
    path: *mut c_char,
}

#[repr(i32)]
pub enum RequestMethods {
    Read = 1,
    Write = 2,
    Delete = 0,
}
#[repr(C)]
pub struct Request {
    pub method: RequestMethods,
    pub key: u64,
    pub value: u64,
}

#[repr(C)]
pub struct Meta {
    pub max: u64,
    pub min: u64,
    pub len: u64,
}
unsafe extern "C" {
    pub fn init(self_: *mut BTree, path: *mut c_char) -> DBTreeError;
    pub fn bt_request(self_: *mut BTree, req: *mut Request, req_count: usize) -> DBTreeError;
    pub fn normalize(self_: *mut BTree) -> DBTreeError;
}

impl Drop for BTree {
    fn drop(&mut self) {
        unsafe {
            if !self.path.is_null() {
                let _ = CString::from_raw(self.path);
            }
            if !self.b.is_null() && self.c > 0 {
                let _ = Vec::from_raw_parts(self.b, self.c as usize, self.c as usize);
            }
            if self.a >= 0 {
                let _ = File::from_raw_fd(self.a);
                self.a = -1;
            }
        }
    }
}

unsafe impl Send for BTree {}
unsafe impl Sync for BTree {}

impl BTree {
    pub fn new(path: String) -> Result<Self, Error> {
        let mut s = BTree {
            a: 0,
            b: null_mut(),
            c: 0,
            d: 0,
            path: null_mut(),
        };
        let path: *mut c_char = CString::from_str(&path.replace("\0", ""))
            .unwrap()
            .into_raw();
        let res = unsafe { init(&mut s, path) };
        if res == DBTreeError::Success {
            return Ok(s);
        } else {
            return Err(res.tio());
        }
    }
    pub fn request(&mut self, entries: &mut Vec<Request>) -> Result<(), Error> {
        let count = entries.len();
        let r = entries.as_mut_ptr();
        let b = unsafe { bt_request(self, r, count) };
        return if b == DBTreeError::Success {
            Ok(())
        } else {
            Err(b.tio())
        };
    }
    pub fn normalize(&mut self) -> Result<(), Error> {
        let b = unsafe { normalize(self) };
        match b {
            DBTreeError::Success => Ok(()),
            _ => Err(b.tio()),
        }
    }
}

#[allow(dead_code)]
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DBTreeError {
    Success = 0,
    SomethingWentWrong = -1,
    PermissionDenied = -2,
    FileDoesNotExist = -3,
    FileExists = -4,
    ProcessHaveTooManyOpenFiles = -5,
    SystemWideLimitOnOpenFiles = -6,
    TriedOpeningADirectory = -7,
    InvalidMemory = -8,
    InvalidArgument = -9,
    NoSpaceLeft = -10,
    IoError = -11,
    InterruptedBySignal = -12,
    BrokenPipe = -13,
    FailedToAllocateMemory = -14,
    ResourceTemporarilyUnavailable = -15,
    BadFileDescriptor = -16,
    ArgumentListTooLong = -17,
    ExecFormatError = -18,
    NoChildProcesses = -19,
    AddressAlreadyInUse = -20,
    AddressNotAvailable = -21,
    AddressFamilyNotSupported = -22,
    AlreadyInProgress = -23,
    BadMessage = -24,
    InvalidRequestDescriptor = -25,
    InvalidExchange = -26,
    BadFileDescriptorState = -27,
    TooManySymbolicLinks = -29,
    FileTooLarge = -30,
    NoSpaceLeftOnDevice = -31,
    InvalidSeek = -32,
    ReadOnlyFileSystem = -33,
    TooManyLinks = -34,
    NumericResultTooLarge = -36,
    NoLocksAvailable = -37,
    FunctionNotImplemented = -38,
    DirectoryNotEmpty = -39,
    TooManyLevelsOfSymbolicLinks = -40,
    UnknownError = -41,
}

impl DBTreeError {
    pub fn tio(self) -> io::Error {
        let kind = match self {
            DBTreeError::Success => ErrorKind::Other,
            DBTreeError::SomethingWentWrong => ErrorKind::Other,
            DBTreeError::PermissionDenied => ErrorKind::PermissionDenied,
            DBTreeError::FileDoesNotExist => ErrorKind::NotFound,
            DBTreeError::FileExists => ErrorKind::AlreadyExists,
            DBTreeError::ProcessHaveTooManyOpenFiles | DBTreeError::SystemWideLimitOnOpenFiles => {
                ErrorKind::WouldBlock
            }
            DBTreeError::TriedOpeningADirectory => ErrorKind::Other,
            DBTreeError::InvalidMemory => ErrorKind::Other,
            DBTreeError::InvalidArgument
            | DBTreeError::InvalidRequestDescriptor
            | DBTreeError::InvalidExchange
            | DBTreeError::InvalidSeek
            | DBTreeError::ArgumentListTooLong => ErrorKind::InvalidInput,
            DBTreeError::NoSpaceLeft | DBTreeError::NoSpaceLeftOnDevice => ErrorKind::WriteZero,
            DBTreeError::IoError => ErrorKind::Other,
            DBTreeError::InterruptedBySignal => ErrorKind::Interrupted,
            DBTreeError::BrokenPipe => ErrorKind::BrokenPipe,
            DBTreeError::FailedToAllocateMemory => ErrorKind::Other,
            DBTreeError::ResourceTemporarilyUnavailable | DBTreeError::AlreadyInProgress => {
                ErrorKind::WouldBlock
            }
            DBTreeError::BadFileDescriptor | DBTreeError::BadFileDescriptorState => {
                ErrorKind::Other
            }
            DBTreeError::ExecFormatError | DBTreeError::FunctionNotImplemented => {
                ErrorKind::Unsupported
            }
            DBTreeError::NoChildProcesses => ErrorKind::Other,
            DBTreeError::AddressAlreadyInUse => ErrorKind::AddrInUse,
            DBTreeError::AddressNotAvailable => ErrorKind::AddrNotAvailable,
            DBTreeError::AddressFamilyNotSupported => ErrorKind::AddrNotAvailable,
            DBTreeError::BadMessage => ErrorKind::InvalidData,
            DBTreeError::TooManySymbolicLinks | DBTreeError::TooManyLevelsOfSymbolicLinks => {
                ErrorKind::Other
            }
            DBTreeError::FileTooLarge => ErrorKind::Other,
            DBTreeError::ReadOnlyFileSystem => ErrorKind::PermissionDenied,
            DBTreeError::TooManyLinks => ErrorKind::Other,
            DBTreeError::NumericResultTooLarge => ErrorKind::Other,
            DBTreeError::NoLocksAvailable => ErrorKind::Other,
            DBTreeError::DirectoryNotEmpty => ErrorKind::Other,
            DBTreeError::UnknownError => ErrorKind::Other,
        };
        Error::new(kind, format!("{:?}", self))
    }
}
