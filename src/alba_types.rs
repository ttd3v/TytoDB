use std::{
    fmt,
    io::{Error, ErrorKind},
};

use base64::{Engine, engine::general_purpose};
use serde::{Deserialize, Serialize};

use crate::{Token, database::MAX_STR_LEN};

#[derive(Clone, PartialEq, Deserialize, Serialize)]
pub enum AlbaTypes {
    Text(String),
    Int(i32),
    Bigint(i64),
    Float(f64),
    Bool(bool),
    Char(char),
    NanoString(String),
    SmallString(String),
    MediumString(String),
    BigString(String),
    LargeString(String),
    NanoBytes(Vec<u8>),
    SmallBytes(Vec<u8>),
    MediumBytes(Vec<u8>),
    BigSBytes(Vec<u8>),
    LargeBytes(Vec<u8>),

    LightPassword(Vec<u8>),  // [u8;32]
    MediumPassword(Vec<u8>), // [u8;64]
    HeavyPassword(Vec<u8>),  // [u8;128]

    Email(String), // max-len 320 (ascii only); overhead -> 2bytes (u16)

    Geo((f64, f64)),
    Slice4(Vec<u8>), // [32; u8]
    Slice3(Vec<u8>), // [20; u8]
    Slice2(Vec<u8>), // [16; u8]
    Slice1(Vec<u8>), // [6;u8]
    Slice0(Vec<u8>), // [4;u8]

    UInt(u32),
    UBigint(u64),
    NanoInt(i8),
    UNanoInt(u8),
    Short(i16),
    UShort(u16),
    HugeInt(i128),
    UHugeInt(u128),
    NONE,
}

fn serialize_closed_string(item_size: usize, s: &String, buffer: &mut Vec<u8>) {
    let mut bytes = Vec::with_capacity(item_size);
    let size = s.len().to_be_bytes();
    let mut str = s.to_owned();
    let _ = s;

    if item_size == AlbaTypes::size(&AlbaTypes::Email(String::new())) {
        let mut buff = [0u8; 320];
        let b = if s.is_ascii() {
            s.as_bytes().to_vec()
        } else {
            let u = unidecode::unidecode(&s);
            u.as_bytes().to_vec()
        };
        buff[..b.len()].copy_from_slice(&b);
        buffer.extend_from_slice(&buff);
        return;
    }

    if str.len() > item_size - 8 {
        str.truncate(item_size - 8);
    }
    let str_bytes = str.as_bytes();
    bytes.extend_from_slice(&size);
    bytes.extend_from_slice(str_bytes);

    if bytes.len() < item_size {
        bytes.resize(item_size, 0);
    }
    buffer.extend_from_slice(&bytes);
}

fn serialize_closed_blob(item_size: usize, mut blob: Vec<u8>, buffer: &mut Vec<u8>) {
    if item_size == 0 {
        eprintln!("Warning: item_size is 0, zero bytes were inserted instead");
        return;
    }

    if item_size > 1024 * 1024 {
        eprintln!(
            "Warning: item_size {} exceeds safe limit, skipping serialization",
            item_size
        );
        return;
    }

    match item_size {
        x if x == 32 || x == 64 || x == 4 || x == 6 || x == 16 || x == 20 || x == 128 => {
            if blob.len() > item_size {
                eprintln!(
                    "Warning: blob size {} exceeds item_size {}, truncating",
                    blob.len(),
                    item_size
                );
            }

            let mut bytes = vec![0u8; item_size];

            let copy_len = std::cmp::min(blob.len(), item_size);
            if copy_len > 0 {
                bytes[..copy_len].copy_from_slice(&blob[..copy_len]);
            } else {
                eprintln!("Warning: empty blob for fixed-size serialization");
            }

            buffer.extend_from_slice(&bytes);
        }
        _ => {
            let u64_size = size_of::<u64>();

            if item_size <= u64_size {
                eprintln!(
                    "Warning: item_size {} too small for length prefix ({}), using minimum",
                    item_size, u64_size
                );
                let blob_len = blob.len() as u64;
                let len_bytes = blob_len.to_le_bytes();
                let copy_len = std::cmp::min(item_size, len_bytes.len());
                let mut bytes = vec![0u8; item_size];
                bytes[..copy_len].copy_from_slice(&len_bytes[..copy_len]);
                buffer.extend_from_slice(&bytes);
                return;
            }

            let max_blob_size = item_size - u64_size;

            if blob.len() > max_blob_size {
                eprintln!(
                    "Warning: blob size {} exceeds available space {} (item_size {} - length_prefix {}), truncating",
                    blob.len(),
                    max_blob_size,
                    item_size,
                    u64_size
                );
                blob.truncate(max_blob_size);
            }

            let original_len = blob.len() as u64;
            let blob_length_bytes = original_len.to_le_bytes();

            let mut bytes = Vec::with_capacity(item_size);

            bytes.extend_from_slice(&blob_length_bytes);

            bytes.extend_from_slice(&blob);

            bytes.resize(item_size, 0);

            if bytes.len() != item_size {
                eprintln!(
                    "Warning: serialized size {} doesn't match expected item_size {}",
                    bytes.len(),
                    item_size
                );
                bytes.truncate(item_size);
                if bytes.len() < item_size {
                    bytes.resize(item_size, 0);
                }
            }

            buffer.extend_from_slice(&bytes);
        }
    }
}

pub fn into_schema(target: &mut Vec<AlbaTypes>, schema: &Vec<AlbaTypes>) -> Result<(), Error> {
    if target.len() != schema.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "Target length ({}) doesn't match schema length ({})",
                target.len(),
                schema.len()
            ),
        ));
    }

    for (t, s) in target.iter_mut().zip(schema.iter()) {
        if std::mem::discriminant(t) != std::mem::discriminant(s) {
            match convert_to_schema_type(t.clone(), s) {
                Ok(new_value) => {
                    *t = new_value;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}
fn convert_to_schema_type(source: AlbaTypes, schema_type: &AlbaTypes) -> Result<AlbaTypes, Error> {
    match schema_type {
        AlbaTypes::Text(_) => AlbaTypes::Text(String::new()).try_from_existing(source),
        AlbaTypes::Int(_) => AlbaTypes::Int(0).try_from_existing(source),
        AlbaTypes::Bigint(_) => AlbaTypes::Bigint(0).try_from_existing(source),
        AlbaTypes::Float(_) => AlbaTypes::Float(0.0).try_from_existing(source),
        AlbaTypes::Bool(_) => AlbaTypes::Bool(false).try_from_existing(source),
        AlbaTypes::Char(_) => AlbaTypes::Char('\0').try_from_existing(source),
        AlbaTypes::NanoString(_) => AlbaTypes::NanoString(String::new()).try_from_existing(source),
        AlbaTypes::SmallString(_) => {
            AlbaTypes::SmallString(String::new()).try_from_existing(source)
        }
        AlbaTypes::MediumString(_) => {
            AlbaTypes::MediumString(String::new()).try_from_existing(source)
        }
        AlbaTypes::BigString(_) => AlbaTypes::BigString(String::new()).try_from_existing(source),
        AlbaTypes::LargeString(_) => {
            AlbaTypes::LargeString(String::new()).try_from_existing(source)
        }
        AlbaTypes::NanoBytes(_) => AlbaTypes::NanoBytes(Vec::new()).try_from_existing(source),
        AlbaTypes::SmallBytes(_) => AlbaTypes::SmallBytes(Vec::new()).try_from_existing(source),
        AlbaTypes::MediumBytes(_) => AlbaTypes::MediumBytes(Vec::new()).try_from_existing(source),
        AlbaTypes::BigSBytes(_) => AlbaTypes::BigSBytes(Vec::new()).try_from_existing(source),
        AlbaTypes::LargeBytes(_) => AlbaTypes::LargeBytes(Vec::new()).try_from_existing(source),
        AlbaTypes::NONE => Ok(AlbaTypes::NONE),
        AlbaTypes::LightPassword(_) => {
            AlbaTypes::LightPassword(Vec::new()).try_from_existing(source)
        }
        AlbaTypes::MediumPassword(_) => {
            AlbaTypes::MediumPassword(Vec::new()).try_from_existing(source)
        }
        AlbaTypes::HeavyPassword(_) => {
            AlbaTypes::HeavyPassword(Vec::new()).try_from_existing(source)
        }
        AlbaTypes::Email(_) => AlbaTypes::Email(String::new()).try_from_existing(source),
        AlbaTypes::Geo(_) => AlbaTypes::Geo((0.0, 0.0)).try_from_existing(source),
        AlbaTypes::Slice4(_) => AlbaTypes::Slice4(Vec::new()).try_from_existing(source),
        AlbaTypes::Slice3(_) => AlbaTypes::Slice3(Vec::new()).try_from_existing(source),
        AlbaTypes::Slice2(_) => AlbaTypes::Slice2(Vec::new()).try_from_existing(source),
        AlbaTypes::Slice1(_) => AlbaTypes::Slice1(Vec::new()).try_from_existing(source),
        AlbaTypes::Slice0(_) => AlbaTypes::Slice0(Vec::new()).try_from_existing(source),
        AlbaTypes::UInt(_) => AlbaTypes::UInt(0).try_from_existing(source),
        AlbaTypes::UBigint(_) => AlbaTypes::UBigint(0).try_from_existing(source),
        AlbaTypes::NanoInt(_) => AlbaTypes::NanoInt(0).try_from_existing(source),
        AlbaTypes::UNanoInt(_) => AlbaTypes::UNanoInt(0).try_from_existing(source),
        AlbaTypes::Short(_) => AlbaTypes::Short(0).try_from_existing(source),
        AlbaTypes::UShort(_) => AlbaTypes::UShort(0).try_from_existing(source),
        AlbaTypes::HugeInt(_) => AlbaTypes::HugeInt(0).try_from_existing(source),
        AlbaTypes::UHugeInt(_) => AlbaTypes::UHugeInt(0).try_from_existing(source),
    }
}

impl AlbaTypes {
    pub fn serialize_into(&self, array: &mut Vec<u8>) {
        match self {
            // String types - serialize as bytes
            AlbaTypes::Text(a) => array.extend_from_slice(a.as_bytes()),

            // String types with size constraint
            AlbaTypes::NanoString(a)
            | AlbaTypes::SmallString(a)
            | AlbaTypes::MediumString(a)
            | AlbaTypes::BigString(a)
            | AlbaTypes::LargeString(a)
            | AlbaTypes::Email(a) => serialize_closed_string(self.size(), a, array),

            // All numeric types (little-endian) - each type separately due to different byte array sizes
            AlbaTypes::Int(a) => array.extend_from_slice(&a.to_le_bytes()),
            AlbaTypes::Bigint(a) => array.extend_from_slice(&a.to_le_bytes()),
            AlbaTypes::Float(a) => array.extend_from_slice(&a.to_le_bytes()),
            AlbaTypes::UInt(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::UBigint(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::NanoInt(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::UNanoInt(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::Short(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::UShort(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::HugeInt(u) => array.extend_from_slice(&u.to_le_bytes()),
            AlbaTypes::UHugeInt(u) => array.extend_from_slice(&u.to_le_bytes()),

            // Blob types (bytes and passwords)
            AlbaTypes::NanoBytes(items)
            | AlbaTypes::SmallBytes(items)
            | AlbaTypes::MediumBytes(items)
            | AlbaTypes::BigSBytes(items)
            | AlbaTypes::LargeBytes(items)
            | AlbaTypes::LightPassword(items)
            | AlbaTypes::MediumPassword(items)
            | AlbaTypes::HeavyPassword(items)
            | AlbaTypes::Slice4(items)
            | AlbaTypes::Slice3(items)
            | AlbaTypes::Slice2(items)
            | AlbaTypes::Slice1(items)
            | AlbaTypes::Slice0(items) => serialize_closed_blob(self.size(), items.clone(), array),

            // Special cases
            AlbaTypes::Bool(a) => array.push(*a as u8),
            AlbaTypes::Char(a) => array.extend_from_slice(&(*a as u32).to_le_bytes()),
            AlbaTypes::Geo((a, b)) => {
                array.extend_from_slice(&a.to_le_bytes());
                array.extend_from_slice(&b.to_le_bytes());
            }
            AlbaTypes::NONE => {}
        }
    }
}

fn format_bytes_debug(
    f: &mut fmt::Formatter<'_>,
    variant_name: &str,
    bytes: &Vec<u8>,
    limit: usize,
) -> fmt::Result {
    write!(f, "{}(", variant_name)?;
    let mut list = f.debug_list();
    list.entries(bytes.iter().take(limit));
    if bytes.len() > limit {
        list.entry(&format_args!("... ({} more bytes)", bytes.len() - limit));
    }
    list.finish()?;
    write!(f, ")")
}
impl fmt::Debug for AlbaTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // String types
            AlbaTypes::Text(s) => f.debug_tuple("Text").field(s).finish(),
            AlbaTypes::NanoString(s) => f.debug_tuple("NanoString").field(s).finish(),
            AlbaTypes::SmallString(s) => f.debug_tuple("SmallString").field(s).finish(),
            AlbaTypes::MediumString(s) => f.debug_tuple("MediumString").field(s).finish(),
            AlbaTypes::BigString(s) => f.debug_tuple("BigString").field(s).finish(),
            AlbaTypes::LargeString(s) => f.debug_tuple("LargeString").field(s).finish(),
            AlbaTypes::Email(s) => f.debug_tuple("Email").field(s).finish(),

            // Numeric types
            AlbaTypes::Int(i) => f.debug_tuple("Int").field(i).finish(),
            AlbaTypes::Bigint(i) => f.debug_tuple("Bigint").field(i).finish(),
            AlbaTypes::Float(fl) => f.debug_tuple("Float").field(fl).finish(),
            AlbaTypes::UInt(u) => f.debug_tuple("UInt").field(u).finish(),
            AlbaTypes::UBigint(u) => f.debug_tuple("UBigint").field(u).finish(),
            AlbaTypes::NanoInt(i) => f.debug_tuple("NanoInt").field(i).finish(),
            AlbaTypes::UNanoInt(u) => f.debug_tuple("UNanoInt").field(u).finish(),
            AlbaTypes::Short(s) => f.debug_tuple("Short").field(s).finish(),
            AlbaTypes::UShort(u) => f.debug_tuple("UShort").field(u).finish(),
            AlbaTypes::HugeInt(h) => f.debug_tuple("HugeInt").field(h).finish(),
            AlbaTypes::UHugeInt(u) => f.debug_tuple("UHugeInt").field(u).finish(),

            // Other primitive types
            AlbaTypes::Bool(b) => f.debug_tuple("Bool").field(b).finish(),
            AlbaTypes::Char(c) => f.debug_tuple("Char").field(c).finish(),
            AlbaTypes::Geo((lat, lon)) => f.debug_tuple("Geo").field(&(lat, lon)).finish(),

            // Byte types with limited display
            AlbaTypes::NanoBytes(bytes) => format_bytes_debug(f, "NanoBytes", bytes, 10),
            AlbaTypes::SmallBytes(bytes) => format_bytes_debug(f, "SmallBytes", bytes, 10),
            AlbaTypes::MediumBytes(bytes) => format_bytes_debug(f, "MediumBytes", bytes, 10),
            AlbaTypes::BigSBytes(bytes) => format_bytes_debug(f, "BigSBytes", bytes, 10),
            AlbaTypes::LargeBytes(bytes) => format_bytes_debug(f, "LargeBytes", bytes, 10),

            // Password types (limited display for security)
            AlbaTypes::LightPassword(bytes) => format_bytes_debug(f, "LightPassword", bytes, 3),
            AlbaTypes::MediumPassword(bytes) => format_bytes_debug(f, "MediumPassword", bytes, 3),
            AlbaTypes::HeavyPassword(bytes) => format_bytes_debug(f, "HeavyPassword", bytes, 3),

            // Slice types
            AlbaTypes::Slice4(bytes) => format_bytes_debug(f, "Slice4", bytes, 10),
            AlbaTypes::Slice3(bytes) => format_bytes_debug(f, "Slice3", bytes, 10),
            AlbaTypes::Slice2(bytes) => format_bytes_debug(f, "Slice2", bytes, 10),
            AlbaTypes::Slice1(bytes) => format_bytes_debug(f, "Slice1", bytes, 10),
            AlbaTypes::Slice0(bytes) => format_bytes_debug(f, "Slice0", bytes, 10),

            // Special case
            AlbaTypes::NONE => write!(f, "NONE"),
        }
    }
}

/*
char ~ 1
string~n ~ 10 + 8
string~s ~ 100 + 8
string-m ~ 500 + 8
string-b ~ 2,000 + 8
string-l ~ 3,000 + 8
bytes~n ~ 10 + 8
bytes~s ~ 1,000 + 8
bytes-m ~ 10,000 + 8
bytes-b ~ 100,000 + 8
bytes-l ~ 1,000,000 + 8

*/

impl AlbaTypes {
    pub fn from_id(code: u8) -> Result<AlbaTypes, Error> {
        match code {
            0 => Ok(AlbaTypes::NONE),
            1 => Ok(AlbaTypes::Char('\0')),
            2 => Ok(AlbaTypes::Int(0)),
            3 => Ok(AlbaTypes::Bigint(0)),
            4 => Ok(AlbaTypes::Bool(false)),
            5 => Ok(AlbaTypes::Float(0.0)),
            6 => Ok(AlbaTypes::Text(String::new())),
            7 => Ok(AlbaTypes::NanoString(String::new())),
            8 => Ok(AlbaTypes::SmallString(String::new())),
            9 => Ok(AlbaTypes::MediumString(String::new())),
            10 => Ok(AlbaTypes::BigString(String::new())),
            11 => Ok(AlbaTypes::LargeString(String::new())),
            12 => Ok(AlbaTypes::NanoBytes(Vec::new())),
            13 => Ok(AlbaTypes::SmallBytes(Vec::new())),
            14 => Ok(AlbaTypes::MediumBytes(Vec::new())),
            15 => Ok(AlbaTypes::BigSBytes(Vec::new())),
            16 => Ok(AlbaTypes::LargeBytes(Vec::new())),
            17 => Ok(AlbaTypes::LightPassword(Vec::new())),
            18 => Ok(AlbaTypes::MediumPassword(Vec::new())),
            19 => Ok(AlbaTypes::HeavyPassword(Vec::new())),
            20 => Ok(AlbaTypes::Email(String::new())),
            21 => Ok(AlbaTypes::Geo((0.0, 0.0))),
            22 => Ok(AlbaTypes::Slice4(Vec::new())),
            23 => Ok(AlbaTypes::Slice3(Vec::new())),
            24 => Ok(AlbaTypes::Slice2(Vec::new())),
            25 => Ok(AlbaTypes::Slice1(Vec::new())),
            26 => Ok(AlbaTypes::Slice0(Vec::new())),
            27 => Ok(AlbaTypes::UInt(0)),
            28 => Ok(AlbaTypes::UBigint(0)),
            29 => Ok(AlbaTypes::NanoInt(0)),
            30 => Ok(AlbaTypes::UNanoInt(0)),
            31 => Ok(AlbaTypes::Short(0)),
            32 => Ok(AlbaTypes::UShort(0)),
            33 => Ok(AlbaTypes::HugeInt(0)),
            34 => Ok(AlbaTypes::UHugeInt(0)),
            x => Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown AlbaTypes code: {}", x),
            )),
        }
    }
    pub fn get_id(&self) -> u8 {
        match self {
            AlbaTypes::NONE => 0,
            AlbaTypes::Char(_) => 1,
            AlbaTypes::Int(_) => 2,
            AlbaTypes::Bigint(_) => 3,
            AlbaTypes::Bool(_) => 4,
            AlbaTypes::Float(_) => 5,
            AlbaTypes::Text(_) => 6,
            AlbaTypes::NanoString(_) => 7,
            AlbaTypes::SmallString(_) => 8,
            AlbaTypes::MediumString(_) => 9,
            AlbaTypes::BigString(_) => 10,
            AlbaTypes::LargeString(_) => 11,
            AlbaTypes::NanoBytes(_) => 12,
            AlbaTypes::SmallBytes(_) => 13,
            AlbaTypes::MediumBytes(_) => 14,
            AlbaTypes::BigSBytes(_) => 15,
            AlbaTypes::LargeBytes(_) => 16,
            AlbaTypes::LightPassword(_) => 17,
            AlbaTypes::MediumPassword(_) => 18,
            AlbaTypes::HeavyPassword(_) => 19,
            AlbaTypes::Email(_) => 20,
            AlbaTypes::Geo(_) => 21,
            AlbaTypes::Slice4(_) => 22,
            AlbaTypes::Slice3(_) => 23,
            AlbaTypes::Slice2(_) => 24,
            AlbaTypes::Slice1(_) => 25,
            AlbaTypes::Slice0(_) => 26,
            AlbaTypes::UInt(_) => 27,
            AlbaTypes::UBigint(_) => 28,
            AlbaTypes::NanoInt(_) => 29,
            AlbaTypes::UNanoInt(_) => 30,
            AlbaTypes::Short(_) => 31,
            AlbaTypes::UShort(_) => 32,
            AlbaTypes::HugeInt(_) => 33,
            AlbaTypes::UHugeInt(_) => 34,
        }
    }

    // pub fn get_id_from_text(keyword: &str) -> Result<u8, Error> {
    //     match keyword.to_uppercase().as_str() {
    //         "INT"             => Ok(2),
    //         "BIGINT"          => Ok(3),
    //         "BOOL"            => Ok(4),
    //         "FLOAT"           => Ok(5),
    //         "TEXT"            => Ok(6),
    //         "NANO-STRING"     => Ok(7),
    //         "SMALL-STRING"    => Ok(8),
    //         "MEDIUM-STRING"   => Ok(9),
    //         "BIG-STRING"      => Ok(10),
    //         "LARGE-STRING"    => Ok(11),
    //         "NANO-BYTES"      => Ok(12),
    //         "SMALL-BYTES"     => Ok(13),
    //         "MEDIUM-BYTES"    => Ok(14),
    //         "BIG-BYTES"       => Ok(15),
    //         "LARGE-BYTES"     => Ok(16),
    //         other => Err(Error::new(
    //             ErrorKind::InvalidInput,
    //             format!("Unknown type keyword: {}", other)
    //         )),
    //     }
    // }
}

impl AlbaTypes {
    pub fn try_from_existing(&self, i: AlbaTypes) -> Result<AlbaTypes, Error> {
        match self {
            AlbaTypes::Text(_) => {
                let text = match i {
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s,
                    AlbaTypes::Int(n) => n.to_string(),
                    AlbaTypes::Bigint(n) => n.to_string(),
                    AlbaTypes::Float(f) => f.to_string(),
                    AlbaTypes::Bool(b) => b.to_string(),
                    AlbaTypes::Char(c) => c.to_string(),
                    AlbaTypes::UInt(n) => n.to_string(),
                    AlbaTypes::UBigint(n) => n.to_string(),
                    AlbaTypes::NanoInt(n) => n.to_string(),
                    AlbaTypes::UNanoInt(n) => n.to_string(),
                    AlbaTypes::Short(n) => n.to_string(),
                    AlbaTypes::UShort(n) => n.to_string(),
                    AlbaTypes::HugeInt(n) => n.to_string(),
                    AlbaTypes::UHugeInt(n) => n.to_string(),
                    AlbaTypes::NanoBytes(b)
                    | AlbaTypes::SmallBytes(b)
                    | AlbaTypes::MediumBytes(b)
                    | AlbaTypes::BigSBytes(b)
                    | AlbaTypes::LargeBytes(b)
                    | AlbaTypes::LightPassword(b)
                    | AlbaTypes::MediumPassword(b)
                    | AlbaTypes::HeavyPassword(b)
                    | AlbaTypes::Slice4(b)
                    | AlbaTypes::Slice3(b)
                    | AlbaTypes::Slice2(b)
                    | AlbaTypes::Slice1(b)
                    | AlbaTypes::Slice0(b) => general_purpose::STANDARD.encode(&b),
                    AlbaTypes::Geo((lat, lon)) => format!("({}, {})", lat, lon),
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Text",
                        ));
                    }
                };
                Ok(AlbaTypes::Text(text))
            }
            AlbaTypes::Int(_) => {
                let int_val = match i {
                    AlbaTypes::Int(n) => n,
                    AlbaTypes::Bigint(n) => {
                        if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                            n as i32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Bigint out of range for i32",
                            ));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Cannot convert NaN or infinite float to i32",
                            ));
                        }
                        f as i32
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::UInt(n) => {
                        if n <= i32::MAX as u32 {
                            n as i32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UInt out of range for i32",
                            ));
                        }
                    }
                    AlbaTypes::UBigint(n) => {
                        if n <= i32::MAX as u64 {
                            n as i32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UBigint out of range for i32",
                            ));
                        }
                    }
                    AlbaTypes::NanoInt(n) => n as i32,
                    AlbaTypes::UNanoInt(n) => n as i32,
                    AlbaTypes::Short(n) => n as i32,
                    AlbaTypes::UShort(n) => n as i32,
                    AlbaTypes::HugeInt(n) => {
                        if n >= i32::MIN as i128 && n <= i32::MAX as i128 {
                            n as i32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "HugeInt out of range for i32",
                            ));
                        }
                    }
                    AlbaTypes::UHugeInt(n) => {
                        if n <= i32::MAX as u128 {
                            n as i32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UHugeInt out of range for i32",
                            ));
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<i32>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as i32")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Int",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Int",
                        ));
                    }
                };
                Ok(AlbaTypes::Int(int_val))
            }
            AlbaTypes::Bigint(_) => {
                let bigint_val = match i {
                    AlbaTypes::Bigint(n) => n,
                    AlbaTypes::Int(n) => n as i64,
                    AlbaTypes::Float(f) => {
                        if f.is_nan() || f.is_infinite() {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Cannot convert NaN or infinite float to i64",
                            ));
                        }
                        f as i64
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::UInt(n) => n as i64,
                    AlbaTypes::UBigint(n) => {
                        if n <= i64::MAX as u64 {
                            n as i64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UBigint out of range for i64",
                            ));
                        }
                    }
                    AlbaTypes::NanoInt(n) => n as i64,
                    AlbaTypes::UNanoInt(n) => n as i64,
                    AlbaTypes::Short(n) => n as i64,
                    AlbaTypes::UShort(n) => n as i64,
                    AlbaTypes::HugeInt(n) => {
                        if n >= i64::MIN as i128 && n <= i64::MAX as i128 {
                            n as i64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "HugeInt out of range for i64",
                            ));
                        }
                    }
                    AlbaTypes::UHugeInt(n) => {
                        if n <= i64::MAX as u128 {
                            n as i64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UHugeInt out of range for i64",
                            ));
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<i64>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as i64")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Bigint",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Bigint",
                        ));
                    }
                };
                Ok(AlbaTypes::Bigint(bigint_val))
            }
            AlbaTypes::Float(_) => {
                let float_val = match i {
                    AlbaTypes::Float(f) => f,
                    AlbaTypes::Int(n) => n as f64,
                    AlbaTypes::Bigint(n) => n as f64,
                    AlbaTypes::Bool(b) => {
                        if b {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    AlbaTypes::UInt(n) => n as f64,
                    AlbaTypes::UBigint(n) => n as f64,
                    AlbaTypes::NanoInt(n) => n as f64,
                    AlbaTypes::UNanoInt(n) => n as f64,
                    AlbaTypes::Short(n) => n as f64,
                    AlbaTypes::UShort(n) => n as f64,
                    AlbaTypes::HugeInt(n) => n as f64,
                    AlbaTypes::UHugeInt(n) => n as f64,
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<f64>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as f64")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Float",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Float",
                        ));
                    }
                };
                Ok(AlbaTypes::Float(float_val))
            }
            AlbaTypes::Bool(_) => {
                let bool_val = match i {
                    AlbaTypes::Bool(b) => b,
                    AlbaTypes::Int(n) => n != 0,
                    AlbaTypes::Bigint(n) => n != 0,
                    AlbaTypes::Float(f) => f != 0.0,
                    AlbaTypes::UInt(n) => n != 0,
                    AlbaTypes::UBigint(n) => n != 0,
                    AlbaTypes::NanoInt(n) => n != 0,
                    AlbaTypes::UNanoInt(n) => n != 0,
                    AlbaTypes::Short(n) => n != 0,
                    AlbaTypes::UShort(n) => n != 0,
                    AlbaTypes::HugeInt(n) => n != 0,
                    AlbaTypes::UHugeInt(n) => n != 0,
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => {
                        let trimmed = s.trim().to_lowercase();
                        match trimmed.as_str() {
                            "0" | "f" | "false" => false,
                            "1" | "t" | "true" => true,
                            _ => {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid boolean string",
                                ));
                            }
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Bool",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Bool",
                        ));
                    }
                };
                Ok(AlbaTypes::Bool(bool_val))
            }
            AlbaTypes::Char(_) => {
                let char_val = match i {
                    AlbaTypes::Char(c) => c,
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => {
                        if s.len() == 1 {
                            s.chars().next().unwrap()
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "String must be a single character for Char",
                            ));
                        }
                    }
                    AlbaTypes::UNanoInt(n) => n as char,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Char",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Char",
                        ));
                    }
                };
                Ok(AlbaTypes::Char(char_val))
            }
            AlbaTypes::NanoString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::NanoString(truncate_or_pad_string(s, 10)))
            }
            AlbaTypes::SmallString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::SmallString(truncate_or_pad_string(s, 100)))
            }
            AlbaTypes::MediumString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::MediumString(truncate_or_pad_string(s, 500)))
            }
            AlbaTypes::BigString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::BigString(truncate_or_pad_string(s, 2000)))
            }
            AlbaTypes::LargeString(_) => {
                let s = get_string_from_alba_type(i)?;
                Ok(AlbaTypes::LargeString(truncate_or_pad_string(s, 3000)))
            }
            AlbaTypes::Email(_) => {
                let s = get_string_from_alba_type(i)?;
                // Basic email validation could be added here
                Ok(AlbaTypes::Email(truncate_or_pad_string(s, 320)))
            }
            AlbaTypes::NanoBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::NanoBytes(truncate_or_pad_bytes(bytes, 10)))
            }
            AlbaTypes::SmallBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::SmallBytes(truncate_or_pad_bytes(bytes, 1000)))
            }
            AlbaTypes::MediumBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::MediumBytes(truncate_or_pad_bytes(bytes, 10_000)))
            }
            AlbaTypes::BigSBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::BigSBytes(truncate_or_pad_bytes(bytes, 100_000)))
            }
            AlbaTypes::LargeBytes(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::LargeBytes(truncate_or_pad_bytes(
                    bytes, 1_000_000,
                )))
            }
            AlbaTypes::LightPassword(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::LightPassword(truncate_or_pad_bytes(bytes, 32)))
            }
            AlbaTypes::MediumPassword(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::MediumPassword(truncate_or_pad_bytes(bytes, 64)))
            }
            AlbaTypes::HeavyPassword(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::HeavyPassword(truncate_or_pad_bytes(bytes, 128)))
            }
            AlbaTypes::Geo(_) => {
                let (lat, lon) = match i {
                    AlbaTypes::Geo(coords) => coords,
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => {
                        // Parse string format like "(lat, lon)" or "lat,lon"
                        let cleaned = s.trim().trim_matches('(').trim_matches(')');
                        let parts: Vec<&str> = cleaned.split(',').collect();
                        if parts.len() == 2 {
                            let lat: f64 = parts[0].trim().parse().map_err(|_| {
                                Error::new(ErrorKind::InvalidData, "Invalid latitude")
                            })?;
                            let lon: f64 = parts[1].trim().parse().map_err(|_| {
                                Error::new(ErrorKind::InvalidData, "Invalid longitude")
                            })?;
                            (lat, lon)
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Invalid geo format, expected 'lat,lon'",
                            ));
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Geo",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Geo",
                        ));
                    }
                };
                Ok(AlbaTypes::Geo((lat, lon)))
            }
            AlbaTypes::Slice4(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::Slice4(truncate_or_pad_bytes(bytes, 32)))
            }
            AlbaTypes::Slice3(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::Slice3(truncate_or_pad_bytes(bytes, 20)))
            }
            AlbaTypes::Slice2(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::Slice2(truncate_or_pad_bytes(bytes, 16)))
            }
            AlbaTypes::Slice1(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::Slice1(truncate_or_pad_bytes(bytes, 6)))
            }
            AlbaTypes::Slice0(_) => {
                let bytes = get_bytes_from_alba_type(i)?;
                Ok(AlbaTypes::Slice0(truncate_or_pad_bytes(bytes, 4)))
            }
            AlbaTypes::UInt(_) => {
                let uint_val = match i {
                    AlbaTypes::UInt(n) => n,
                    AlbaTypes::Int(n) => {
                        if n >= 0 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative value cannot be converted to UInt",
                            ));
                        }
                    }
                    AlbaTypes::UBigint(n) => {
                        if n <= u32::MAX as u64 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UBigint out of range for u32",
                            ));
                        }
                    }
                    AlbaTypes::Bigint(n) => {
                        if n >= 0 && n <= u32::MAX as i64 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Bigint out of range for u32",
                            ));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f >= 0.0 && f <= u32::MAX as f64 && !f.is_nan() && !f.is_infinite() {
                            f as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Float out of range for u32",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NanoInt(n) => {
                        if n >= 0 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative NanoInt cannot be converted to UInt",
                            ));
                        }
                    }
                    AlbaTypes::UNanoInt(n) => n as u32,
                    AlbaTypes::Short(n) => {
                        if n >= 0 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Short cannot be converted to UInt",
                            ));
                        }
                    }
                    AlbaTypes::UShort(n) => n as u32,
                    AlbaTypes::HugeInt(n) => {
                        if n >= 0 && n <= u32::MAX as i128 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "HugeInt out of range for u32",
                            ));
                        }
                    }
                    AlbaTypes::UHugeInt(n) => {
                        if n <= u32::MAX as u128 {
                            n as u32
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UHugeInt out of range for u32",
                            ));
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<u32>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as u32")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to UInt",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to UInt",
                        ));
                    }
                };
                Ok(AlbaTypes::UInt(uint_val))
            }
            AlbaTypes::UBigint(_) => {
                let ubigint_val = match i {
                    AlbaTypes::UBigint(n) => n,
                    AlbaTypes::UInt(n) => n as u64,
                    AlbaTypes::Bigint(n) => {
                        if n >= 0 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative value cannot be converted to UBigint",
                            ));
                        }
                    }
                    AlbaTypes::Int(n) => {
                        if n >= 0 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative value cannot be converted to UBigint",
                            ));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f >= 0.0 && f <= u64::MAX as f64 && !f.is_nan() && !f.is_infinite() {
                            f as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Float out of range for u64",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NanoInt(n) => {
                        if n >= 0 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative NanoInt cannot be converted to UBigint",
                            ));
                        }
                    }
                    AlbaTypes::UNanoInt(n) => n as u64,
                    AlbaTypes::Short(n) => {
                        if n >= 0 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Short cannot be converted to UBigint",
                            ));
                        }
                    }
                    AlbaTypes::UShort(n) => n as u64,
                    AlbaTypes::HugeInt(n) => {
                        if n >= 0 && n <= u64::MAX as i128 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "HugeInt out of range for u64",
                            ));
                        }
                    }
                    AlbaTypes::UHugeInt(n) => {
                        if n <= u64::MAX as u128 {
                            n as u64
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UHugeInt out of range for u64",
                            ));
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<u64>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as u64")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to UBigint",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to UBigint",
                        ));
                    }
                };
                Ok(AlbaTypes::UBigint(ubigint_val))
            }
            AlbaTypes::NanoInt(_) => {
                let nano_val = match i {
                    AlbaTypes::NanoInt(n) => n,
                    AlbaTypes::UNanoInt(n) => {
                        if n <= i8::MAX as u8 {
                            n as i8
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UNanoInt out of range for i8",
                            ));
                        }
                    }
                    AlbaTypes::Int(n) => {
                        if n >= i8::MIN as i32 && n <= i8::MAX as i32 {
                            n as i8
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Int out of range for i8",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to NanoInt",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to NanoInt",
                        ));
                    }
                };
                Ok(AlbaTypes::NanoInt(nano_val))
            }
            AlbaTypes::UNanoInt(_) => {
                let unano_val = match i {
                    AlbaTypes::UNanoInt(n) => n,
                    AlbaTypes::NanoInt(n) => {
                        if n >= 0 {
                            n as u8
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative NanoInt cannot be converted to u8",
                            ));
                        }
                    }
                    AlbaTypes::Int(n) => {
                        if n >= 0 && n <= u8::MAX as i32 {
                            n as u8
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Int out of range for u8",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to UNanoInt",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to UNanoInt",
                        ));
                    }
                };
                Ok(AlbaTypes::UNanoInt(unano_val))
            }
            AlbaTypes::Short(_) => {
                let short_val = match i {
                    AlbaTypes::Short(n) => n,
                    AlbaTypes::UShort(n) => {
                        if n <= i16::MAX as u16 {
                            n as i16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UShort out of range for i16",
                            ));
                        }
                    }
                    AlbaTypes::Int(n) => {
                        if n >= i16::MIN as i32 && n <= i16::MAX as i32 {
                            n as i16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Int out of range for i16",
                            ));
                        }
                    }
                    AlbaTypes::NanoInt(n) => n as i16,
                    AlbaTypes::UNanoInt(n) => n as i16,
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to Short",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to Short",
                        ));
                    }
                };
                Ok(AlbaTypes::Short(short_val))
            }
            AlbaTypes::UShort(_) => {
                let ushort_val = match i {
                    AlbaTypes::UShort(n) => n,
                    AlbaTypes::Short(n) => {
                        if n >= 0 {
                            n as u16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Short cannot be converted to u16",
                            ));
                        }
                    }
                    AlbaTypes::Int(n) => {
                        if n >= 0 && n <= u16::MAX as i32 {
                            n as u16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Int out of range for u16",
                            ));
                        }
                    }
                    AlbaTypes::UInt(n) => {
                        if n <= u16::MAX as u32 {
                            n as u16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UInt out of range for u16",
                            ));
                        }
                    }
                    AlbaTypes::NanoInt(n) => {
                        if n >= 0 {
                            n as u16
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative NanoInt cannot be converted to u16",
                            ));
                        }
                    }
                    AlbaTypes::UNanoInt(n) => n as u16,
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to UShort",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to UShort",
                        ));
                    }
                };
                Ok(AlbaTypes::UShort(ushort_val))
            }
            AlbaTypes::HugeInt(_) => {
                let huge_val = match i {
                    AlbaTypes::HugeInt(n) => n,
                    AlbaTypes::UHugeInt(n) => {
                        if n <= i128::MAX as u128 {
                            n as i128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "UHugeInt out of range for i128",
                            ));
                        }
                    }
                    AlbaTypes::Bigint(n) => n as i128,
                    AlbaTypes::UBigint(n) => n as i128,
                    AlbaTypes::Int(n) => n as i128,
                    AlbaTypes::UInt(n) => n as i128,
                    AlbaTypes::Short(n) => n as i128,
                    AlbaTypes::UShort(n) => n as i128,
                    AlbaTypes::NanoInt(n) => n as i128,
                    AlbaTypes::UNanoInt(n) => n as i128,
                    AlbaTypes::Float(f) => {
                        if !f.is_nan() && !f.is_infinite() {
                            f as i128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Cannot convert NaN or infinite float to i128",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<i128>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as i128")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to HugeInt",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to HugeInt",
                        ));
                    }
                };
                Ok(AlbaTypes::HugeInt(huge_val))
            }
            AlbaTypes::UHugeInt(_) => {
                let uhuge_val = match i {
                    AlbaTypes::UHugeInt(n) => n,
                    AlbaTypes::HugeInt(n) => {
                        if n >= 0 {
                            n as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative HugeInt cannot be converted to u128",
                            ));
                        }
                    }
                    AlbaTypes::UBigint(n) => n as u128,
                    AlbaTypes::Bigint(n) => {
                        if n >= 0 {
                            n as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Bigint cannot be converted to u128",
                            ));
                        }
                    }
                    AlbaTypes::UInt(n) => n as u128,
                    AlbaTypes::Int(n) => {
                        if n >= 0 {
                            n as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Int cannot be converted to u128",
                            ));
                        }
                    }
                    AlbaTypes::UShort(n) => n as u128,
                    AlbaTypes::Short(n) => {
                        if n >= 0 {
                            n as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative Short cannot be converted to u128",
                            ));
                        }
                    }
                    AlbaTypes::UNanoInt(n) => n as u128,
                    AlbaTypes::NanoInt(n) => {
                        if n >= 0 {
                            n as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Negative NanoInt cannot be converted to u128",
                            ));
                        }
                    }
                    AlbaTypes::Float(f) => {
                        if f >= 0.0 && !f.is_nan() && !f.is_infinite() {
                            f as u128
                        } else {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                "Float out of range for u128",
                            ));
                        }
                    }
                    AlbaTypes::Bool(b) => {
                        if b {
                            1
                        } else {
                            0
                        }
                    }
                    AlbaTypes::Text(s)
                    | AlbaTypes::NanoString(s)
                    | AlbaTypes::SmallString(s)
                    | AlbaTypes::MediumString(s)
                    | AlbaTypes::BigString(s)
                    | AlbaTypes::LargeString(s)
                    | AlbaTypes::Email(s) => s.parse::<u128>().map_err(|_| {
                        Error::new(ErrorKind::InvalidData, "Failed to parse string as u128")
                    })?,
                    AlbaTypes::NONE => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Cannot convert NONE to UHugeInt",
                        ));
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Unsupported conversion to UHugeInt",
                        ));
                    }
                };
                Ok(AlbaTypes::UHugeInt(uhuge_val))
            }
            AlbaTypes::NONE => Ok(AlbaTypes::NONE),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            // Basic numeric types
            AlbaTypes::Bool(_) => size_of::<bool>(),
            AlbaTypes::Char(_) => size_of::<char>(), // 4 bytes in Rust (Unicode scalar)
            AlbaTypes::NanoInt(_) => size_of::<i8>(),
            AlbaTypes::UNanoInt(_) => size_of::<u8>(),
            AlbaTypes::Short(_) => size_of::<i16>(),
            AlbaTypes::UShort(_) => size_of::<u16>(),
            AlbaTypes::Int(_) => size_of::<i32>(),
            AlbaTypes::UInt(_) => size_of::<u32>(),
            AlbaTypes::Bigint(_) => size_of::<i64>(),
            AlbaTypes::UBigint(_) => size_of::<u64>(),
            AlbaTypes::HugeInt(_) => size_of::<i128>(),
            AlbaTypes::UHugeInt(_) => size_of::<u128>(),
            AlbaTypes::Float(_) => size_of::<f64>(),

            // Special types
            AlbaTypes::NONE => 0,
            AlbaTypes::Text(_) => MAX_STR_LEN,
            AlbaTypes::Geo(_) => size_of::<f64>() * 2, // (lat, lon) - two f64s

            // String types with fixed sizes + length header
            AlbaTypes::NanoString(_) => 10 + size_of::<u64>(),
            AlbaTypes::SmallString(_) => 100 + size_of::<u64>(),
            AlbaTypes::MediumString(_) => 500 + size_of::<u64>(),
            AlbaTypes::BigString(_) => 2_000 + size_of::<u64>(),
            AlbaTypes::LargeString(_) => 3_000 + size_of::<u64>(),
            AlbaTypes::Email(_) => 320, // max 320 chars

            // Byte types with fixed sizes + length header
            AlbaTypes::NanoBytes(_) => 10 + size_of::<u64>(),
            AlbaTypes::SmallBytes(_) => 1_000 + size_of::<u64>(),
            AlbaTypes::MediumBytes(_) => 10_000 + size_of::<u64>(),
            AlbaTypes::BigSBytes(_) => 100_000 + size_of::<u64>(),
            AlbaTypes::LargeBytes(_) => 1_000_000 + size_of::<u64>(),

            AlbaTypes::LightPassword(_) => 32,  // [u8;32]
            AlbaTypes::MediumPassword(_) => 64, // [u8;64]
            AlbaTypes::HeavyPassword(_) => 128, // [u8;128]

            AlbaTypes::Slice0(_) => 4,  // [4;u8]
            AlbaTypes::Slice1(_) => 6,  // [6;u8]
            AlbaTypes::Slice2(_) => 16, // [16;u8]
            AlbaTypes::Slice3(_) => 20, // [20;u8]
            AlbaTypes::Slice4(_) => 32, // [32;u8]
        }
    }
}

fn get_string_from_alba_type(i: AlbaTypes) -> Result<String, Error> {
    match i {
        AlbaTypes::Text(s)
        | AlbaTypes::NanoString(s)
        | AlbaTypes::SmallString(s)
        | AlbaTypes::MediumString(s)
        | AlbaTypes::BigString(s)
        | AlbaTypes::LargeString(s)
        | AlbaTypes::Email(s) => Ok(s),
        AlbaTypes::Int(n) => Ok(n.to_string()),
        AlbaTypes::Bigint(n) => Ok(n.to_string()),
        AlbaTypes::Float(f) => Ok(f.to_string()),
        AlbaTypes::UInt(n) => Ok(n.to_string()),
        AlbaTypes::UBigint(n) => Ok(n.to_string()),
        AlbaTypes::NanoInt(n) => Ok(n.to_string()),
        AlbaTypes::UNanoInt(n) => Ok(n.to_string()),
        AlbaTypes::Short(n) => Ok(n.to_string()),
        AlbaTypes::UShort(n) => Ok(n.to_string()),
        AlbaTypes::HugeInt(n) => Ok(n.to_string()),
        AlbaTypes::UHugeInt(n) => Ok(n.to_string()),

        AlbaTypes::Bool(b) => Ok(b.to_string()),
        AlbaTypes::Char(c) => Ok(c.to_string()),

        AlbaTypes::Geo((lat, lon)) => Ok(format!("({}, {})", lat, lon)),

        AlbaTypes::NanoBytes(b)
        | AlbaTypes::SmallBytes(b)
        | AlbaTypes::MediumBytes(b)
        | AlbaTypes::BigSBytes(b)
        | AlbaTypes::LargeBytes(b)
        | AlbaTypes::LightPassword(b)
        | AlbaTypes::MediumPassword(b)
        | AlbaTypes::HeavyPassword(b)
        | AlbaTypes::Slice4(b)
        | AlbaTypes::Slice3(b)
        | AlbaTypes::Slice2(b)
        | AlbaTypes::Slice1(b)
        | AlbaTypes::Slice0(b) => Ok(general_purpose::STANDARD.encode(&b)),

        AlbaTypes::NONE => Err(Error::new(
            ErrorKind::InvalidData,
            "Cannot convert NONE to string",
        )),
    }
}

fn truncate_or_pad_string(s: String, max_len: usize) -> String {
    if s.len() > max_len {
        s[..max_len].to_string()
    } else {
        s
    }
}

fn get_bytes_from_alba_type(i: AlbaTypes) -> Result<Vec<u8>, Error> {
    match i {
        AlbaTypes::NanoBytes(b)
        | AlbaTypes::SmallBytes(b)
        | AlbaTypes::MediumBytes(b)
        | AlbaTypes::BigSBytes(b)
        | AlbaTypes::LargeBytes(b) => Ok(b),
        AlbaTypes::Text(s)
        | AlbaTypes::NanoString(s)
        | AlbaTypes::SmallString(s)
        | AlbaTypes::MediumString(s)
        | AlbaTypes::BigString(s)
        | AlbaTypes::LargeString(s) => general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid base64 string")),
        AlbaTypes::NONE => Err(Error::new(
            ErrorKind::InvalidData,
            "Cannot convert NONE to bytes",
        )),
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            "Unsupported conversion to bytes",
        )),
    }
}

fn truncate_or_pad_bytes(b: Vec<u8>, max_len: usize) -> Vec<u8> {
    let mut bytes = b;
    if bytes.len() > max_len {
        bytes.truncate(max_len);
    } else {
        bytes.resize(max_len, 0);
    }
    bytes
}

impl TryFrom<Token> for AlbaTypes {
    type Error = &'static str;

    fn try_from(token: Token) -> Result<Self, Self::Error> {
        match token {
            Token::Bytes(b) => {
                // match size {
                //     10 => values.push(AlbaTypes::NanoBytes(blob)),
                //     1000 => values.push(AlbaTypes::SmallBytes(blob)),
                //     10_000 => values.push(AlbaTypes::MediumBytes(blob)),
                //     100_000 => values.push(AlbaTypes::BigSBytes(blob)),
                //     1_000_000 => values.push(AlbaTypes::LargeBytes(blob)),
                //     _ => unreachable!(),
                // }
                let l = b.len();
                Ok(if l <= 10 {
                    AlbaTypes::NanoBytes(b)
                } else if l > 10 && l <= 1000 {
                    AlbaTypes::SmallBytes(b)
                } else if l > 1000 && l <= 10000 {
                    AlbaTypes::MediumBytes(b)
                } else if l > 10000 && l <= 100000 {
                    AlbaTypes::BigSBytes(b)
                } else {
                    AlbaTypes::LargeBytes(b)
                })
            }
            Token::String(s) => Ok(AlbaTypes::LargeString(s)), // moved, no clone

            Token::Int(i) if (i32::MIN as i64) <= i && i <= (i32::MAX as i64) => {
                Ok(AlbaTypes::Int(i as i32))
            }

            Token::Int(i) => Ok(AlbaTypes::Bigint(i)),

            Token::Float(f) => Ok(AlbaTypes::Float(f)),

            Token::Bool(b) => Ok(AlbaTypes::Bool(b)),
            Token::Keyword(s) => match s.to_uppercase().as_str().trim() {
                "INT" => Ok(AlbaTypes::Int(0)), // default dummy values
                "BIGINT" => Ok(AlbaTypes::Bigint(0)),
                "FLOAT" => Ok(AlbaTypes::Float(0.0)),
                "BOOL" => Ok(AlbaTypes::Bool(false)),
                "TEXT" => Ok(AlbaTypes::Text(String::new())),
                "NANO-STRING" => Ok(AlbaTypes::NanoString(String::new())),
                "SMALL-STRING" => Ok(AlbaTypes::SmallString(String::new())),
                "MEDIUM-STRING" => Ok(AlbaTypes::MediumString(String::new())),
                "BIG-STRING" => Ok(AlbaTypes::BigString(String::new())),
                "LARGE-STRING" => Ok(AlbaTypes::LargeString(String::new())),
                "NANO-BYTES" => Ok(AlbaTypes::NanoBytes(Vec::new())),
                "SMALL-BYTES" => Ok(AlbaTypes::SmallBytes(Vec::new())),
                "MEDIUM-BYTES" => Ok(AlbaTypes::MediumBytes(Vec::new())),
                "BIG-BYTES" => Ok(AlbaTypes::BigSBytes(Vec::new())),
                "LARGE-BYTES" => Ok(AlbaTypes::LargeBytes(Vec::new())),
                _ => return Err(format!("Unknown type keyword: {}", s).leak()),
            },
            _ => {
                let va = format!(
                    "Cannot convert token to AlbaTypes: unsupported token type {:#?}. Expected one of: String, Int, Float, Bool, or Keyword (for type definitions).",
                    token
                );
                return Err(va.leak());
            }
        }
    }
}
