use std::cmp::Ordering;
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign, Mul, MulAssign, Div, DivAssign, Rem, RemAssign};
use std::convert::{From, TryFrom};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSizes {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Usize(usize),
}

impl IndexSizes {
    pub fn to_usize(a: IndexSizes) -> usize {
        match a {
            IndexSizes::U8(a) => a as usize,
            IndexSizes::U16(a) => a as usize,
            IndexSizes::U32(a) => a as usize,
            IndexSizes::U64(a) => a as usize,
            IndexSizes::Usize(a) => a as usize,
        }
    }
    
    pub fn as_usize(&self) -> usize {
        match self {
            IndexSizes::U8(val) => *val as usize,
            IndexSizes::U16(val) => *val as usize,
            IndexSizes::U32(val) => *val as usize,
            IndexSizes::U64(val) => *val as usize,
            IndexSizes::Usize(val) => *val,
        }
    }

    pub fn proper(r: usize) -> IndexSizes {
        if r <= u8::MAX as usize {
            return IndexSizes::U8(r as u8);
        }
        if r <= u16::MAX as usize {
            return IndexSizes::U16(r as u16);
        }
        if r <= u32::MAX as usize {
            return IndexSizes::U32(r as u32);
        }
        if r <= u64::MAX as usize {
            return IndexSizes::U64(r as u64);
        }
        IndexSizes::Usize(r)
    }
    
    pub fn proper_u64(r: u64) -> IndexSizes {
        if r <= u8::MAX as u64 {
            return IndexSizes::U8(r as u8);
        }
        if r <= u16::MAX as u64 {
            return IndexSizes::U16(r as u16);
        }
        if r <= u32::MAX as u64 {
            return IndexSizes::U32(r as u32);
        }
        IndexSizes::U64(r)
    }
    
    pub fn to_u64(&self) -> u64 {
        match self {
            IndexSizes::U8(val) => *val as u64,
            IndexSizes::U16(val) => *val as u64,
            IndexSizes::U32(val) => *val as u64,
            IndexSizes::U64(val) => *val,
            IndexSizes::Usize(val) => *val as u64,
        }
    }
    
    pub fn checked_add(self, rhs: Self) -> Option<Self> {
        let a = self.to_u64();
        let b = rhs.to_u64();
        a.checked_add(b).map(|r| Self::proper_u64(r))
    }
    
    pub fn checked_sub(self, rhs: Self) -> Option<Self> {
        let a = self.to_u64();
        let b = rhs.to_u64();
        a.checked_sub(b).map(|r| Self::proper_u64(r))
    }
    
    pub fn checked_mul(self, rhs: Self) -> Option<Self> {
        let a = self.to_u64();
        let b = rhs.to_u64();
        a.checked_mul(b).map(|r| Self::proper_u64(r))
    }
    
    pub fn checked_div(self, rhs: Self) -> Option<Self> {
        if rhs.to_u64() == 0 {
            return None;
        }
        let a = self.to_u64();
        let b = rhs.to_u64();
        a.checked_div(b).map(|r| Self::proper_u64(r))
    }
    
    pub fn wrapping_add(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.wrapping_add(b))
    }
    
    pub fn wrapping_sub(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.wrapping_sub(b))
    }
    
    pub fn wrapping_mul(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.wrapping_mul(b))
    }
    
    pub fn max(self, other: Self) -> Self {
        if self >= other { self } else { other }
    }
    
    pub fn min(self, other: Self) -> Self {
        if self <= other { self } else { other }
    }
}

impl PartialOrd for IndexSizes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexSizes {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_val = match self {
            IndexSizes::U8(val) => *val as u64,
            IndexSizes::U16(val) => *val as u64,
            IndexSizes::U32(val) => *val as u64,
            IndexSizes::U64(val) => *val,
            IndexSizes::Usize(val) => *val as u64, 
        };
        let other_val = match other {
            IndexSizes::U8(val) => *val as u64,
            IndexSizes::U16(val) => *val as u64,
            IndexSizes::U32(val) => *val as u64,
            IndexSizes::U64(val) => *val,
            IndexSizes::Usize(val) => *val as u64,
        };
        if self_val == other_val {
            let self_rank = match self {
                IndexSizes::U8(_) => 0,
                IndexSizes::U16(_) => 1,
                IndexSizes::U32(_) => 2,
                IndexSizes::U64(_) => 3,
                IndexSizes::Usize(_) => 4,
            };
            let other_rank = match other {
                IndexSizes::U8(_) => 0,
                IndexSizes::U16(_) => 1,
                IndexSizes::U32(_) => 2,
                IndexSizes::U64(_) => 3,
                IndexSizes::Usize(_) => 4,
            };
            self_rank.cmp(&other_rank)
        } else {
            self_val.cmp(&other_val)
        }
    }
}

// Addition
impl Add for IndexSizes {
    type Output = Self;
    
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (IndexSizes::U8(a), IndexSizes::U8(b)) => {
                if let Some(result) = a.checked_add(b) {
                    IndexSizes::U8(result)
                } else {
                    IndexSizes::U16(a as u16 + b as u16)
                }
            },
            (IndexSizes::U8(a), IndexSizes::U16(b)) => {
                if let Some(result) = (a as u16).checked_add(b) {
                    IndexSizes::U16(result)
                } else {
                    IndexSizes::U32(a as u32 + b as u32)
                }
            },
            (IndexSizes::U16(a), IndexSizes::U8(b)) => {
                if let Some(result) = a.checked_add(b as u16) {
                    IndexSizes::U16(result)
                } else {
                    IndexSizes::U32(a as u32 + b as u32)
                }
            },
            (IndexSizes::U16(a), IndexSizes::U16(b)) => {
                if let Some(result) = a.checked_add(b) {
                    IndexSizes::U16(result)
                } else {
                    IndexSizes::U32(a as u32 + b as u32)
                }
            },
            // Handle all other combinations by converting to u64 and then finding proper size
            _ => {
                let a = self.to_u64();
                let b = rhs.to_u64();
                IndexSizes::proper_u64(a + b)
            }
        }
    }
}

// Addition with primitive types
macro_rules! impl_add_primitive {
    ($($t:ty),*) => {
        $(
            impl Add<$t> for IndexSizes {
                type Output = Self;
                
                fn add(self, rhs: $t) -> Self::Output {
                    self + Self::from(rhs as u64)
                }
            }
            
            impl Add<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn add(self, rhs: IndexSizes) -> Self::Output {
                    IndexSizes::from(self as u64) + rhs
                }
            }
            
            impl AddAssign<$t> for IndexSizes {
                fn add_assign(&mut self, rhs: $t) {
                    *self = *self + Self::from(rhs as u64);
                }
            }
        )*
    };
}

impl_add_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl AddAssign for IndexSizes {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

// Subtraction
impl Sub for IndexSizes {
    type Output = Self;
    
    fn sub(self, rhs: Self) -> Self::Output {
        // For subtraction, we need to be careful about underflow
        let a = self.to_u64();
        let b = rhs.to_u64();
        
        if a < b {
            panic!("Attempt to subtract with underflow");
        }
        
        IndexSizes::proper_u64(a - b)
    }
}

// Subtraction with primitive types
macro_rules! impl_sub_primitive {
    ($($t:ty),*) => {
        $(
            impl Sub<$t> for IndexSizes {
                type Output = Self;
                
                fn sub(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    let b = rhs as u64;
                    
                    if a < b {
                        panic!("Attempt to subtract with underflow");
                    }
                    
                    IndexSizes::proper_u64(a - b)
                }
            }
            
            impl Sub<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn sub(self, rhs: IndexSizes) -> Self::Output {
                    let a = self as u64;
                    let b = rhs.to_u64();
                    
                    if a < b {
                        panic!("Attempt to subtract with underflow");
                    }
                    
                    IndexSizes::proper_u64(a - b)
                }
            }
            
            impl SubAssign<$t> for IndexSizes {
                fn sub_assign(&mut self, rhs: $t) {
                    *self = *self - rhs;
                }
            }
        )*
    };
}

impl_sub_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl SubAssign for IndexSizes {
    fn sub_assign(&mut self, rhs: Self) {
        *self = *self - rhs;
    }
}

// Multiplication
impl Mul for IndexSizes {
    type Output = Self;
    
    fn mul(self, rhs: Self) -> Self::Output {
        let a = self.to_u64();
        let b = rhs.to_u64();
        IndexSizes::proper_u64(a * b)
    }
}

// Multiplication with primitive types
macro_rules! impl_mul_primitive {
    ($($t:ty),*) => {
        $(
            impl Mul<$t> for IndexSizes {
                type Output = Self;
                
                fn mul(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    let b = rhs as u64;
                    IndexSizes::proper_u64(a * b)
                }
            }
            
            impl Mul<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn mul(self, rhs: IndexSizes) -> Self::Output {
                    let a = self as u64;
                    let b = rhs.to_u64();
                    IndexSizes::proper_u64(a * b)
                }
            }
            
            impl MulAssign<$t> for IndexSizes {
                fn mul_assign(&mut self, rhs: $t) {
                    *self = *self * rhs;
                }
            }
        )*
    };
}

impl_mul_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl MulAssign for IndexSizes {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

// Division
impl Div for IndexSizes {
    type Output = Self;
    
    fn div(self, rhs: Self) -> Self::Output {
        let b = rhs.to_u64();
        if b == 0 {
            panic!("Division by zero");
        }
        
        let a = self.to_u64();
        IndexSizes::proper_u64(a / b)
    }
}

// Division with primitive types
macro_rules! impl_div_primitive {
    ($($t:ty),*) => {
        $(
            impl Div<$t> for IndexSizes {
                type Output = Self;
                
                fn div(self, rhs: $t) -> Self::Output {
                    let b = rhs as u64;
                    if b == 0 {
                        panic!("Division by zero");
                    }
                    
                    let a = self.to_u64();
                    IndexSizes::proper_u64(a / b)
                }
            }
            
            impl Div<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn div(self, rhs: IndexSizes) -> Self::Output {
                    let b = rhs.to_u64();
                    if b == 0 {
                        panic!("Division by zero");
                    }
                    
                    let a = self as u64;
                    IndexSizes::proper_u64(a / b)
                }
            }
            
            impl DivAssign<$t> for IndexSizes {
                fn div_assign(&mut self, rhs: $t) {
                    *self = *self / rhs;
                }
            }
        )*
    };
}

impl_div_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl DivAssign for IndexSizes {
    fn div_assign(&mut self, rhs: Self) {
        *self = *self / rhs;
    }
}

// Remainder
impl Rem for IndexSizes {
    type Output = Self;
    
    fn rem(self, rhs: Self) -> Self::Output {
        let b = rhs.to_u64();
        if b == 0 {
            panic!("Division by zero");
        }
        
        let a = self.to_u64();
        IndexSizes::proper_u64(a % b)
    }
}

// Remainder with primitive types
macro_rules! impl_rem_primitive {
    ($($t:ty),*) => {
        $(
            impl Rem<$t> for IndexSizes {
                type Output = Self;
                
                fn rem(self, rhs: $t) -> Self::Output {
                    let b = rhs as u64;
                    if b == 0 {
                        panic!("Division by zero");
                    }
                    
                    let a = self.to_u64();
                    IndexSizes::proper_u64(a % b)
                }
            }
            
            impl Rem<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn rem(self, rhs: IndexSizes) -> Self::Output {
                    let b = rhs.to_u64();
                    if b == 0 {
                        panic!("Division by zero");
                    }
                    
                    let a = self as u64;
                    IndexSizes::proper_u64(a % b)
                }
            }
            
            impl RemAssign<$t> for IndexSizes {
                fn rem_assign(&mut self, rhs: $t) {
                    *self = *self % rhs;
                }
            }
        )*
    };
}

impl_rem_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl RemAssign for IndexSizes {
    fn rem_assign(&mut self, rhs: Self) {
        *self = *self % rhs;
    }
}

// From implementations for various integer types
impl From<u8> for IndexSizes {
    fn from(value: u8) -> Self {
        IndexSizes::U8(value)
    }
}

impl From<u16> for IndexSizes {
    fn from(value: u16) -> Self {
        if value <= u8::MAX as u16 {
            IndexSizes::U8(value as u8)
        } else {
            IndexSizes::U16(value)
        }
    }
}

impl From<u32> for IndexSizes {
    fn from(value: u32) -> Self {
        if value <= u8::MAX as u32 {
            IndexSizes::U8(value as u8)
        } else if value <= u16::MAX as u32 {
            IndexSizes::U16(value as u16)
        } else {
            IndexSizes::U32(value)
        }
    }
}

impl From<u64> for IndexSizes {
    fn from(value: u64) -> Self {
        IndexSizes::proper_u64(value)
    }
}

impl From<usize> for IndexSizes {
    fn from(value: usize) -> Self {
        IndexSizes::proper(value)
    }
}

// TryFrom implementations for conversion to primitive types
impl TryFrom<IndexSizes> for u8 {
    type Error = &'static str;
    
    fn try_from(value: IndexSizes) -> Result<Self, Self::Error> {
        match value {
            IndexSizes::U8(v) => Ok(v),
            _ => {
                let as_u64 = value.to_u64();
                if as_u64 <= u8::MAX as u64 {
                    Ok(as_u64 as u8)
                } else {
                    Err("Value too large for u8")
                }
            }
        }
    }
}

impl TryFrom<IndexSizes> for u16 {
    type Error = &'static str;
    
    fn try_from(value: IndexSizes) -> Result<Self, Self::Error> {
        match value {
            IndexSizes::U8(v) => Ok(v as u16),
            IndexSizes::U16(v) => Ok(v),
            _ => {
                let as_u64 = value.to_u64();
                if as_u64 <= u16::MAX as u64 {
                    Ok(as_u64 as u16)
                } else {
                    Err("Value too large for u16")
                }
            }
        }
    }
}

impl TryFrom<IndexSizes> for u32 {
    type Error = &'static str;
    
    fn try_from(value: IndexSizes) -> Result<Self, Self::Error> {
        match value {
            IndexSizes::U8(v) => Ok(v as u32),
            IndexSizes::U16(v) => Ok(v as u32),
            IndexSizes::U32(v) => Ok(v),
            _ => {
                let as_u64 = value.to_u64();
                if as_u64 <= u32::MAX as u64 {
                    Ok(as_u64 as u32)
                } else {
                    Err("Value too large for u32")
                }
            }
        }
    }
}

impl TryFrom<IndexSizes> for u64 {
    type Error = &'static str;
    
    fn try_from(value: IndexSizes) -> Result<Self, Self::Error> {
        Ok(value.to_u64())
    }
}

impl TryFrom<IndexSizes> for usize {
    type Error = &'static str;
    
    fn try_from(value: IndexSizes) -> Result<Self, Self::Error> {
        Ok(value.as_usize())
    }
}

// Display implementation for easy printing
impl fmt::Display for IndexSizes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexSizes::U8(v) => write!(f, "{}", v),
            IndexSizes::U16(v) => write!(f, "{}", v),
            IndexSizes::U32(v) => write!(f, "{}", v),
            IndexSizes::U64(v) => write!(f, "{}", v),
            IndexSizes::Usize(v) => write!(f, "{}", v),
        }
    }
}

// Implementation for common number-like traits
impl Default for IndexSizes {
    fn default() -> Self {
        IndexSizes::U8(0)
    }
}

// Implement zero and one constants
impl IndexSizes {
    pub const ZERO: IndexSizes = IndexSizes::U8(0);
    pub const ONE: IndexSizes = IndexSizes::U8(1);
    
    pub fn is_zero(&self) -> bool {
        match self {
            IndexSizes::U8(v) => *v == 0,
            IndexSizes::U16(v) => *v == 0,
            IndexSizes::U32(v) => *v == 0,
            IndexSizes::U64(v) => *v == 0,
            IndexSizes::Usize(v) => *v == 0,
        }
    }
    
    pub fn is_one(&self) -> bool {
        match self {
            IndexSizes::U8(v) => *v == 1,
            IndexSizes::U16(v) => *v == 1,
            IndexSizes::U32(v) => *v == 1,
            IndexSizes::U64(v) => *v == 1,
            IndexSizes::Usize(v) => *v == 1,
        }
    }
}

// Implement From<IndexSizes> for String for easy conversion to string
impl From<IndexSizes> for String {
    fn from(value: IndexSizes) -> Self {
        match value {
            IndexSizes::U8(v) => v.to_string(),
            IndexSizes::U16(v) => v.to_string(),
            IndexSizes::U32(v) => v.to_string(),
            IndexSizes::U64(v) => v.to_string(),
            IndexSizes::Usize(v) => v.to_string(),
        }
    }
}

// Implement FromStr for parsing strings into IndexSizes
impl std::str::FromStr for IndexSizes {
    type Err = &'static str;
    
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u64>() {
            Ok(value) => Ok(IndexSizes::proper_u64(value)),
            Err(_) => Err("Failed to parse string into IndexSizes"),
        }
    }
}

// Bit operations
impl std::ops::BitAnd for IndexSizes {
    type Output = Self;
    
    fn bitand(self, rhs: Self) -> Self::Output {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a & b)
    }
}

// BitAnd with primitive types
macro_rules! impl_bitand_primitive {
    ($($t:ty),*) => {
        $(
            impl std::ops::BitAnd<$t> for IndexSizes {
                type Output = Self;
                
                fn bitand(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    let b = rhs as u64;
                    Self::proper_u64(a & b)
                }
            }
            
            impl std::ops::BitAnd<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn bitand(self, rhs: IndexSizes) -> Self::Output {
                    let a = self as u64;
                    let b = rhs.to_u64();
                    IndexSizes::proper_u64(a & b)
                }
            }
            
            impl std::ops::BitAndAssign<$t> for IndexSizes {
                fn bitand_assign(&mut self, rhs: $t) {
                    *self = *self & rhs;
                }
            }
        )*
    };
}

impl_bitand_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl std::ops::BitAndAssign for IndexSizes {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

impl std::ops::BitOr for IndexSizes {
    type Output = Self;
    
    fn bitor(self, rhs: Self) -> Self::Output {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a | b)
    }
}

// BitOr with primitive types
macro_rules! impl_bitor_primitive {
    ($($t:ty),*) => {
        $(
            impl std::ops::BitOr<$t> for IndexSizes {
                type Output = Self;
                
                fn bitor(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    let b = rhs as u64;
                    Self::proper_u64(a | b)
                }
            }
            
            impl std::ops::BitOr<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn bitor(self, rhs: IndexSizes) -> Self::Output {
                    let a = self as u64;
                    let b = rhs.to_u64();
                    IndexSizes::proper_u64(a | b)
                }
            }
            
            impl std::ops::BitOrAssign<$t> for IndexSizes {
                fn bitor_assign(&mut self, rhs: $t) {
                    *self = *self | rhs;
                }
            }
        )*
    };
}

impl_bitor_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl std::ops::BitOrAssign for IndexSizes {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

impl std::ops::BitXor for IndexSizes {
    type Output = Self;
    
    fn bitxor(self, rhs: Self) -> Self::Output {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a ^ b)
    }
}

// BitXor with primitive types
macro_rules! impl_bitxor_primitive {
    ($($t:ty),*) => {
        $(
            impl std::ops::BitXor<$t> for IndexSizes {
                type Output = Self;
                
                fn bitxor(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    let b = rhs as u64;
                    Self::proper_u64(a ^ b)
                }
            }
            
            impl std::ops::BitXor<IndexSizes> for $t {
                type Output = IndexSizes;
                
                fn bitxor(self, rhs: IndexSizes) -> Self::Output {
                    let a = self as u64;
                    let b = rhs.to_u64();
                    IndexSizes::proper_u64(a ^ b)
                }
            }
            
            impl std::ops::BitXorAssign<$t> for IndexSizes {
                fn bitxor_assign(&mut self, rhs: $t) {
                    *self = *self ^ rhs;
                }
            }
        )*
    };
}

impl_bitxor_primitive!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl std::ops::BitXorAssign for IndexSizes {
    fn bitxor_assign(&mut self, rhs: Self) {
        *self = *self ^ rhs;
    }
}

impl std::ops::Shl<u32> for IndexSizes {
    type Output = Self;
    
    fn shl(self, rhs: u32) -> Self::Output {
        let a = self.to_u64();
        Self::proper_u64(a << rhs)
    }
}
impl IndexSizes {
    pub fn as_u8(&self) -> u8 {
        self.to_u64() as u8
    }
    
    pub fn as_u16(&self) -> u16 {
        self.to_u64() as u16
    }
    
    pub fn as_u32(&self) -> u32 {
        self.to_u64() as u32
    }
    
    pub fn as_u64(&self) -> u64 {
        self.to_u64()
    }
}
// Add support for other integer types with shift operations
macro_rules! impl_shift_primitive {
    ($($t:ty),*) => {
        $(
            impl std::ops::Shl<$t> for IndexSizes {
                type Output = Self;
                
                fn shl(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    Self::proper_u64(a << (rhs as u32))
                }
            }
            
            impl std::ops::ShlAssign<$t> for IndexSizes {
                fn shl_assign(&mut self, rhs: $t) {
                    *self = *self << (rhs as u32);
                }
            }
            
            impl std::ops::Shr<$t> for IndexSizes {
                type Output = Self;
                
                fn shr(self, rhs: $t) -> Self::Output {
                    let a = self.to_u64();
                    Self::proper_u64(a >> (rhs as u32))
                }
            }
            
            impl std::ops::ShrAssign<$t> for IndexSizes {
                fn shr_assign(&mut self, rhs: $t) {
                    *self = *self >> (rhs as u32);
                }
            }
        )*
    };
}

impl_shift_primitive!(u8, u16, u64, usize, i8, i16, i32, i64, isize);

impl std::ops::ShlAssign<u32> for IndexSizes {
    fn shl_assign(&mut self, rhs: u32) {
        *self = *self << rhs;
    }
}

impl std::ops::Shr<u32> for IndexSizes {
    type Output = Self;
    
    fn shr(self, rhs: u32) -> Self::Output {
        let a = self.to_u64();
        Self::proper_u64(a >> rhs)
    }
}

impl std::ops::ShrAssign<u32> for IndexSizes {
    fn shr_assign(&mut self, rhs: u32) {
        *self = *self >> rhs;
    }
}

// Additional convenience methods
impl IndexSizes {
    pub fn pow(self, exp: u32) -> Self {
        let base = self.to_u64();
        Self::proper_u64(base.pow(exp))
    }
    
    pub fn saturating_add(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.saturating_add(b))
    }
    
    pub fn saturating_sub(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.saturating_sub(b))
    }
    
    pub fn saturating_mul(self, rhs: Self) -> Self {
        let a = self.to_u64();
        let b = rhs.to_u64();
        Self::proper_u64(a.saturating_mul(b))
    }
}