type Paper = (u8, u64, Vec<u8>);

#[derive(Default)]
pub struct BurningMap {
    paper_vector: Vec<Paper>,
    capacity: u64,
}

fn hash(x: u64) -> u64 {
    let mut x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

impl BurningMap {
    pub fn new() -> Self {
        return BurningMap::default();
    }
    pub fn capacity(&mut self, capacity: u64) {
        self.capacity = capacity;
        self.paper_vector.clear();
        self.paper_vector = Vec::with_capacity(capacity as usize);
        for _ in 0..self.capacity {
            self.paper_vector.push((0, 0, Vec::with_capacity(0)));
        }
    }
    pub fn add(&mut self, key: u64, value: Vec<u8>) {
        let val = &mut self.paper_vector[(hash(key) % self.capacity) as usize];
        if val.0 > 0 && val.1 != key {
            val.0 -= 1;
            return;
        };
        let mut value = value;
        value.shrink_to_fit();
        if val.0 == 0 || val.1 == key {
            val.0 += 1;
            val.1 = key;
            val.2 = value;
        }
    }
    pub fn get(&mut self, key: u64, buffer: &mut Vec<u8>) -> bool {
        let val = &mut self.paper_vector[(hash(key) % self.capacity) as usize];
        if val.0 > 0 {
            val.0 -= 1;
            return false;
        };
        if val.1 == key && val.0 > 0 {
            val.0 += 1;
            buffer.copy_from_slice(&val.2);
            return true;
        }
        return false;
    }
    pub fn deplete(&mut self, key: u64) {
        let val = &mut self.paper_vector[(hash(key) % self.capacity) as usize];
        if val.1 == key {
            val.0 = 0;
            val.2.clear();
        }
    }
}
