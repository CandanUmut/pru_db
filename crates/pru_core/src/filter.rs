//! Minimal, stable Bloom filter for read-only segments (advisory).
#[derive(Clone, Debug)]
pub struct Bloom {
    pub m_bits: u32,
    pub k: u32,
    pub bits: Vec<u8>,
}

impl Bloom {
    pub fn new(m_bits: u32, k: u32) -> Self {
        let bytes = ((m_bits as usize) + 7) / 8;
        Self { m_bits, k: k.max(1), bits: vec![0u8; bytes] }
    }
    pub fn from_bytes(k: u32, bits: Vec<u8>) -> Self {
        let m_bits = (bits.len() * 8) as u32;
        Self { m_bits, k: k.max(1), bits }
    }
    #[inline]
    fn hashes(&self, key: &[u8]) -> impl Iterator<Item=u32> {
        let dig = blake3::hash(key).as_bytes().clone();
        let h1 = u64::from_le_bytes(dig[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(dig[8..16].try_into().unwrap());
        let m = self.m_bits as u64;
        (0..self.k).map(move |i| ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % m) as u32)
    }
    pub fn add(&mut self, key: &[u8]) {
        if self.m_bits == 0 { return; }
        for bit in self.hashes(key) {
            let idx = (bit / 8) as usize; let off = (bit & 7) as u8;
            self.bits[idx] |= 1u8 << off;
        }
    }
    pub fn contains(&self, key: &[u8]) -> bool {
        if self.m_bits == 0 { return true; }
        self.hashes(key).all(|bit| {
            let idx = (bit / 8) as usize; let off = (bit & 7) as u8;
            (self.bits[idx] & (1u8 << off)) != 0
        })
    }
}
