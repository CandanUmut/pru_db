use crate::utils::{uvarint_encode, uvarint_decode};

pub fn encode_sorted_u64(nums: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nums.len() * 2);
    let mut prev = 0u64;
    for &n in nums { let d = n - prev; prev = n; uvarint_encode(d, &mut out); }
    out
}

pub fn decode_sorted_u64(buf: &[u8]) -> Vec<u64> {
    let mut res = Vec::new();
    let mut prev = 0u64; let mut cur = buf;
    while !cur.is_empty() { let (d, rest) = uvarint_decode(cur); cur = rest; prev += d; res.push(prev); }
    res
}

pub fn merge_sorted(a: &[u64], b: &[u64]) -> Vec<u64> {
    let (mut i, mut j) = (0usize, 0usize);
    let mut out = Vec::with_capacity(a.len()+b.len());
    while i<a.len() || j<b.len() {
        if j==b.len() || (i<a.len() && a[i] <= b[j]) { out.push(a[i]); i+=1; } else { out.push(b[j]); j+=1; }
    }
    out
}

pub fn intersect_sorted(a: &[u64], b: &[u64]) -> Vec<u64> {
    let (mut i, mut j) = (0usize, 0usize);
    let mut out = Vec::new();
    while i<a.len() && j<b.len() {
        if a[i]==b[j] { out.push(a[i]); i+=1; j+=1; }
        else if a[i]<b[j] { i+=1; } else { j+=1; }
    }
    out
}
