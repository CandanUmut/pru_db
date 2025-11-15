use crate::consts::ATOM_ID_BYTES;

pub type AtomId = [u8; ATOM_ID_BYTES];

pub fn atom_id128(bytes: &[u8]) -> AtomId {
    let h = blake3::hash(bytes);               // 32 byte
    let b = h.as_bytes();
    let mut out = [0u8; ATOM_ID_BYTES];        // 16 byte
    out.copy_from_slice(&b[..ATOM_ID_BYTES]);  // ilk 16 baytı al
    out
}
