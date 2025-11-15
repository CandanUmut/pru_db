use crate::consts::ATOM_ID_BYTES;

#[derive(Debug, Clone, Copy)]
pub enum KeyKind { S, P, O, SP, PO, SO }

#[derive(Debug, Clone)]
pub struct ResolverKey(pub Vec<u8>); // 1-byte prefix + 16 or 32 bytes

impl ResolverKey {
    pub fn single(kind: KeyKind, a: &[u8;ATOM_ID_BYTES]) -> Self {
        let tag = match kind { KeyKind::S=>0x10, KeyKind::P=>0x11, KeyKind::O=>0x12, _=>panic!("pair req") };
        let mut v = Vec::with_capacity(1+ATOM_ID_BYTES);
        v.push(tag); v.extend_from_slice(a); Self(v)
    }
    pub fn pair(kind: KeyKind, a: &[u8;ATOM_ID_BYTES], b: &[u8;ATOM_ID_BYTES]) -> Self {
        let tag = match kind { KeyKind::SP=>0x13, KeyKind::PO=>0x14, KeyKind::SO=>0x15, _=>panic!("pair kind") };
        let mut v = Vec::with_capacity(1+ATOM_ID_BYTES*2);
        v.push(tag); v.extend_from_slice(a); v.extend_from_slice(b); Self(v)
    }
}
