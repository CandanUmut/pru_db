// crates/pru_core/src/consts.rs

use core::mem::size_of;

pub const MAGIC_SEG: &[u8;4] = b"PRUS";
pub const VERSION: u16 = 1;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentKind {
    Dict   = 1,   // atoms dictionary (id↔value)
    Fact   = 2,   // fact log (reserved)
    Resolver = 3, // resolver postings
}

pub const HDR_SIZE: usize = 48;
pub const IDX_ENTRY_SIZE: usize = 24;

pub const ATOM_ID_BYTES: usize = 16;

pub const INDEX_KIND_LINEAR: u32 = 0;
pub const INDEX_KIND_HASHTAB: u32 = 1;

const _: () = { assert!(size_of::<[u8;4]>() == 4); };
