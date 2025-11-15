use crate::consts::ATOM_ID_BYTES;

/// Stable, opaque integer identifier used by the higher-level store APIs.
pub type AtomId = u64;

/// Convenience aliases for typed atom identifiers.
pub type EntityId = AtomId;
pub type PredicateId = AtomId;
pub type LiteralId = AtomId;

/// Legacy 128-bit (truncated) atom digest used by the resolver layer.
pub type AtomHash = [u8; ATOM_ID_BYTES];

/// Compute a truncated BLAKE3 digest for use in resolver keys.
pub fn atom_id128(bytes: &[u8]) -> AtomHash {
    let h = blake3::hash(bytes); // 32 byte
    let b = h.as_bytes();
    let mut out = [0u8; ATOM_ID_BYTES]; // 16 byte
    out.copy_from_slice(&b[..ATOM_ID_BYTES]); // ilk 16 baytı al
    out
}
