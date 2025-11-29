pub mod atoms;
pub mod consts;
pub mod errors;
pub mod filter;
pub mod manifest;
pub mod postings;
pub mod resolver;
pub mod resolver_store;
pub mod segment;
pub mod truth_store;
pub mod utils;

pub use atoms::{atom_id128, AtomHash, AtomId, EntityId, LiteralId, PredicateId};
pub use consts::SegmentKind;
pub use postings::{decode_sorted_u64, encode_sorted_u64, intersect_sorted, merge_sorted};
pub use resolver::{KeyKind, ResolverKey};
pub use resolver_store::ResolveMode; // ← ek
pub use segment::{SegmentReader, SegmentWriter};
pub use truth_store::{Fact, PruStore, Query};

use std::sync::{Arc, Mutex};

/// Shared handle type used by higher-level crates when coordinating access to a
/// [`PruStore`]. The store itself is not thread-safe; wrapping it in a mutex
/// makes it usable across async contexts and HTTP handlers.
pub type PruDbHandle = Arc<Mutex<PruStore>>;
