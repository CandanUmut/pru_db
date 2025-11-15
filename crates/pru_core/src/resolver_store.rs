use crate::errors::Result;
use crate::manifest::Manifest;
use crate::postings::{decode_sorted_u64, intersect_sorted, merge_sorted};
use crate::segment::SegmentReader;
use crate::consts::SegmentKind;
use std::path::Path;

pub enum ResolveMode {
    Union,
    Dedup,
    Intersect,
}

pub struct ResolverStore {
    readers: Vec<SegmentReader>, // yalnızca AKTİF resolver segmentleri
}

impl ResolverStore {
    pub fn open(dir: &Path) -> Result<Self> {
        let man = Manifest::load(dir)?;
        let active = man.active_segment_paths();
        let mut readers = Vec::new();
        for p in active {
            if let Some(rec) = man.segments.iter().find(|s| s.path == p) {
                if rec.kind != SegmentKind::Resolver { continue; }
            }
            let full = dir.join(&p);
            if full.exists() {
                if let Ok(r) = SegmentReader::open(&full) {
                    if r.kind == SegmentKind::Resolver { readers.push(r); }
                }
            }
        }
        if readers.is_empty() {
            for s in &man.segments {
                if s.kind != SegmentKind::Resolver { continue; }
                let full = dir.join(&s.path);
                if let Ok(r) = SegmentReader::open(&full) { readers.push(r); }
            }
        }
        Ok(Self { readers })
    }

    pub fn resolve(&self, key: &[u8]) -> Vec<u64> {
        let mut out: Vec<u64> = Vec::new();
        for r in &self.readers {
            if let Some(v) = r.get(key) {
                let mut v = decode_sorted_u64(v);
                out = merge_sorted(&out, &v);
            }
        }
        out
    }

    pub fn resolve_with_mode(&self, mode: ResolveMode, keys: &[Vec<u8>]) -> Vec<u64> {
        self.resolve_with_mode_set(mode, keys, false)
    }

    /// set_semantics=true iken, INTERSECT işleminde operandlar önce dedup edilir (set-kesişimi).
    pub fn resolve_with_mode_set(&self, mode: ResolveMode, keys: &[Vec<u8>], set_semantics: bool) -> Vec<u64> {
        match mode {
            ResolveMode::Union => {
                let mut acc: Vec<u64> = Vec::new();
                for k in keys {
                    let v = self.resolve(k);
                    acc = merge_sorted(&acc, &v);
                }
                acc
            }
            ResolveMode::Dedup => {
                let mut acc: Vec<u64> = Vec::new();
                for k in keys {
                    let v = self.resolve(k);
                    acc = merge_sorted(&acc, &v);
                }
                acc.dedup();
                acc
            }
            ResolveMode::Intersect => {
                if keys.is_empty() { return vec![]; }
                let mut acc = self.resolve(&keys[0]);
                if set_semantics { acc.dedup(); }
                for k in &keys[1..] {
                    let mut v = self.resolve(k);
                    if set_semantics { v.dedup(); }
                    acc = intersect_sorted(&acc, &v);
                    if acc.is_empty() { break; }
                }
                acc
            }
        }
    }
}
