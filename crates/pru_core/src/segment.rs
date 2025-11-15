//! Segment file format & IO (V1/V2 index + Bloom/XOR filter)
//!
//! Header (LE, 48 bytes):
//!   magic[4] = "PRUS"
//!   version[2] = 1
//!   kind[2]    = SegmentKind (1=dict,2=fact,3=resolver)
//!   rsv[4]     = 0
//!   idx_off[8] = index block offset
//!   flt_off[8] = filter block offset
//!   data_off[8]= data start (şimdilik HDR_SIZE)
//!   foot_off[8]= footer offset (şimdilik dosya sonu)
//!
//! Index V1 (kind=1):
//!   u32 kind
//!   u64 cap (power-of-two)
//!   repeat cap * { u64 h, u64 off, u32 size, u32 pad }
//!
//! Index V2 (kind=2)  ← default yazım
//!   u32 kind
//!   u64 cap
//!   repeat cap * { u64 h, u64 fp, u64 off, u32 size, u32 pad }
//!
//! Filter block:
//!   Legacy Bloom:
//!     [u32 k][u32 blen][bytes]
//!   XOR8 (yeni):
//!     [u32 tag="XOR8"][u32 len][bytes = xorfilter::Xor8::to_bytes()]
//!
//! Value kaydı: [value bytes][crc32(value)]

use crate::consts::{MAGIC_SEG, VERSION, HDR_SIZE, INDEX_KIND_HASHTAB, SegmentKind};
use crate::errors::{PruError, Result};
use crate::filter::Bloom;
use crate::utils::{crc32, write_u32};
use memmap2::Mmap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tempfile::NamedTempFile;
use xorfilter::Xor8;

const INDEX_KIND_HASHTAB_V1: u32 = INDEX_KIND_HASHTAB; // 1
const INDEX_KIND_HASHTAB_V2: u32 = 2; // yeni: hash+fingerprint
const FILTER_TAG_XOR8: u32 = u32::from_le_bytes(*b"XOR8");

#[inline]
fn h64(key: &[u8]) -> u64 { xxhash_rust::xxh3::xxh3_64(key) }

#[inline]
fn fp64(key: &[u8]) -> u64 {
    let b = blake3::hash(key);
    u64::from_le_bytes(b.as_bytes()[0..8].try_into().unwrap())
}

#[cfg(unix)]
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = path.parent().unwrap_or(Path::new("."));
    let f = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECTORY).open(dir)?;
    f.sync_all()
}
#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> std::io::Result<()> { Ok(()) }

#[derive(Clone, Copy)]
enum FilterKind { Bloom, Xor8 }

/// Writer: append-only; index & filter bloklarını yazar, sonra atomik publish (Windows-safe).
pub struct SegmentWriter {
    path_final: PathBuf,
    tmp: NamedTempFile,
    kind: SegmentKind,
    // in-memory tablo: (hash, fp, off, size)
    items: Vec<(u64, u64, u64, u32)>,
    bloom: Bloom,
    index_kind: u32,      // V1/V2 (default V2)
    filter_kind: FilterKind, // default XOR8
}

impl SegmentWriter {
    /// Yeni segment (publish edilmeden)
    pub fn create(path: impl AsRef<Path>, kind: SegmentKind, bloom_bits: u32, bloom_k: u32) -> Result<Self> {
        let path_final = path.as_ref().to_path_buf();
        let dir = path_final.parent().unwrap_or(Path::new("."));
        let mut tmp = tempfile::Builder::new().prefix("pru_seg_").tempfile_in(dir)?;
        tmp.as_file_mut().write_all(&vec![0u8; HDR_SIZE])?; // header yeri
        Ok(Self{
            path_final,
            tmp,
            kind,
            items: Vec::new(),
            bloom: Bloom::new(bloom_bits, bloom_k),
            index_kind: INDEX_KIND_HASHTAB_V2,
            filter_kind: FilterKind::Xor8, // varsayılan: XOR8
        })
    }

    /// İndeks türünü seç (geri uyum veya compact için)
    pub fn set_index_kind(&mut self, kind: u32) { self.index_kind = kind; }
    pub fn set_filter_xor8(&mut self) { self.filter_kind = FilterKind::Xor8; }
    pub fn set_filter_bloom(&mut self) { self.filter_kind = FilterKind::Bloom; }

    /// (key,value) kaydı ekle. Value sonuna crc32(value).
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let f = self.tmp.as_file_mut();
        let off = f.seek(SeekFrom::End(0))? as u64;
        f.write_all(value)?;
        write_u32(f, crc32(value))?;
        let size = (f.seek(SeekFrom::End(0))? as u64 - off) as u32;
        self.items.push((h64(key), fp64(key), off, size));
        // Bloom için advisory set; XOR8 için gerekmiyor ama zararı yok
        self.bloom.add(key);
        Ok(())
    }

    /// Compact için: hazır hash ile ekle (key byteları olmadan).
    /// Not: V2 indeks fingerprint'i key'den üretildiği için bu fonksiyonu kullanırken V1 indeks seçin.
    pub fn add_hashed(&mut self, hash: u64, value: &[u8]) -> Result<()> {
        let f = self.tmp.as_file_mut();
        let off = f.seek(SeekFrom::End(0))? as u64;
        f.write_all(value)?;
        write_u32(f, crc32(value))?;
        let size = (f.seek(SeekFrom::End(0))? as u64 - off) as u32;
        // fingerprint'i 0 bırakıyoruz; V1 indeks ile yazın.
        self.items.push((hash, 0, off, size));
        Ok(())
    }

    fn build_hashtable_v1(&self) -> (u64, Vec<u8>) {
        // entry: h(8), off(8), size(4), pad(4) = 24
        let n = self.items.len() as u64;
        let mut cap = 1u64;
        while cap < (n * 5) / 4 + 1 { cap <<= 1 } // ≈0.8 LF
        let mut table: Vec<(u64, u64, u32)> = vec![(0,0,0); cap as usize];
        for (h, _fp, off, size) in &self.items {
            let mut idx = h & (cap - 1);
            loop {
                if table[idx as usize].0 == 0 {
                    table[idx as usize] = (*h, *off, *size);
                    break;
                }
                idx = (idx + 1) & (cap - 1);
            }
        }
        let mut buf = Vec::with_capacity(12 + (cap as usize) * (8+8+4+4));
        buf.extend_from_slice(&INDEX_KIND_HASHTAB_V1.to_le_bytes());
        buf.extend_from_slice(&cap.to_le_bytes());
        for (h, off, size) in table {
            buf.extend_from_slice(&h.to_le_bytes());
            buf.extend_from_slice(&off.to_le_bytes());
            buf.extend_from_slice(&size.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes()); // pad
        }
        (cap, buf)
    }

    fn build_hashtable_v2(&self) -> (u64, Vec<u8>) {
        // entry: h(8), fp(8), off(8), size(4), pad(4) = 32
        let n = self.items.len() as u64;
        let mut cap = 1u64;
        while cap < (n * 5) / 4 + 1 { cap <<= 1 }
        let mut table: Vec<(u64, u64, u64, u32)> = vec![(0,0,0,0); cap as usize];
        for (h, fp, off, size) in &self.items {
            let mut idx = h & (cap - 1);
            loop {
                if table[idx as usize].0 == 0 {
                    table[idx as usize] = (*h, *fp, *off, *size);
                    break;
                }
                idx = (idx + 1) & (cap - 1);
            }
        }
        let mut buf = Vec::with_capacity(12 + (cap as usize) * (8+8+8+4+4));
        buf.extend_from_slice(&INDEX_KIND_HASHTAB_V2.to_le_bytes());
        buf.extend_from_slice(&cap.to_le_bytes());
        for (h, fp, off, size) in table {
            buf.extend_from_slice(&h.to_le_bytes());
            buf.extend_from_slice(&fp.to_le_bytes());
            buf.extend_from_slice(&off.to_le_bytes());
            buf.extend_from_slice(&size.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
        (cap, buf)
    }

    /// finalize: index + filter + header, sonra atomik publish
        /// finalize: index + filter + header, sonra atomik publish
    pub fn finalize(mut self) -> Result<PathBuf> {
        // 1) Index offset'i belirle (kısa borrow scope)
        let index_off = {
            let f = self.tmp.as_file_mut();
            f.seek(SeekFrom::End(0))? as u64
        };

        // 1.a) Index bytes'larını MUT borrow olmadan hesapla
        let (_cap, idx_bytes) = match self.index_kind {
            INDEX_KIND_HASHTAB_V1 => self.build_hashtable_v1(),
            _ => self.build_hashtable_v2(),
        };

        // 1.b) Index'i yaz (yeni kısa borrow)
        {
            let f = self.tmp.as_file_mut();
            f.write_all(&idx_bytes)?;
        }

        // 2) Filter bloğu yaz
        let bloom_off = {
            let f = self.tmp.as_file_mut();
            f.seek(SeekFrom::End(0))? as u64
        };

        match self.filter_kind {
            FilterKind::Bloom => {
                let f = self.tmp.as_file_mut();
                write_u32(f, self.bloom.k)?;
                write_u32(f, self.bloom.bits.len() as u32)?;
                f.write_all(&self.bloom.bits)?;
            }
            FilterKind::Xor8 => {
                // bytes'ı önce hazırla (borrow yok)
                let mut digests: Vec<u64> = self.items.iter().map(|(h,_,_,_)| *h).collect();
                digests.sort_unstable();
                digests.dedup();
                let mut xf: Xor8 = Xor8::new();
                xf.build_keys(&digests).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("xor build: {e:?}"))
                })?;
                let bytes = xf.to_bytes();

                // sonra yaz (kısa borrow)
                let f = self.tmp.as_file_mut();
                f.write_all(&FILTER_TAG_XOR8.to_le_bytes())?;
                write_u32(f, bytes.len() as u32)?;
                f.write_all(&bytes)?;
            }
        }

        // 3) Footer (şimdilik sadece son ofset)
        let footer_off = {
            let f = self.tmp.as_file_mut();
            f.seek(SeekFrom::End(0))? as u64
        };

        // 4) Header'ı yaz
        {
            let f = self.tmp.as_file_mut();
            f.seek(SeekFrom::Start(0))?;
            let mut hdr = Vec::with_capacity(HDR_SIZE);
            hdr.extend_from_slice(MAGIC_SEG);
            hdr.extend_from_slice(&VERSION.to_le_bytes());
            hdr.extend_from_slice(&(self.kind as u16).to_le_bytes());
            hdr.extend_from_slice(&0u32.to_le_bytes());
            hdr.extend_from_slice(&index_off.to_le_bytes());
            hdr.extend_from_slice(&bloom_off.to_le_bytes());
            hdr.extend_from_slice(&(HDR_SIZE as u64).to_le_bytes());
            hdr.extend_from_slice(&footer_off.to_le_bytes());
            hdr.resize(HDR_SIZE, 0);
            f.write_all(&hdr)?;
            f.sync_all()?; // diske yaz
        }

        // 5) Atomic publish (Windows-safe)
        let _persisted = self.tmp.persist(&self.path_final)?;
        let _ = fsync_dir(&self.path_final);
        Ok(self.path_final)
    }

}

/// Reader: V1/V2 index + Bloom/XOR filter okur, iterator & verify yardımcıları sağlar.
pub struct SegmentReader {
    _f: File,
    mmap: Mmap,
    pub kind: SegmentKind,
    index_off: u64,
    bloom_off: u64,
    filter_cache: OnceLock<FilterCache>,
}

enum FilterCache {
    Bloom { k: u32, bits: Vec<u8> },
    Xor8(Xor8),
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::open(path)?;
        let mmap = unsafe { Mmap::map(&f)? };
        if &mmap[0..4] != MAGIC_SEG { return Err(PruError::BadHeader); }
        let ver = u16::from_le_bytes(mmap[4..6].try_into().unwrap());
        if ver != VERSION { return Err(PruError::BadHeader); }
        let kind = u16::from_le_bytes(mmap[6..8].try_into().unwrap());
        let kind = match kind { 1=>SegmentKind::Dict, 2=>SegmentKind::Fact, 3=>SegmentKind::Resolver, _=>return Err(PruError::Unsupported) };
        let index_off = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        let bloom_off = u64::from_le_bytes(mmap[20..28].try_into().unwrap());
        Ok(Self{ _f: f, mmap, kind, index_off, bloom_off, filter_cache: OnceLock::new() })
    }

    fn ensure_filter(&self) -> &FilterCache {
        self.filter_cache.get_or_init(|| {
            // XOR8 tag mı?
            let tag = u32::from_le_bytes(self.mmap[self.bloom_off as usize .. self.bloom_off as usize + 4].try_into().unwrap());
            if tag == FILTER_TAG_XOR8 {
                let len = u32::from_le_bytes(self.mmap[self.bloom_off as usize + 4 .. self.bloom_off as usize + 8].try_into().unwrap()) as usize;
                let bytes = self.mmap[(self.bloom_off as usize + 8)..(self.bloom_off as usize + 8 + len)].to_vec();
                let xf = Xor8::from_bytes(bytes).unwrap_or_else(|_| Xor8::new()); // worst-case empty
                FilterCache::Xor8(xf)
            } else {
                // legacy Bloom: [k][blen][bits...]
                let k = tag;
                let blen = u32::from_le_bytes(self.mmap[self.bloom_off as usize + 4 .. self.bloom_off as usize + 8].try_into().unwrap()) as usize;
                let bits = self.mmap[(self.bloom_off as usize + 8)..(self.bloom_off as usize + 8 + blen)].to_vec();
                FilterCache::Bloom { k, bits }
            }
        })
    }

    #[inline]
    fn filter_allows_key(&self, key: &[u8]) -> bool {
        match self.ensure_filter() {
            FilterCache::Bloom { k, bits } => {
                let bloom = Bloom::from_bytes(*k, bits.clone());

                bloom.contains(key)
            }
            FilterCache::Xor8(xf) => {
                let d = h64(key);
                xf.contains_key(d)
            }
        }
    }

    /// Sadece XOR filtre için: hazır digest üyeliği test et
    pub fn filter_contains_digest(&self, digest: u64) -> Option<bool> {
        match self.ensure_filter() {
            FilterCache::Xor8(xf) => Some(xf.contains_key(digest)),
            _ => None,
        }
    }

    /// İndeks başlığı (kind, cap, entries_base, entry_size)
    fn index_info(&self) -> (u32, u64, usize, usize) {
        let mut pos = self.index_off as usize;
        let kind = u32::from_le_bytes(self.mmap[pos..pos+4].try_into().unwrap()); pos+=4;
        let cap  = u64::from_le_bytes(self.mmap[pos..pos+8].try_into().unwrap()); pos+=8;
        let esz = match kind {
            INDEX_KIND_HASHTAB_V1 => 8 + 8 + 4 + 4,
            INDEX_KIND_HASHTAB_V2 => 8 + 8 + 8 + 4 + 4,
            _ => 0,
        };
        (kind, cap, pos, esz)
    }

    /// Tekil get (crc hariç dilim). Bulamazsa None.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        if !self.filter_allows_key(key) { return None; }
        let (kind, cap, base, esz) = self.index_info();
        if esz == 0 || cap == 0 { return None; }
        let h = h64(key);
        let fp = fp64(key);
        let mut idx = (h & (cap-1)) as usize;
        for _ in 0..cap {
            let epos = base + idx * esz;
            let eh = u64::from_le_bytes(self.mmap[epos..epos+8].try_into().unwrap());
            if eh == 0 { return None; }
            match kind {
                INDEX_KIND_HASHTAB_V1 => {
                    if eh == h {
                        let off = u64::from_le_bytes(self.mmap[epos+8..epos+16].try_into().unwrap()) as usize;
                        let size = u32::from_le_bytes(self.mmap[epos+16..epos+20].try_into().unwrap()) as usize;
                        let end = off + size;
                        return Some(&self.mmap[off..end-4]);
                    }
                }
                INDEX_KIND_HASHTAB_V2 => {
                    let efp = u64::from_le_bytes(self.mmap[epos+8..epos+16].try_into().unwrap());
                    if eh == h && efp == fp {
                        let off = u64::from_le_bytes(self.mmap[epos+16..epos+24].try_into().unwrap()) as usize;
                        let size = u32::from_le_bytes(self.mmap[epos+24..epos+28].try_into().unwrap()) as usize;
                        let end = off + size;
                        return Some(&self.mmap[off..end-4]);
                    }
                }
                _ => return None,
            }
            idx = (idx + 1) & ((cap as usize) - 1);
        }
        None
    }

    pub fn index_meta(&self) -> Option<(u32, u64)> {
        let (kind, cap, _base, esz) = self.index_info();
        if esz == 0 { None } else { Some((kind, cap)) }
    }

    /// [value][crc] kaydını crc32 ile doğrula.
    pub fn verify_crc_at(&self, off: usize, size: usize) -> bool {
        let end = off + size;
        if end > self.mmap.len() || size < 4 { return false; }
        let val = &self.mmap[off..end-4];
        let want = u32::from_le_bytes(self.mmap[end-4..end].try_into().unwrap());
        crc32(val) == want
    }

    /// Kayıt payload (crc hariç)
    pub fn value_at(&self, off: usize, size: usize) -> Option<&[u8]> {
        let end = off.checked_add(size)?;
        if end < 4 || end > self.mmap.len() { return None; }
        Some(&self.mmap[off..end-4])
    }

    /// İndeks üzerinde dolaşan iterator (V1/V2 farklarını soyutlar).
    pub fn iter(&self) -> IndexIter<'_> {
        let (kind, cap, base, esz) = self.index_info();
        IndexIter { rdr: self, kind, cap, base, esz, i: 0 }
    }
}

/// Index girdisi (V1’de fingerprint None)
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry { pub hash: u64, pub fingerprint: Option<u64>, pub off: u64, pub size: u32 }

pub struct IndexIter<'a> { rdr: &'a SegmentReader, kind: u32, cap: u64, base: usize, esz: usize, i: u64 }

impl<'a> Iterator for IndexIter<'a> {
    type Item = IndexEntry;
    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.cap {
            let epos = self.base + (self.i as usize) * self.esz;
            self.i += 1;
            let eh = u64::from_le_bytes(self.rdr.mmap[epos..epos+8].try_into().ok()?);
            if eh == 0 { continue; }
            return match self.kind {
                INDEX_KIND_HASHTAB_V1 => {
                    let off = u64::from_le_bytes(self.rdr.mmap[epos+8..epos+16].try_into().ok()?);
                    let size = u32::from_le_bytes(self.rdr.mmap[epos+16..epos+20].try_into().ok()?);
                    Some(IndexEntry{ hash: eh, fingerprint: None, off, size })
                }
                INDEX_KIND_HASHTAB_V2 => {
                    let efp = u64::from_le_bytes(self.rdr.mmap[epos+8..epos+16].try_into().ok()?);
                    let off = u64::from_le_bytes(self.rdr.mmap[epos+16..epos+24].try_into().ok()?);
                    let size = u32::from_le_bytes(self.rdr.mmap[epos+24..epos+28].try_into().ok()?);
                    Some(IndexEntry{ hash: eh, fingerprint: Some(efp), off, size })
                }
                _ => None
            };
        }
        None
    }
}
