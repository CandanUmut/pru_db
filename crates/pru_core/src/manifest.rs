use crate::consts::SegmentKind;
use crate::errors::{Result, PruError};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRec {
    pub kind: SegmentKind,
    #[serde(with = "path_serde")]
    pub path: PathBuf,
}

mod path_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::path::PathBuf;

    pub fn serialize<S: Serializer>(p: &PathBuf, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&p.to_string_lossy())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<PathBuf, D::Error> {
        let s = String::deserialize(d)?;
        Ok(PathBuf::from(s))
    }
}

/// Manifest format (geri uyumlu):
/// - Eski dosyalarda sadece `segments` vardır.
/// - Yeni formatta `active_paths`/`archived_paths` opsiyoneldir.
///   `active_paths` boşsa, tüm `segments` aktiftir.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub segments: Vec<SegmentRec>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub active_paths: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archived_paths: Vec<String>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            segments: vec![],
            active_paths: vec![],
            archived_paths: vec![],
        }
    }
}

impl Manifest {
    pub fn load(dir: &Path) -> Result<Self> {
        let p = dir.join("manifest.json");
        if !p.exists() {
            return Ok(Self::default());
        }
        let s = fs::read_to_string(p)?;
        let m: Manifest = serde_json::from_str(&s)?;
        Ok(m)
    }

    pub fn save_atomic(&self, dir: &Path) -> Result<()> {
        let p = dir.join("manifest.json");
        let tmp = dir.join("manifest.json.tmp");
        let mut f = fs::File::create(&tmp)?;
        f.write_all(serde_json::to_string_pretty(self)?.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &p)?;
        Ok(())
    }

    pub fn add_segment(&mut self, _dir: &Path, name: &str, kind: SegmentKind) -> Result<()> {
        let rec = SegmentRec {
            kind,
            path: PathBuf::from(name),
        };
        self.segments.push(rec);
        // varsayılan davranış: yeni segment aktif
        if !self.active_paths.contains(&name.to_string()) {
            self.active_paths.push(name.to_string());
        }
        Ok(())
    }

    /// Aktif segmentlerin dosya yolları (active_paths boşsa tüm segmentler aktiftir)
    pub fn active_segment_paths(&self) -> Vec<PathBuf> {
        if self.active_paths.is_empty() {
            return self.segments.iter().map(|s| s.path.clone()).collect();
        }
        let set: std::collections::HashSet<&str> =
            self.active_paths.iter().map(|s| s.as_str()).collect();
        self.segments
            .iter()
            .filter(|s| set.contains(s.path.to_string_lossy().as_ref()))
            .map(|s| s.path.clone())
            .collect()
    }

    /// Promote: Resolver segmentleri için tek “aktif” segment bırak.
    /// - Eğer `resolver-compact-*.prus` varsa en sonuncuyu aktif bırak.
    /// - Yoksa en son yazılmış resolver segmentini aktif bırak.
    /// Diğer türler (Dict/Fact) aktif kalır.
    pub fn promote_resolver_compact(&mut self) -> Result<usize> {
        // 1) Resolver segmentlerini ayır
        let mut resolver: Vec<&SegmentRec> = self
            .segments
            .iter()
            .filter(|s| s.kind == SegmentKind::Resolver)
            .collect();
        if resolver.is_empty() {
            return Ok(0);
        }

        // 2) Önce compact olanları bul
        resolver.sort_by_key(|s| s.path.clone());
        let mut last_compact: Option<&SegmentRec> = None;
        for s in &resolver {
            let fname = s.path.to_string_lossy();
            if fname.starts_with("resolver-compact-") {
                last_compact = Some(s);
            }
        }
        let chosen = if let Some(s) = last_compact {
            s
        } else {
            // compact yoksa en son resolver’ı seç
            *resolver.last().unwrap()
        };

        // 3) active_paths’i yeniden kur:
        // - resolver için sadece `chosen`
        // - diğer türler için mevcut aktif (varsa) veya tümü
        let resolver_name = chosen.path.to_string_lossy().to_string();

        // Diğer türler:
        let mut keep: Vec<String> = vec![];
        if self.active_paths.is_empty() {
            // henüz hiç set edilmemişse tüm non-resolver’ları ekle
            for s in &self.segments {
                if s.kind != SegmentKind::Resolver {
                    keep.push(s.path.to_string_lossy().to_string());
                }
            }
        } else {
            // aktifler içinden non-resolver olanları koru
            let set: std::collections::HashSet<&str> =
                self.active_paths.iter().map(|s| s.as_str()).collect();
            for s in &self.segments {
                if s.kind != SegmentKind::Resolver
                    && set.contains(s.path.to_string_lossy().as_ref())
                {
                    keep.push(s.path.to_string_lossy().to_string());
                }
            }
        }

        // resolver için tek seçilmiş segment
        keep.push(resolver_name.clone());

        // archived_paths’i de dolduralım ( bilgi amaçlı )
        let keep_set: std::collections::HashSet<&str> =
            keep.iter().map(|s| s.as_str()).collect();
        self.archived_paths = self
            .segments
            .iter()
            .map(|s| s.path.to_string_lossy().to_string())
            .filter(|name| !keep_set.contains(name.as_str()))
            .collect();

        self.active_paths = keep;
        Ok(1)
    }
}
