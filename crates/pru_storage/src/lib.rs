use anyhow::Result;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub struct MediaStorage {
    pub root: PathBuf,
}

impl MediaStorage {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn store_media(&self, hash: &str, ext: &str, bytes: &[u8]) -> Result<PathBuf> {
        fs::create_dir_all(&self.root)?;
        let path = self.root.join(format!("{hash}.{ext}"));
        let mut file = File::create(&path)?;
        file.write_all(bytes)?;
        Ok(path)
    }

    pub fn load_media(&self, hash: &str, ext: &str) -> Result<Vec<u8>> {
        let path = self.root.join(format!("{hash}.{ext}"));
        let mut buf = Vec::new();
        File::open(&path)?.read_to_end(&mut buf)?;
        Ok(buf)
    }
}
