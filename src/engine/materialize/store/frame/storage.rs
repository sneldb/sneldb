use std::path::{Path, PathBuf};

use crate::engine::materialize::MaterializationError;

use super::reader::FrameReader;
use super::writer::FrameWriter;

#[derive(Debug, Clone)]
pub struct FrameStorage {
    dir: PathBuf,
}

impl FrameStorage {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, MaterializationError> {
        let dir = path.as_ref();
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    pub fn writer(&self) -> FrameWriter<'_> {
        FrameWriter::new(&self.dir)
    }

    pub fn reader(&self) -> FrameReader<'_> {
        FrameReader::new(&self.dir)
    }

    pub fn remove(&self, file_name: &str) {
        let path = self.dir.join(file_name);
        let _ = std::fs::remove_file(path);
    }
}
