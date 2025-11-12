use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::engine::materialize::MaterializationError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use tracing::warn;

use super::entry::MaterializationEntry;

/// Handles loading and persisting individual materialization entry files
pub struct EntryFile {
    path: PathBuf,
}

impl EntryFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<MaterializationEntry, MaterializationError> {
        if !self.path.exists() {
            return Err(MaterializationError::Corrupt(format!(
                "Entry file does not exist: {}",
                self.path.display()
            )));
        }

        // Read entire file
        let mut buf = Vec::new();
        {
            let mut file = File::open(&self.path)?;
            file.read_to_end(&mut buf).map_err(|e| {
                MaterializationError::Corrupt(format!("Failed to read entry file: {}", e))
            })?;
        }

        if buf.len() < BinaryHeader::TOTAL_LEN {
            return Err(MaterializationError::Corrupt("Entry file too small".into()));
        }

        // Validate header from buffer
        {
            use std::io::Cursor;
            let mut cursor = Cursor::new(&buf[..BinaryHeader::TOTAL_LEN]);
            if let Err(err) = self.validate_header(&mut cursor) {
                warn!(
                    error = %err,
                    path = %self.path.display(),
                    "entry header invalid"
                );
                return Err(MaterializationError::Corrupt(format!(
                    "Invalid entry file header: {}",
                    err
                )));
            }
        }

        // Skip header when deserializing
        let data = &buf[BinaryHeader::TOTAL_LEN..];
        bincode::deserialize::<MaterializationEntry>(data).map_err(|e| {
            MaterializationError::Corrupt(format!("Failed to deserialize entry: {}", e))
        })
    }

    pub fn persist(&self, entry: &MaterializationEntry) -> Result<(), MaterializationError> {
        // Ensure parent directory exists
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut tmp_path = self.path.clone();
        tmp_path.set_extension("tmp");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Write header
        let header = BinaryHeader::new(FileKind::MaterializationCatalogEntry.magic(), 1, 0);
        header
            .write_to(&mut file)
            .map_err(|e| MaterializationError::Header(e.to_string()))?;

        // Serialize and write entry
        let serialized = bincode::serialize(entry)?;
        file.write_all(&serialized)?;
        file.flush()?;
        file.sync_all()?;

        // Atomic rename
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn validate_header<R: std::io::Read>(
        &self,
        reader: &mut R,
    ) -> Result<(), MaterializationError> {
        let header = BinaryHeader::read_from(reader)?;
        let expected_magic = FileKind::MaterializationCatalogEntry.magic();

        if header.magic != expected_magic {
            return Err(MaterializationError::Header(
                "unexpected magic value".into(),
            ));
        }

        if header.version != 1 {
            return Err(MaterializationError::Header(format!(
                "unsupported version {}",
                header.version
            )));
        }

        Ok(())
    }
}

/// Helper to construct entry file path from root directory and name
pub fn entry_file_path(root_dir: &Path, name: &str) -> PathBuf {
    root_dir.join(name).join("entry.mcatentry")
}
