use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::engine::materialize::MaterializationError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::shared::time::now;
use tracing::{error, warn};

/// Lightweight index entry mapping name to entry file path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub name: String,
    pub entry_path: PathBuf,
    pub created_at: u64,
}

/// Catalog index file storing name -> entry path mappings
/// This is small and fast to load.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogIndex {
    pub entries: Vec<IndexEntry>,
    pub version: u16,
    pub updated_at: u64,
}

impl CatalogIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            version: 2,
            updated_at: now(),
        }
    }

    pub fn from_entries(entries: Vec<IndexEntry>) -> Self {
        Self {
            entries,
            version: 2,
            updated_at: now(),
        }
    }

    pub fn to_map(&self) -> HashMap<String, PathBuf> {
        self.entries
            .iter()
            .map(|e| (e.name.clone(), e.entry_path.clone()))
            .collect()
    }

    pub fn get_path(&self, name: &str) -> Option<&PathBuf> {
        self.entries
            .iter()
            .find(|e| e.name == name)
            .map(|e| &e.entry_path)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.entries.iter().any(|e| e.name == name)
    }

    pub fn add(&mut self, name: String, entry_path: PathBuf) {
        if let Some(existing) = self.entries.iter_mut().find(|e| e.name == name) {
            existing.entry_path = entry_path;
            existing.created_at = now();
        } else {
            self.entries.push(IndexEntry {
                name,
                entry_path,
                created_at: now(),
            });
        }
        self.updated_at = now();
    }

    pub fn remove(&mut self, name: &str) -> bool {
        let len_before = self.entries.len();
        self.entries.retain(|e| e.name != name);
        let removed = self.entries.len() < len_before;
        if removed {
            self.updated_at = now();
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for CatalogIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Handles loading and persisting the catalog index file
#[derive(Debug)]
pub struct IndexFile {
    path: PathBuf,
}

impl IndexFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<CatalogIndex, MaterializationError> {
        if !self.path.exists() {
            return Ok(CatalogIndex::new());
        }

        // Read entire file
        let mut buf = Vec::new();
        {
            let mut file = File::open(&self.path)?;
            if let Err(err) = file.read_to_end(&mut buf) {
                warn!(
                    error = %err,
                    path = %self.path.display(),
                    "index read failed; starting with empty index"
                );
                drop(file);
                self.backup_corrupt();
                return Ok(CatalogIndex::new());
            }
        }

        if buf.len() < crate::shared::storage_header::BinaryHeader::TOTAL_LEN {
            return Ok(CatalogIndex::new());
        }

        // Validate header
        {
            use std::io::Cursor;
            let mut cursor = Cursor::new(&buf[..crate::shared::storage_header::BinaryHeader::TOTAL_LEN]);
            if let Err(err) = self.validate_header(&mut cursor) {
                warn!(
                    error = %err,
                    path = %self.path.display(),
                    "index header invalid; starting with empty index"
                );
                self.backup_corrupt();
                return Ok(CatalogIndex::new());
            }
        }

        // Skip header when deserializing
        let data = &buf[crate::shared::storage_header::BinaryHeader::TOTAL_LEN..];
        match bincode::deserialize::<CatalogIndex>(data) {
            Ok(index) => Ok(index),
            Err(err) => {
                warn!(
                    error = %err,
                    path = %self.path.display(),
                    "index deserialize failed; starting with empty index"
                );
                self.backup_corrupt();
                Ok(CatalogIndex::new())
            }
        }
    }

    pub fn persist(&self, index: &CatalogIndex) -> Result<(), MaterializationError> {
        let mut tmp_path = self.path.clone();
        tmp_path.set_extension("tmp");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        // Write header
        let header = BinaryHeader::new(FileKind::MaterializationCatalog.magic(), 2, 0);
        header
            .write_to(&mut file)
            .map_err(|e| MaterializationError::Header(e.to_string()))?;

        // Serialize and write index
        let serialized = bincode::serialize(index)?;
        file.write_all(&serialized)?;
        file.flush()?;
        file.sync_all()?;

        // Atomic rename
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    fn validate_header<R: std::io::Read>(&self, reader: &mut R) -> Result<(), MaterializationError> {
        let header = BinaryHeader::read_from(reader)?;
        let expected_magic = FileKind::MaterializationCatalog.magic();

        if header.magic != expected_magic {
            return Err(MaterializationError::Header(
                "unexpected magic value".into(),
            ));
        }

        if header.version != 2 {
            return Err(MaterializationError::Header(format!(
                "unsupported version {}",
                header.version
            )));
        }

        Ok(())
    }

    fn backup_corrupt(&self) {
        let mut backup = self.path.clone();
        backup.set_extension(format!("corrupt-{}", now()));
        if let Err(rename_err) = fs::rename(&self.path, &backup) {
            error!(
                error = %rename_err,
                original = %self.path.display(),
                backup = %backup.display(),
                "failed to rename corrupt index"
            );
        } else {
            warn!(
                backup = %backup.display(),
                "moved corrupt index to backup"
            );
        }
    }
}

