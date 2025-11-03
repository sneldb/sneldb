use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::shared::time::now;
use tracing::{error, warn};

use super::entry::MaterializationEntry;
use crate::engine::materialize::MaterializationError;

#[derive(Debug)]
pub struct CatalogFile {
    path: PathBuf,
}

impl CatalogFile {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load_entries(
        &self,
    ) -> Result<HashMap<String, MaterializationEntry>, MaterializationError> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }

        let mut file = File::open(&self.path)?;
        if let Err(err) = Self::validate_header(&mut file) {
            warn!(
                error = %err,
                path = %self.path.display(),
                "catalog header invalid; starting with empty catalog"
            );
            drop(file);
            self.backup_corrupt();
            return Ok(HashMap::new());
        }

        let mut buf = Vec::new();
        if let Err(err) = file.read_to_end(&mut buf) {
            warn!(
                error = %err,
                path = %self.path.display(),
                "catalog read failed; starting with empty catalog"
            );
            drop(file);
            self.backup_corrupt();
            return Ok(HashMap::new());
        }
        if buf.is_empty() {
            return Ok(HashMap::new());
        }

        match bincode::deserialize::<Vec<MaterializationEntry>>(&buf) {
            Ok(stored) => Ok(stored
                .into_iter()
                .map(|entry| (entry.name.clone(), entry))
                .collect()),
            Err(err) => {
                warn!(
                    error = %err,
                    path = %self.path.display(),
                    "catalog deserialize failed; starting with empty catalog"
                );
                drop(file);
                self.backup_corrupt();
                Ok(HashMap::new())
            }
        }
    }

    pub fn persist(
        &self,
        entries: &HashMap<String, MaterializationEntry>,
    ) -> Result<(), MaterializationError> {
        let mut tmp_path = self.path.clone();
        tmp_path.set_extension("tmp");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        let header = BinaryHeader::new(FileKind::MaterializationCatalog.magic(), 1, 0);
        header
            .write_to(&mut file)
            .map_err(|e| MaterializationError::Header(e.to_string()))?;

        let mut values: Vec<&MaterializationEntry> = entries.values().collect();
        values.sort_by(|a, b| a.name.cmp(&b.name));

        let serialized = bincode::serialize(&values)?;
        file.write_all(&serialized)?;
        file.flush()?;
        file.sync_all()?;

        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    pub fn validate_header(reader: &mut File) -> Result<(), MaterializationError> {
        let header = BinaryHeader::read_from(reader)?;
        if header.magic != FileKind::MaterializationCatalog.magic() {
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

    fn backup_corrupt(&self) {
        let mut backup = self.path.clone();
        backup.set_extension(format!("corrupt-{}", now()));
        if let Err(rename_err) = fs::rename(&self.path, &backup) {
            error!(
                error = %rename_err,
                original = %self.path.display(),
                backup = %backup.display(),
                "failed to rename corrupt catalog"
            );
        } else {
            warn!(
                backup = %backup.display(),
                "moved corrupt catalog to backup"
            );
        }
    }
}
