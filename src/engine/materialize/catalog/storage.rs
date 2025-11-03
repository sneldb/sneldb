use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::entry::MaterializationEntry;
use super::file::CatalogFile;
use crate::engine::materialize::MaterializationError;

#[derive(Debug)]
pub struct MaterializationCatalog {
    root_dir: PathBuf,
    file: CatalogFile,
    entries: HashMap<String, MaterializationEntry>,
}

impl MaterializationCatalog {
    pub fn load(base_dir: impl AsRef<Path>) -> Result<Self, MaterializationError> {
        let root_dir = base_dir.as_ref().join("materializations");
        fs::create_dir_all(&root_dir)?;
        let catalog_path = root_dir.join("catalog.bin");

        let file = CatalogFile::new(catalog_path);
        let entries = file.load_entries()?;

        Ok(Self {
            root_dir,
            file,
            entries,
        })
    }

    pub fn entries(&self) -> &HashMap<String, MaterializationEntry> {
        &self.entries
    }

    pub fn get(&self, name: &str) -> Option<&MaterializationEntry> {
        self.entries.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut MaterializationEntry> {
        self.entries.get_mut(name)
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn catalog_path(&self) -> &Path {
        self.file.path()
    }

    pub fn insert(&mut self, entry: MaterializationEntry) -> Result<(), MaterializationError> {
        if self.entries.contains_key(&entry.name) {
            return Err(MaterializationError::Duplicate(entry.name));
        }
        self.entries.insert(entry.name.clone(), entry);
        self.persist()
    }

    pub fn upsert(&mut self, entry: MaterializationEntry) -> Result<(), MaterializationError> {
        self.entries.insert(entry.name.clone(), entry);
        self.persist()
    }

    pub fn remove(
        &mut self,
        name: &str,
    ) -> Result<Option<MaterializationEntry>, MaterializationError> {
        let removed = self.entries.remove(name);
        if removed.is_some() {
            self.persist()?;
        }
        Ok(removed)
    }

    pub fn persist(&self) -> Result<(), MaterializationError> {
        self.file.persist(&self.entries)
    }
}
