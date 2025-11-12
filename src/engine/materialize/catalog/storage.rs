use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::cache::GlobalMaterializationCatalogCache;
use super::entry::MaterializationEntry;
use super::entry_file::{EntryFile, entry_file_path};
use super::index::IndexFile;
use crate::engine::materialize::MaterializationError;

/// MaterializationCatalog manages materialization entries using a per-entry file system.
///
/// Architecture:
/// - Lightweight index file (`catalog.mcat`) stores name -> entry path mappings
/// - Individual entry files (`materializations/<name>/entry.mcatentry`) store full entry data
/// - Global cache provides fast access to index and entries
///
/// Performance:
/// - O(1) index load (small file, cached)
/// - O(1) entry load (on-demand, cached)
/// - Incremental updates (only update changed entry + index)
/// - Scales to thousands of materializations
#[derive(Debug)]
pub struct MaterializationCatalog {
    root_dir: PathBuf,
    index_path: PathBuf,
    index_file: IndexFile,
    cache: &'static GlobalMaterializationCatalogCache,
}

impl MaterializationCatalog {
    /// Load the catalog from the given base directory.
    ///
    /// Loads only the lightweight index, not all entries.
    /// Individual entries are loaded on-demand via `get()`.
    pub fn load(base_dir: impl AsRef<Path>) -> Result<Self, MaterializationError> {
        let root_dir = base_dir.as_ref().join("materializations");
        fs::create_dir_all(&root_dir)?;
        let index_path = root_dir.join("catalog.mcat");

        let index_file = IndexFile::new(index_path.clone());

        // Load index file, creating empty index if it doesn't exist
        let index = index_file.load()?;

        // Ensure index is persisted if it was just created
        if index.is_empty() {
            index_file.persist(&index)?;
        }

        Ok(Self {
            root_dir,
            index_path,
            index_file,
            cache: GlobalMaterializationCatalogCache::instance(),
        })
    }

    /// Get a specific entry by name
    ///
    /// This loads the entry on-demand using the cache.
    pub fn get(&self, name: &str) -> Result<Option<MaterializationEntry>, MaterializationError> {
        // Load index from cache
        let (index, _) = self.cache.get_or_load_index(&self.index_path)?;

        // Find entry path in index
        let entry_path = match index.get_path(name) {
            Some(path) => path.clone(),
            None => return Ok(None),
        };

        // Load entry from cache
        let (entry_arc, _) = self.cache.get_or_load_entry(&entry_path)?;
        Ok(Some((*entry_arc).clone()))
    }

    /// Get all entry names (fast - only loads index)
    pub fn list_names(&self) -> Result<Vec<String>, MaterializationError> {
        let (index, _) = self.cache.get_or_load_index(&self.index_path)?;
        Ok(index.entries.iter().map(|e| e.name.clone()).collect())
    }

    /// Get all entries (slower - loads all entries)
    ///
    /// For large catalogs, prefer using `list_names()` and `get()` individually.
    pub fn entries(&self) -> Result<HashMap<String, MaterializationEntry>, MaterializationError> {
        let (index, _) = self.cache.get_or_load_index(&self.index_path)?;
        let mut entries = HashMap::new();

        for index_entry in &index.entries {
            let (entry_arc, _) = self.cache.get_or_load_entry(&index_entry.entry_path)?;
            entries.insert(index_entry.name.clone(), (*entry_arc).clone());
        }

        Ok(entries)
    }

    /// Insert a new entry
    ///
    /// This creates the entry file and updates the index.
    pub fn insert(&mut self, entry: MaterializationEntry) -> Result<(), MaterializationError> {
        // Load index (clone to get mutable copy)
        let index_arc = self.cache.get_or_load_index(&self.index_path)?.0;
        let mut index = (*index_arc).clone();

        // Check if already exists
        if index.contains(&entry.name) {
            return Err(MaterializationError::Duplicate(entry.name));
        }

        // Persist entry file
        let entry_path = entry_file_path(&self.root_dir, &entry.name);
        let entry_file = EntryFile::new(entry_path.clone());
        entry_file.persist(&entry)?;

        // Update index
        index.add(entry.name.clone(), entry_path.clone());
        self.index_file.persist(&index)?;

        // Invalidate caches
        self.cache.invalidate_index(&self.index_path);
        self.cache.invalidate_entry(&entry_path);

        Ok(())
    }

    /// Update or insert an entry
    ///
    /// This updates the entry file and index if needed.
    pub fn upsert(&mut self, entry: MaterializationEntry) -> Result<(), MaterializationError> {
        // Load index (clone to get mutable copy)
        let index_arc = self.cache.get_or_load_index(&self.index_path)?.0;
        let mut index = (*index_arc).clone();
        let is_new = !index.contains(&entry.name);

        // Persist entry file
        let entry_path = entry_file_path(&self.root_dir, &entry.name);
        let entry_file = EntryFile::new(entry_path.clone());
        entry_file.persist(&entry)?;

        // Update index if new
        if is_new {
            index.add(entry.name.clone(), entry_path.clone());
            self.index_file.persist(&index)?;
            self.cache.invalidate_index(&self.index_path);
        }

        // Invalidate entry cache
        self.cache.invalidate_entry(&entry_path);

        Ok(())
    }

    /// Remove an entry
    ///
    /// This removes the entry file and updates the index.
    pub fn remove(
        &mut self,
        name: &str,
    ) -> Result<Option<MaterializationEntry>, MaterializationError> {
        // Load index (clone to get mutable copy)
        let index_arc = self.cache.get_or_load_index(&self.index_path)?.0;
        let mut index = (*index_arc).clone();

        // Get entry path
        let entry_path = match index.get_path(name) {
            Some(path) => path.clone(),
            None => return Ok(None),
        };

        // Load entry before removing
        let entry_arc = self.cache.get_or_load_entry(&entry_path)?.0;
        let entry = (*entry_arc).clone();

        // Remove entry file
        if entry_path.exists() {
            if let Err(e) = fs::remove_file(&entry_path) {
                tracing::warn!(
                    error = %e,
                    path = %entry_path.display(),
                    "Failed to remove entry file"
                );
            }
        }

        // Update index
        index.remove(name);
        self.index_file.persist(&index)?;

        // Invalidate caches
        self.cache.invalidate_index(&self.index_path);
        self.cache.invalidate_entry(&entry_path);

        Ok(Some(entry))
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    pub fn catalog_path(&self) -> &Path {
        &self.index_path
    }
}
