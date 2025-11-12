use std::path::PathBuf;

use crate::engine::materialize::MaterializationCatalog;
use crate::engine::materialize::MaterializationEntry;

use crate::shared::path::absolutize;

use super::errors::{ShowError, ShowResult};

pub trait CatalogGateway: Send + Sync {
    fn load(&self) -> ShowResult<CatalogHandle>;
}

pub struct FileCatalogGateway {
    data_dir: PathBuf,
}

impl FileCatalogGateway {
    pub fn new(data_dir: impl AsRef<std::path::Path>) -> Self {
        Self {
            data_dir: absolutize(data_dir.as_ref().to_path_buf()),
        }
    }
}

impl CatalogGateway for FileCatalogGateway {
    fn load(&self) -> ShowResult<CatalogHandle> {
        let catalog = MaterializationCatalog::load(&self.data_dir).map_err(|err| {
            ShowError::new(format!("Failed to load materialization catalog: {err}"))
        })?;
        Ok(CatalogHandle { catalog })
    }
}

pub struct CatalogHandle {
    catalog: MaterializationCatalog,
}

impl CatalogHandle {
    pub fn new(catalog: MaterializationCatalog) -> Self {
        Self { catalog }
    }

    pub fn fetch(&self, alias: &str) -> ShowResult<MaterializationEntry> {
        self.catalog
            .get(alias)
            .map_err(|e| ShowError::new(format!("Failed to load materialization: {e}")))?
            .ok_or_else(|| ShowError::new(format!("Materialization '{alias}' not found")))
    }

    pub fn upsert(&mut self, entry: MaterializationEntry) -> ShowResult<()> {
        self.catalog
            .upsert(entry)
            .map_err(|err| ShowError::new(format!("Failed to update catalog: {err}")))
    }
}
