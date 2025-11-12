mod cache;
mod entry;
mod entry_file;
mod index;
mod policy;
mod schema;
mod serde_ext;
mod storage;
mod telemetry;

pub use cache::{
    CacheOutcome, GlobalMaterializationCatalogCache, MaterializationCatalogCacheStats,
};
pub use entry::MaterializationEntry;
pub use entry_file::{entry_file_path, EntryFile};
pub use index::{CatalogIndex, IndexEntry, IndexFile};
pub use policy::RetentionPolicy;
pub use schema::SchemaSnapshot;
pub use storage::MaterializationCatalog;
pub use telemetry::MaterializationTelemetry;

#[cfg(test)]
mod cache_tests;
#[cfg(test)]
mod entry_file_tests;
#[cfg(test)]
mod entry_tests;
#[cfg(test)]
mod index_tests;
#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod schema_tests;
#[cfg(test)]
mod storage_tests;
