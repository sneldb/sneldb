mod entry;
mod file;
mod policy;
mod schema;
mod serde_ext;
mod storage;
mod telemetry;

pub use entry::MaterializationEntry;
pub use policy::RetentionPolicy;
pub use schema::SchemaSnapshot;
pub use storage::MaterializationCatalog;
pub use telemetry::MaterializationTelemetry;

#[cfg(test)]
mod entry_tests;
#[cfg(test)]
mod file_tests;
#[cfg(test)]
mod policy_tests;
#[cfg(test)]
mod schema_tests;
#[cfg(test)]
mod storage_tests;
