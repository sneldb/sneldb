pub mod catalog;
mod error;
mod high_water;
mod sink;
mod source;
mod spec;
mod store;

#[cfg(test)]
mod high_water_tests;
#[cfg(test)]
mod sink_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod spec_tests;

pub use catalog::{MaterializationCatalog, MaterializationEntry, SchemaSnapshot};
pub use error::MaterializationError;
pub use high_water::HighWaterMark;
pub use sink::MaterializedSink;
pub use source::MaterializedSource;
pub use spec::MaterializedQuerySpecExt;
pub use store::{
    MaterializedStore, StoredFrameMeta, batch_schema_to_snapshots, schema_to_batch_schema,
};
