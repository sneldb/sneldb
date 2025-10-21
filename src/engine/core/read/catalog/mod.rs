pub mod index_kind;
pub mod index_registry;
pub mod segment_index_catalog;

pub use index_kind::IndexKind;
pub use index_registry::IndexRegistry;
pub use segment_index_catalog::SegmentIndexCatalog;

#[cfg(test)]
mod index_registry_test;
#[cfg(test)]
mod segment_index_catalog_test;
