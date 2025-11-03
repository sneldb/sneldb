pub(crate) mod data;
pub(crate) mod header;
pub(crate) mod metadata;
pub(crate) mod reader;
pub(crate) mod storage;
pub(crate) mod writer;

pub use metadata::StoredFrameMeta;

#[cfg(test)]
mod data_tests;
#[cfg(test)]
mod header_tests;
#[cfg(test)]
mod metadata_tests;
#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod storage_tests;
#[cfg(test)]
mod writer_tests;
