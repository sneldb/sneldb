mod guard;
mod header;
mod reader;
mod store;
mod types;
mod writer;

#[cfg(test)]
mod guard_test;
#[cfg(test)]
mod header_test;
#[cfg(test)]
mod reader_test;
#[cfg(test)]
mod store_test;
#[cfg(test)]
mod types_test;
#[cfg(test)]
mod writer_test;

pub use guard::FileLockGuard;
pub use header::{ensure_header, is_file_empty, read_and_validate_header};
pub use reader::{read_records, read_single_record, read_u32, record_skipped_record};
pub use store::SchemaStore;
pub use types::{
    MAX_RECORD_LEN_BYTES, RecordReadResult, SCHEMA_STORE_VERSION, SchemaStoreDiagnostics,
    SchemaStoreOptions,
};
pub use writer::{compute_crc32, write_record};
