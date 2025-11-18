use crate::engine::schema::registry::SchemaRecord;

/// Result of reading a single record.
pub enum RecordReadResult {
    /// Valid record read successfully
    Valid(SchemaRecord),
    /// Record corrupted but should continue reading next record
    Corrupted,
    /// End of file reached, stop reading
    Eof,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SchemaStoreDiagnostics {
    pub version: Option<u16>,
    pub valid_records: usize,
    pub skipped_records: usize,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStoreOptions {
    /// When true, flushes schema definitions to disk after each append for durability.
    pub fsync: bool,
}

impl Default for SchemaStoreOptions {
    fn default() -> Self {
        Self { fsync: false }
    }
}

pub const SCHEMA_STORE_VERSION: u16 = 1;
pub const MAX_RECORD_LEN_BYTES: u32 = 10 * 1024;
