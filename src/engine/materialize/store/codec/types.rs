use crate::engine::materialize::catalog::SchemaSnapshot;

pub struct EncodedFrame {
    pub schema: Vec<SchemaSnapshot>,
    pub schema_hash: u64,
    pub row_count: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub max_event_id: u64,
    pub null_bitmap_len: u32,
    pub compressed: Vec<u8>,
    pub uncompressed_len: u32,
}

// Columnar storage format with per-value length metadata for faster decoding
// Format: [null_bitmap] [num_var_cols:u32] [length_tables] [column_data]
// - null_bitmap: one bit per value (row * column_count + col_idx)
// - num_var_cols: number of variable-size columns (String, JSON, Object, Array)
// - length_tables: for each variable-size column, store u32 length per row
// - column_data: all values stored column-by-column (not row-major)
pub(super) struct EncodedColumns {
    pub null_bitmap: Vec<u8>,
    pub length_tables: Vec<Vec<u32>>, // One length table per variable-size column
    pub column_data: Vec<Vec<u8>>,    // One buffer per column (columnar storage)
    pub schema_hash: u64,
    pub row_count: usize,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub max_event_id: u64,
}
