use serde::{Deserialize, Serialize};

use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredFrameMeta {
    pub file_name: String,
    pub schema: Vec<SchemaSnapshot>,
    pub schema_hash: u64,
    pub row_count: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub max_event_id: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub null_bitmap_len: u32,
    #[serde(default)]
    pub high_water_mark: HighWaterMark,
}
