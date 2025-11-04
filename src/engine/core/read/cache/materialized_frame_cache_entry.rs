use std::sync::Arc;

use crate::engine::core::read::flow::ColumnBatch;

/// Stored value in the process-wide materialized frame cache.
/// Contains the decoded ColumnBatch and its estimated size in bytes.
#[derive(Clone, Debug)]
pub struct MaterializedFrameCacheEntry {
    pub batch: Arc<ColumnBatch>,
    pub size_bytes: usize,
}

impl MaterializedFrameCacheEntry {
    pub fn new(batch: Arc<ColumnBatch>, size_bytes: usize) -> Self {
        Self { batch, size_bytes }
    }

    pub fn estimated_size(&self) -> usize {
        self.size_bytes
    }
}
