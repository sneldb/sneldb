use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;

/// Stored value in the process-wide ZoneXorFilter cache.
/// Contains the loaded XOR filter index and file identity snapshot used for validation.
#[derive(Clone, Debug)]
pub struct ZoneXorFilterCacheEntry {
    pub index: Arc<ZoneXorFilterIndex>,
    pub path: PathBuf,
    pub segment_id: u32,
    pub uid: String,
    pub field: String,
    pub ino: u64,
    pub mtime: i64,
    pub size: u64,
}

impl ZoneXorFilterCacheEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: Arc<ZoneXorFilterIndex>,
        path: PathBuf,
        segment_id: u32,
        uid: String,
        field: String,
        ino: u64,
        mtime: i64,
        size: u64,
    ) -> Self {
        Self {
            index,
            path,
            segment_id,
            uid,
            field,
            ino,
            mtime,
            size,
        }
    }

    /// Estimate the memory usage of this cache entry
    pub fn estimated_size(&self) -> usize {
        // Estimate size based on number of filters and their approximate size
        // BinaryFuse8 filters are roughly proportional to the number of items they contain
        // Each filter has overhead + data, estimate ~8 bytes per item on average
        let mut total: usize = 0;
        for filter in self.index.filters.values() {
            // BinaryFuse8 internal structure: approximate size
            // This is a rough estimate - actual size depends on filter construction
            // Use a conservative estimate of ~1KB per filter minimum
            total = total.saturating_add(1024);
        }
        // Add overhead for HashMap and other structures
        total = total
            .saturating_add(self.index.filters.len() * 16) // HashMap overhead
            .saturating_add(4096); // General overhead
        // Ensure we don't underestimate below file size
        total.max(self.size as usize)
    }
}
