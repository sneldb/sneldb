use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;

/// Stored value in the process-wide ZoneSurf cache.
/// Contains the loaded surf filter and file identity snapshot used for validation.
#[derive(Clone, Debug)]
pub struct ZoneSurfCacheEntry {
    pub filter: Arc<ZoneSurfFilter>,
    pub path: PathBuf,
    pub segment_id: String,
    pub uid: String,
    pub field: String,
    pub ino: u64,
    pub mtime: i64,
    pub size: u64,
}

impl ZoneSurfCacheEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        filter: Arc<ZoneSurfFilter>,
        path: PathBuf,
        segment_id: String,
        uid: String,
        field: String,
        ino: u64,
        mtime: i64,
        size: u64,
    ) -> Self {
        Self {
            filter,
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
        // Rough estimation: surf filter size + metadata overhead
        // This is a conservative estimate - actual size may vary
        self.size as usize + 1024 // Add 1KB for metadata overhead
    }
}

