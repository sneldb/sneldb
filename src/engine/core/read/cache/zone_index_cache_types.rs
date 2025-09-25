use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::zone::zone_index::ZoneIndex;

/// Process-wide ZoneIndex cache key.
/// We key by absolute index file path to ensure uniqueness across shards.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ZoneIndexCacheKey {
    pub path: PathBuf,
}

impl ZoneIndexCacheKey {
    /// Create a cache key from an absolute index path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

/// Stored value in the process-wide ZoneIndex cache.
/// Contains the loaded index and the file identity snapshot used for validation.
#[derive(Clone, Debug)]
pub struct ZoneIndexEntry {
    pub zone_index: Arc<ZoneIndex>,
    pub path: PathBuf,
    pub segment_id: String,
    pub uid: String,
    pub shard_id: Option<usize>,
    pub ino: u64,
    pub mtime: i64,
    pub size: u64,
}

impl ZoneIndexEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        zone_index: Arc<ZoneIndex>,
        path: PathBuf,
        segment_id: String,
        uid: String,
        shard_id: Option<usize>,
        ino: u64,
        mtime: i64,
        size: u64,
    ) -> Self {
        Self {
            zone_index,
            path,
            segment_id,
            uid,
            shard_id,
            ino,
            mtime,
            size,
        }
    }
}
