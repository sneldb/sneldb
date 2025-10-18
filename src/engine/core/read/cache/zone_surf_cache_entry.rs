use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;

/// Stored value in the process-wide ZoneSurf cache.
/// Contains the loaded surf filter and file identity snapshot used for validation.
#[derive(Clone, Debug)]
pub struct ZoneSurfCacheEntry {
    pub filter: Arc<ZoneSurfFilter>,
    pub path: PathBuf,
    pub segment_id: u32,
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
        segment_id: u32,
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
        // Approximate in-memory usage by summing vector sizes of all tries
        let mut total: usize = 0;
        for e in &self.filter.entries {
            let t = &e.trie;
            total = total
                .saturating_add(t.degrees.len() * std::mem::size_of::<u16>())
                .saturating_add(t.child_offsets.len() * std::mem::size_of::<u32>())
                .saturating_add(t.labels.len() * std::mem::size_of::<u8>())
                .saturating_add(t.edge_to_child.len() * std::mem::size_of::<u32>())
                .saturating_add(t.is_terminal_bits.len() * std::mem::size_of::<u8>());
        }
        // Add some overhead and avoid undercounting below file size
        total = total.saturating_add(4096);
        total.max(self.size as usize)
    }
}
