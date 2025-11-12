use std::path::PathBuf;
use std::sync::Arc;

use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;

/// Stored value in the process-wide enum cache.
/// Contains the loaded enum bitmap index and file identity snapshot used for validation.
#[derive(Clone, Debug)]
pub struct EnumCacheEntry {
    pub index: Arc<EnumBitmapIndex>,
    pub path: PathBuf,
    pub segment_id: u32,
    pub uid: String,
    pub field: String,
    pub ino: u64,
    pub mtime: i64,
    pub size: u64,
}

impl EnumCacheEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index: Arc<EnumBitmapIndex>,
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
        // Estimate size based on variants and zone bitmaps
        let mut total: usize = 0;

        // Variants: each string overhead + bytes
        for variant in &self.index.variants {
            total = total.saturating_add(variant.len()).saturating_add(24); // String overhead
        }

        // Zone bitmaps: HashMap overhead + zone_id (u32) + Vec overhead + bitmap bytes
        total = total.saturating_add(self.index.zone_bitmaps.len() * 24); // HashMap overhead per entry
        for bitsets in self.index.zone_bitmaps.values() {
            total = total.saturating_add(24); // Vec overhead
            for bits in bitsets {
                total = total.saturating_add(bits.len());
            }
        }

        // rows_per_zone (u16) + overhead
        total = total.saturating_add(16);

        // Add some overhead and avoid undercounting below file size
        total = total.saturating_add(4096);
        total.max(self.size as usize)
    }
}
