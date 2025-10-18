use std::collections::HashMap;

use super::segment_id::{LEVEL_SPAN, SegmentId};

/// Simple per-shard allocator that hands out numeric segment ids by level ranges.
#[derive(Debug, Clone)]
pub struct RangeAllocator {
    /// For each level, tracks the next offset within that level's range.
    /// The actual id is computed as: level * LEVEL_SPAN + offset
    next_offset_by_level: HashMap<u32, u32>,
}

impl Default for RangeAllocator {
    fn default() -> Self {
        Self {
            next_offset_by_level: HashMap::new(),
        }
    }
}

impl RangeAllocator {
    /// Create an empty allocator (all levels start at offset 0).
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed allocator from existing numeric directory names (zero-padded strings).
    pub fn from_existing_ids<'a, I>(ids: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut next_by_level: HashMap<u32, u32> = HashMap::new();

        for s in ids {
            if let Some(seg) = SegmentId::from_str(s) {
                let level = seg.level();
                let offset_in_level = seg.id % LEVEL_SPAN;
                let next = next_by_level.get(&level).copied().unwrap_or(0);
                // next offset is one past max seen offset
                let candidate_next = offset_in_level.saturating_add(1);
                if candidate_next > next {
                    next_by_level.insert(level, candidate_next);
                }
            }
        }

        Self {
            next_offset_by_level: next_by_level,
        }
    }

    /// Allocate the next id for a given level, returning the absolute id.
    pub fn next_for_level(&mut self, level: u32) -> u32 {
        let next_off = self.next_offset_by_level.entry(level).or_insert(0);
        let id = level.saturating_mul(LEVEL_SPAN).saturating_add(*next_off);
        *next_off = next_off.saturating_add(1);
        id
    }
}
