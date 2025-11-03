use crate::shared::config::CONFIG;

/// Computes the target number of events per zone for a given output level.
///
/// L0 flushes use the base configuration (handled elsewhere). Compaction now
/// preserves the original zone sizing so we keep the same target row count
/// regardless of compaction level.
pub struct ZoneBatchSizer;

impl ZoneBatchSizer {
    #[inline]
    pub fn target_rows(level: u32) -> usize {
        CONFIG.engine.event_per_zone * ((level as usize) + 1)
    }
}
