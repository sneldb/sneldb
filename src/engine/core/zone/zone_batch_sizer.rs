use crate::shared::config::CONFIG;

/// Computes the target number of events per zone for a given output level.
///
/// L0 flushes use the base configuration (handled elsewhere). For compaction
/// outputs at level N (>=1), we scale as:
///   target = event_per_zone * fill_factor * (level + 1)
pub struct ZoneBatchSizer;

impl ZoneBatchSizer {
    #[inline]
    pub fn target_rows(level: u32) -> usize {
        let base = CONFIG.engine.event_per_zone;
        let fill = CONFIG.engine.fill_factor;
        // Scale linearly with level: L1 => *2, L2 => *3, etc.
        base.saturating_mul(fill)
            .saturating_mul((level as usize).saturating_add(1))
    }
}
