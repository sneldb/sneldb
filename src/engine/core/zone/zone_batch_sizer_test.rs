use crate::engine::core::zone::zone_batch_sizer::ZoneBatchSizer;
use crate::shared::config::CONFIG;

#[test]
fn computes_level_scaled_rows() {
    // From test config: fill_factor * event_per_zone = 3 * 1 = 3
    // Level 0 (not used here) would be 3 * 1 = 3
    // Level 1 => * (1+1) = 6; Level 2 => * (2+1) = 9
    let base = CONFIG.engine.fill_factor * CONFIG.engine.event_per_zone;
    assert_eq!(base, 3);
    assert_eq!(ZoneBatchSizer::target_rows(1), base * 2);
    assert_eq!(ZoneBatchSizer::target_rows(2), base * 3);
}
