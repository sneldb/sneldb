use crate::engine::core::zone::zone_batch_sizer::ZoneBatchSizer;
use crate::shared::config::CONFIG;

#[test]
fn scales_rows_linearly_with_level() {
    // From test config: event_per_zone = 1
    let base = CONFIG.engine.event_per_zone;
    assert_eq!(base, 1);
    assert_eq!(ZoneBatchSizer::target_rows(0), base * 1);
    assert_eq!(ZoneBatchSizer::target_rows(1), base * 2);
    assert_eq!(ZoneBatchSizer::target_rows(2), base * 3);
}
