use crate::engine::core::read::cache::global_calendar_cache::{
    GlobalCalendarCache, GlobalFieldCalendarCache,
};
use crate::engine::core::time::{
    CalendarDir, TemporalCalendarIndex, calendar_dir::GranularityPref,
};
use std::sync::Arc;

#[test]
fn calendar_global_cache_reuses_arc_and_contains_zone() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00000";
    let uid = "uid_cal";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    let mut cal = CalendarDir::new();
    cal.add_zone_range(3, 1_700_000_000, 1_700_000_359);
    cal.save(uid, &seg_dir).expect("save cal");

    let cache = GlobalCalendarCache::instance();
    let c1 = cache
        .get_or_load(&base_dir, segment_id, uid)
        .expect("load1");
    let c2 = cache
        .get_or_load(&base_dir, segment_id, uid)
        .expect("load2");
    assert!(Arc::ptr_eq(&c1, &c2), "expected same Arc from global cache");

    let hb = c1.zones_for(1_700_000_100, GranularityPref::Hour);
    assert!(hb.contains(3));
}

#[test]
fn field_calendar_global_cache_reuses_arc_and_contains_zone() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00001";
    let uid = "uid_fcal";
    let field = "created_at";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    let mut cal = TemporalCalendarIndex::new(field.to_string());
    cal.add_zone_range(5, 1_700_000_000, 1_700_000_359);
    cal.save(uid, &seg_dir).expect("save field cal");

    let cache = GlobalFieldCalendarCache::instance();
    let c1 = cache
        .get_or_load(&base_dir, segment_id, uid, field)
        .expect("load1");
    let c2 = cache
        .get_or_load(&base_dir, segment_id, uid, field)
        .expect("load2");
    assert!(
        Arc::ptr_eq(&c1, &c2),
        "expected same Arc from global field calendar cache"
    );
}
