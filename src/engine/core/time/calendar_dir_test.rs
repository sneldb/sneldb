use super::calendar_dir::{CalendarDir, GranularityPref};
use crate::command::types::TimeGranularity;
use crate::shared::datetime::time_bucketing::naive_bucket_of;

#[test]
fn calendar_single_hour_and_day_bucket() {
    let mut cal = CalendarDir::new();
    let zone_id = 42u32;
    // Choose a timestamp inside an hour
    let ts = 1_700_000_123u64; // arbitrary
    let hour_start = naive_bucket_of(ts, &TimeGranularity::Hour);
    let day_start = naive_bucket_of(ts, &TimeGranularity::Day);

    // min=max within same hour/day
    cal.add_zone_range(zone_id, ts, ts);

    // Hour lookup anywhere within the same hour should find the zone
    let hour_mid = hour_start + 3599;
    let hb = cal.zones_for(hour_mid, GranularityPref::Hour);
    assert!(hb.contains(zone_id));

    // Day lookup anywhere within the same day should find the zone
    let day_mid = day_start + 86_399;
    let db = cal.zones_for(day_mid, GranularityPref::Day);
    assert!(db.contains(zone_id));
}

#[test]
fn calendar_cross_multiple_hours_and_days() {
    let mut cal = CalendarDir::new();
    let zone_id = 7u32;

    // Range that spans 5 hours and crosses day boundary
    // Compute an actual day boundary from an anchor
    let anchor = 1_700_086_400u64;
    let day = naive_bucket_of(anchor, &TimeGranularity::Day);
    let min_ts = day + 23 * 3600; // 23:00:00
    let max_ts = (day + 24 * 3600) + 2 * 3600; // next day 02:00:00
    cal.add_zone_range(zone_id, min_ts, max_ts);

    // Expect hours: 23, 00, 01, 02
    for offset_h in [23u64, 24u64, 25u64, 26u64] {
        let ts = day + offset_h * 3600 + 1;
        let hb = cal.zones_for(ts, GranularityPref::Hour);
        assert!(
            hb.contains(zone_id),
            "missing hour bucket at offset {}",
            offset_h
        );
    }

    // Expect both days marked
    let db1 = cal.zones_for(day + 1, GranularityPref::Day);
    assert!(db1.contains(zone_id));
    let next_day = day + 86_400;
    let db2 = cal.zones_for(next_day + 1, GranularityPref::Day);
    assert!(db2.contains(zone_id));
}

#[test]
fn calendar_empty_when_bucket_not_present() {
    let cal = CalendarDir::new();
    let ts = 1_800_000_000u64;
    let hb = cal.zones_for(ts, GranularityPref::Hour);
    assert!(hb.is_empty());
    let db = cal.zones_for(ts, GranularityPref::Day);
    assert!(db.is_empty());
}

#[test]
fn calendar_serde_roundtrip() {
    let mut cal = CalendarDir::new();
    cal.add_zone_range(1, 1_700_000_000, 1_700_000_123);
    cal.add_zone_range(2, 1_700_000_000 + 86_400, 1_700_000_000 + 86_400 + 1);

    let dir = tempfile::tempdir().expect("tmpdir");
    let uid = "u";
    cal.save(uid, dir.path()).expect("save calendar");

    let loaded = CalendarDir::load(uid, dir.path()).expect("load calendar");

    // Probe a couple of buckets
    let ts1 = 1_700_000_100u64;
    let hb1 = cal.zones_for(ts1, GranularityPref::Hour);
    let hb1_loaded = loaded.zones_for(ts1, GranularityPref::Hour);
    assert_eq!(hb1, hb1_loaded);

    let ts2 = 1_700_000_000u64 + 86_400 + 1;
    let db2 = cal.zones_for(ts2, GranularityPref::Day);
    let db2_loaded = loaded.zones_for(ts2, GranularityPref::Day);
    assert_eq!(db2, db2_loaded);
}

#[test]
fn calendar_hour_boundary_inclusive() {
    let mut cal = CalendarDir::new();
    let zone_id = 3u32;
    let day = naive_bucket_of(1_700_123_456, &TimeGranularity::Day);
    let hour = day + 12 * 3600; // 12:00:00
    let min_ts = hour; // exact hour start
    let max_ts = hour + 3600 - 1; // last second of the hour
    cal.add_zone_range(zone_id, min_ts, max_ts);

    // Check just inside the hour
    let t1 = hour + 1;
    assert!(cal.zones_for(t1, GranularityPref::Hour).contains(zone_id));
    // Check boundary end-1 is still included
    let t2 = hour + 3599;
    assert!(cal.zones_for(t2, GranularityPref::Hour).contains(zone_id));
    // Next hour should be empty
    let t3 = hour + 3600 + 1;
    assert!(!cal.zones_for(t3, GranularityPref::Hour).contains(zone_id));
}

#[test]
fn calendar_day_boundary_zero_length() {
    let mut cal = CalendarDir::new();
    let day = naive_bucket_of(1_700_555_000, &TimeGranularity::Day);
    let zone_id = 9u32;
    // min=max at 00:00:00
    cal.add_zone_range(zone_id, day, day);
    // That day bucket should contain the zone
    assert!(
        cal.zones_for(day + 1, GranularityPref::Day)
            .contains(zone_id)
    );
    // Previous/next day should not
    assert!(
        !cal.zones_for(day - 1, GranularityPref::Day)
            .contains(zone_id)
    );
    assert!(
        !cal.zones_for(day + 86_400 + 1, GranularityPref::Day)
            .contains(zone_id)
    );
}

#[test]
fn calendar_multi_zone_same_bucket() {
    let mut cal = CalendarDir::new();
    let day = naive_bucket_of(1_701_000_000, &TimeGranularity::Day);
    // Two zones in the same hour/day buckets
    cal.add_zone_range(1, day + 10, day + 20);
    cal.add_zone_range(2, day + 30, day + 40);
    let t = day + 35;
    let hb = cal.zones_for(t, GranularityPref::Hour);
    assert!(hb.contains(1));
    assert!(hb.contains(2));
    let db = cal.zones_for(t, GranularityPref::Day);
    assert!(db.contains(1));
    assert!(db.contains(2));
}
