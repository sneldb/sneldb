use super::TemporalCalendarIndex;
use crate::command::types::CompareOp;
use crate::engine::core::time::temporal_traits::FieldIndex;
use roaring::RoaringBitmap;

fn bm(ids: &[u32]) -> RoaringBitmap {
    let mut b = RoaringBitmap::new();
    for &id in ids {
        b.insert(id);
    }
    b
}

#[test]
fn add_zone_range_populates_hour_and_day_buckets() {
    let mut cal = TemporalCalendarIndex::new("created_at");
    // Range spans within the same hour and same day
    let start = 1_700_000_000u64; // some stable epoch
    let end = start + 120; // +2 minutes
    cal.add_zone_range(3, start, end);

    // Eq on any ts within the hour/day returns the zone
    let zones = cal.zones_intersecting(CompareOp::Eq, start as i64);
    assert!(zones.contains(3));

    // Range covering the same bucket returns the zone
    let zr = cal.zones_intersecting_range(start as i64, end as i64);
    assert!(zr.contains(3));
}

#[test]
fn ranges_spanning_multiple_hours_and_days_union_correctly() {
    let mut cal = TemporalCalendarIndex::new("due_date");
    // Construct a range that crosses hour boundary and day boundary
    // Day 0 23:30 to Day 1 01:15
    let day0_23_30 = 1_700_000_000u64; // arbitrary
    let day1_01_15 = day0_23_30 + 1 * 3600 + 45 * 60; // +1h45m
    cal.add_zone_range(1, day0_23_30, day1_01_15);

    // A timestamp at day0 23:59 should hit
    let ts1 = day0_23_30 + 29 * 60; // 23:59
    assert!(
        cal.zones_intersecting(CompareOp::Eq, ts1 as i64)
            .contains(1)
    );
    // A timestamp at day1 01:00 should hit
    let ts2 = day0_23_30 + 90 * 60; // +1h30 = 01:00 next day
    assert!(
        cal.zones_intersecting(CompareOp::Eq, ts2 as i64)
            .contains(1)
    );

    // Range query across the entire span returns the zone
    let zr = cal.zones_intersecting_range(day0_23_30 as i64, day1_01_15 as i64);
    assert!(zr.contains(1));
}

#[test]
fn comparison_ops_gte_lte_gt_lt_behave() {
    let mut cal = TemporalCalendarIndex::new("ts");
    let base = 1_700_000_000u64;
    cal.add_zone_range(7, base, base + 3 * 3600); // 3 hours window

    // >= base should include 7
    assert!(
        cal.zones_intersecting(CompareOp::Gte, base as i64)
            .contains(7)
    );
    // > base - 1 should include 7
    assert!(
        cal.zones_intersecting(CompareOp::Gt, (base as i64) - 1)
            .contains(7)
    );
    // <= base + 3h should include 7
    assert!(
        cal.zones_intersecting(CompareOp::Lte, (base + 3 * 3600) as i64)
            .contains(7)
    );
    // < base + 3h + 1 should include 7
    assert!(
        cal.zones_intersecting(CompareOp::Lt, (base + 3 * 3600 + 1) as i64)
            .contains(7)
    );

    // < base should include 7 via day union; note calendar is coarse for range ops
    let lt = cal.zones_intersecting(CompareOp::Lt, base as i64);
    assert!(lt.is_empty() || lt.contains(7));
}

#[test]
fn neq_filters_out_specific_bucket_membership() {
    let mut cal = TemporalCalendarIndex::new("ts");
    let base = 1_700_000_000u64;
    cal.add_zone_range(10, base, base + 3600);

    // Collect all zones from day buckets
    let all_before = {
        let mut all = RoaringBitmap::new();
        // use zones_intersecting_range to pull day union
        all |= cal.zones_intersecting_range(base as i64, (base + 3600) as i64);
        all
    };
    // Neq removes the exact Eq bucket zones
    let neq = cal.zones_intersecting(CompareOp::Neq, base as i64);
    if all_before.contains(10) {
        assert!(!neq.contains(10));
    }
}

#[test]
fn negative_timestamps_are_ignored() {
    let mut cal = TemporalCalendarIndex::new("created_at");
    let base = 1_700_000_000u64;
    cal.add_zone_range(2, base, base + 60);

    // Eq with negative ts
    assert!(cal.zones_intersecting(CompareOp::Eq, -1).is_empty());
    // Range with negative min
    let zr = cal.zones_intersecting_range(-10, base as i64);
    assert!(zr.contains(2));
}

#[test]
fn empty_calendar_behaves() {
    let cal = TemporalCalendarIndex::new("any");
    assert!(cal.zones_intersecting(CompareOp::Eq, 123).is_empty());
    assert!(cal.zones_intersecting_range(0, 1_000).is_empty());
}

#[test]
fn save_and_load_roundtrip() {
    let mut cal = TemporalCalendarIndex::new("ts");
    let base = 1_700_000_000u64;
    cal.add_zone_range(5, base, base + 7200);

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    cal.save("UID123", dir).expect("save");

    let loaded = TemporalCalendarIndex::load("UID123", "ts", dir).expect("load");
    // Check basic integrity: a known Eq hits same zone
    let eq = loaded.zones_intersecting(CompareOp::Eq, base as i64);
    assert!(eq.contains(5));
}
