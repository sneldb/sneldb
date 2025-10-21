use super::zone_temporal_index::ZoneTemporalIndex;
use crate::command::types::CompareOp;
use crate::engine::core::time::temporal_traits::ZoneRangeIndex;

#[test]
fn zti_contains_and_predecessor_basic() {
    // Monotone timestamps over ~10 seconds with duplicates
    let mut ts = vec![1000, 1000, 1001, 1002, 1005, 1005, 1009];
    let zti = ZoneTemporalIndex::from_timestamps(ts.drain(..).map(|x| x as i64).collect(), 1, 8);

    // Bounds
    assert_eq!(zti.min_ts, 1000);
    assert_eq!(zti.max_ts, 1009);

    // Exact contains
    assert!(zti.contains_ts(1000));
    assert!(zti.contains_ts(1005));
    assert!(!zti.contains_ts(1003));

    // Predecessor
    assert_eq!(zti.predecessor_ts(999), None);
    assert_eq!(zti.predecessor_ts(1000), Some(1000));
    assert_eq!(zti.predecessor_ts(1004), Some(1002));
    assert_eq!(zti.predecessor_ts(1005), Some(1005));
    assert_eq!(zti.predecessor_ts(1010), Some(1009));
}

#[test]
fn zti_stride_and_fences() {
    // Use stride 2
    let ts = vec![0i64, 2, 4, 6, 8, 10, 12, 14];
    let zti = ZoneTemporalIndex::from_timestamps(ts.clone(), 2, 4);

    assert!(zti.contains_ts(0));
    assert!(zti.contains_ts(14));
    assert!(!zti.contains_ts(3));

    // Fences produce a valid window (placeholder impl currently full range)
    let (lo, hi) = zti.fence_lb_ub(6);
    assert!(hi >= lo);
    assert!(hi as usize <= zti.keys.len().saturating_sub(1));
}

#[test]
fn zti_serde_roundtrip() {
    let ts = (0..100).map(|i| 1_700_000_000i64 + i).collect::<Vec<_>>();
    let zti = ZoneTemporalIndex::from_timestamps(ts.clone(), 1, 16);

    let dir = tempfile::tempdir().expect("tmpdir");
    zti.save("uid", 1, dir.path()).expect("save tfi");
    let loaded = ZoneTemporalIndex::load("uid", 1, dir.path()).expect("load tfi");

    // Spot checks
    assert_eq!(loaded.min_ts, zti.min_ts);
    assert_eq!(loaded.max_ts, zti.max_ts);
    assert_eq!(loaded.stride, zti.stride);
    assert_eq!(loaded.keys.len(), zti.keys.len());
    assert!(loaded.contains_ts(1_700_000_000));
    assert!(loaded.contains_ts(1_700_000_050));
}

#[test]
fn zti_empty_and_singleton_cases() {
    // Singleton
    let zti = ZoneTemporalIndex::from_timestamps(vec![1234], 1, 4);
    assert!(zti.contains_ts(1234));
    assert!(!zti.contains_ts(1233));
    assert_eq!(zti.predecessor_ts(1233), None);
    assert_eq!(zti.predecessor_ts(1234), Some(1234));

    // Empty input produces defaults
    let zti2 = ZoneTemporalIndex::from_timestamps(Vec::new(), 1, 4);
    // min_ts/max_ts are 0 by constructor; contains must be false for any ts due to bounds
    assert!(!zti2.contains_ts(0));
}

#[test]
fn zti_stride_membership_edgecases() {
    // stride 5, exact multiples should pass; off-grid should fail
    let base = 10i64;
    let ts = (0..=5).map(|i| base + i * 5).collect::<Vec<_>>();
    let zti = ZoneTemporalIndex::from_timestamps(ts, 5, 4);
    assert!(zti.contains_ts(base));
    assert!(zti.contains_ts(base + 25));
    assert!(!zti.contains_ts(base + 24));
    assert!(!zti.contains_ts(base + 26));
}

#[test]
fn zti_field_aware_serde_roundtrip() {
    // Build a ZTI and persist/load using the field-aware API
    let ts = (0..10)
        .map(|i| 2_000_000_000i64 + 2 * i)
        .collect::<Vec<_>>();
    let zti = ZoneTemporalIndex::from_timestamps(ts.clone(), 2, 5);

    let dir = tempfile::tempdir().expect("tmpdir");
    zti.save_for_field("uidF", "created_at", 7, dir.path())
        .expect("save tfi field");
    let loaded = ZoneTemporalIndex::load_for_field("uidF", "created_at", 7, dir.path())
        .expect("load tfi field");

    assert_eq!(loaded.min_ts, zti.min_ts);
    assert_eq!(loaded.max_ts, zti.max_ts);
    assert_eq!(loaded.stride, zti.stride);
    assert_eq!(loaded.keys.len(), zti.keys.len());
    // Membership check
    assert!(loaded.contains_ts(2_000_000_000));
    assert!(loaded.contains_ts(2_000_000_018));
    assert!(!loaded.contains_ts(2_000_000_019));
}

#[test]
fn zti_may_match_ops_basic() {
    // Values: 100, 105, 110
    let ts = vec![100i64, 105, 110];
    let zti = ZoneTemporalIndex::from_timestamps(ts, 1, 4);

    // Eq
    assert!(zti.may_match(CompareOp::Eq, 105));
    assert!(!zti.may_match(CompareOp::Eq, 104));

    // Neq: multi-value ⇒ always true
    assert!(zti.may_match(CompareOp::Neq, 105));
    assert!(zti.may_match(CompareOp::Neq, 999));

    // Greater/greater-equal vs max=110
    assert!(zti.may_match(CompareOp::Gt, 100));
    assert!(zti.may_match(CompareOp::Gte, 110));
    assert!(!zti.may_match(CompareOp::Gt, 110));

    // Less/less-equal vs min=100
    assert!(zti.may_match(CompareOp::Lt, 110));
    assert!(zti.may_match(CompareOp::Lte, 100));
    assert!(!zti.may_match(CompareOp::Lt, 100));
}

#[test]
fn zti_may_match_range_overlap() {
    // Continuous range [200..300]
    let ts = (0..=10).map(|i| 200i64 + i * 10).collect::<Vec<_>>();
    let zti = ZoneTemporalIndex::from_timestamps(ts, 1, 6);

    // Overlapping ranges
    assert!(zti.may_match_range(150, 210)); // overlaps at 200..210
    assert!(zti.may_match_range(290, 310)); // overlaps at 290..300
    assert!(zti.may_match_range(200, 300)); // full overlap

    // Non-overlapping ranges
    assert!(!zti.may_match_range(0, 199));
    assert!(!zti.may_match_range(301, 1000));

    // Inverted range ⇒ false
    assert!(!zti.may_match_range(300, 200));
}

#[test]
fn zti_field_slab_multiple_zones_roundtrip() {
    // Prepare three distinct zone indexes with different strides and ranges
    let zti_a = ZoneTemporalIndex::from_timestamps(vec![1_000i64, 1_002, 1_004, 1_006], 2, 4);
    let zti_b = ZoneTemporalIndex::from_timestamps(vec![2_000i64, 2_001, 2_002, 2_003], 1, 4);
    let zti_c = ZoneTemporalIndex::from_timestamps(vec![3_000i64, 3_010, 3_020], 10, 4);

    let dir = tempfile::tempdir().expect("tmpdir");
    let entries = vec![(3u32, &zti_a), (5u32, &zti_b), (9u32, &zti_c)];

    // Save all zones into a single per-field slab file
    ZoneTemporalIndex::save_field_slab("uidS", "created_at", dir.path(), &entries)
        .expect("save field slab");

    // Load each zone independently and validate contents
    let la = ZoneTemporalIndex::load_for_field("uidS", "created_at", 3, dir.path())
        .expect("load zone 3");
    assert_eq!(la.stride, 2);
    assert_eq!(la.min_ts, zti_a.min_ts);
    assert_eq!(la.max_ts, zti_a.max_ts);
    assert!(la.contains_ts(1_002));
    assert!(!la.contains_ts(1_003));

    let lb = ZoneTemporalIndex::load_for_field("uidS", "created_at", 5, dir.path())
        .expect("load zone 5");
    assert_eq!(lb.stride, 1);
    assert_eq!(lb.min_ts, zti_b.min_ts);
    assert_eq!(lb.max_ts, zti_b.max_ts);
    assert!(lb.contains_ts(2_003));
    assert!(!lb.contains_ts(1_002)); // cross-zone value should not appear

    let lc = ZoneTemporalIndex::load_for_field("uidS", "created_at", 9, dir.path())
        .expect("load zone 9");
    assert_eq!(lc.stride, 10);
    assert_eq!(lc.min_ts, zti_c.min_ts);
    assert_eq!(lc.max_ts, zti_c.max_ts);
    assert!(lc.contains_ts(3_010));
    assert!(!lc.contains_ts(3_011));

    // Loading a non-existent zone id should return NotFound
    let err = ZoneTemporalIndex::load_for_field("uidS", "created_at", 999, dir.path())
        .err()
        .expect("expected error");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}
