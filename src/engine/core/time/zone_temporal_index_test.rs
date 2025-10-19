use super::zone_temporal_index::ZoneTemporalIndex;

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
