use crate::engine::core::snapshot::snapshot_meta::SnapshotMeta;

#[test]
fn new_normalizes_range_and_sets_fields() {
    let m = SnapshotMeta::new("uid1", "ctx1", 200, 100);
    assert_eq!(m.uid, "uid1");
    assert_eq!(m.context_id, "ctx1");
    assert_eq!(m.from_ts, 100);
    assert_eq!(m.to_ts, 200);
}

#[test]
fn duration_and_contains_ts_work() {
    let m = SnapshotMeta::new("u", "c", 10, 20);
    assert_eq!(m.duration(), 10);
    assert!(m.contains_ts(10));
    assert!(m.contains_ts(15));
    assert!(m.contains_ts(20));
    assert!(!m.contains_ts(9));
    assert!(!m.contains_ts(21));
}

#[test]
fn overlaps_and_adjacent_detection() {
    let a = SnapshotMeta::new("u", "c", 10, 20);
    let b = SnapshotMeta::new("u", "c", 15, 25);
    let c = SnapshotMeta::new("u", "c", 21, 30);
    let d = SnapshotMeta::new("u", "c", 31, 40);

    assert!(a.overlaps_time(&b));
    assert!(!a.overlaps_time(&d));
    assert!(a.is_adjacent_time(&c));
    assert!(!a.is_adjacent_time(&d));
}

#[test]
fn merge_with_if_compatible_success_and_failures() {
    let a = SnapshotMeta::new("u", "c", 10, 20);
    let b = SnapshotMeta::new("u", "c", 21, 30); // adjacent, same uid/context
    let merged = a.merge_with_if_compatible(&b).expect("should merge");
    assert_eq!(merged.from_ts, 10);
    assert_eq!(merged.to_ts, 30);
    assert_eq!(merged.uid, "u");
    assert_eq!(merged.context_id, "c");

    // Different uid
    let diff_uid = SnapshotMeta::new("u2", "c", 15, 25);
    assert!(a.merge_with_if_compatible(&diff_uid).is_none());

    // Different context
    let diff_ctx = SnapshotMeta::new("u", "c2", 15, 25);
    assert!(a.merge_with_if_compatible(&diff_ctx).is_none());

    // Same uid/context but disjoint (non-adjacent, non-overlapping)
    let far = SnapshotMeta::new("u", "c", 32, 40);
    assert!(a.merge_with_if_compatible(&far).is_none());
}
