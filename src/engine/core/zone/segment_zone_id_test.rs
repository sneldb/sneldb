use crate::engine::core::{CandidateZone, SegmentZoneId};
use std::collections::HashSet;

#[test]
fn creates_new_segment_zone_id() {
    let id = SegmentZoneId::new("segment-123".to_string(), 42);
    assert_eq!(id.segment_id(), "segment-123");
    assert_eq!(id.zone_id(), 42);
}

#[test]
fn creates_from_tuple() {
    let id = SegmentZoneId::from_tuple(("segment-456".to_string(), 10));
    assert_eq!(id.segment_id(), "segment-456");
    assert_eq!(id.zone_id(), 10);
}

#[test]
fn creates_from_str_ref_tuple() {
    let id: SegmentZoneId = ("segment-789", 5).into();
    assert_eq!(id.segment_id(), "segment-789");
    assert_eq!(id.zone_id(), 5);
}

#[test]
fn creates_from_candidate_zone() {
    let zone = CandidateZone::new(15, "segment-999".to_string());
    let id = SegmentZoneId::from_zone(&zone);
    assert_eq!(id.segment_id(), "segment-999");
    assert_eq!(id.zone_id(), 15);
}

#[test]
fn normalizes_segment_id_with_prefix() {
    let id = SegmentZoneId::new("segment-abc".to_string(), 7);
    let (normalized_seg, normalized_zone) = id.normalized();
    assert_eq!(normalized_seg, "abc");
    assert_eq!(normalized_zone, 7);
}

#[test]
fn normalizes_segment_id_without_prefix() {
    let id = SegmentZoneId::new("xyz".to_string(), 3);
    let (normalized_seg, normalized_zone) = id.normalized();
    assert_eq!(normalized_seg, "xyz");
    assert_eq!(normalized_zone, 3);
}

#[test]
fn normalizes_empty_segment_id() {
    let id = SegmentZoneId::new("".to_string(), 1);
    let (normalized_seg, normalized_zone) = id.normalized();
    assert_eq!(normalized_seg, "");
    assert_eq!(normalized_zone, 1);
}

#[test]
fn normalizes_segment_prefix_only() {
    let id = SegmentZoneId::new("segment-".to_string(), 2);
    let (normalized_seg, normalized_zone) = id.normalized();
    assert_eq!(normalized_seg, "");
    assert_eq!(normalized_zone, 2);
}

#[test]
fn equality_works_correctly() {
    let id1 = SegmentZoneId::new("segment-100".to_string(), 5);
    let id2 = SegmentZoneId::new("segment-100".to_string(), 5);
    let id3 = SegmentZoneId::new("segment-200".to_string(), 5);
    let id4 = SegmentZoneId::new("segment-100".to_string(), 10);

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
    assert_ne!(id1, id4);
}

#[test]
fn equality_does_not_normalize_for_direct_comparison() {
    // Two IDs that normalize to the same value should NOT be equal
    // because equality compares the raw segment_id
    let id1 = SegmentZoneId::new("segment-abc".to_string(), 1);
    let id2 = SegmentZoneId::new("abc".to_string(), 1);

    assert_ne!(id1, id2);

    // But their normalized forms should match
    assert_eq!(id1.normalized(), id2.normalized());
}

#[test]
fn works_in_hashset() {
    let mut set = HashSet::new();
    let id1 = SegmentZoneId::new("segment-100".to_string(), 5);
    let id2 = SegmentZoneId::new("segment-100".to_string(), 5);
    let id3 = SegmentZoneId::new("segment-200".to_string(), 5);

    assert!(set.insert(id1.clone()));
    assert!(!set.insert(id2)); // duplicate, should return false
    assert!(set.insert(id3));

    assert_eq!(set.len(), 2);
    assert!(set.contains(&id1));
}

#[test]
fn clone_works_correctly() {
    let id1 = SegmentZoneId::new("segment-clone".to_string(), 99);
    let id2 = id1.clone();

    assert_eq!(id1, id2);
    assert_eq!(id1.segment_id(), id2.segment_id());
    assert_eq!(id1.zone_id(), id2.zone_id());
}

#[test]
fn debug_format_is_readable() {
    let id = SegmentZoneId::new("segment-debug".to_string(), 42);
    let debug_str = format!("{:?}", id);
    assert!(debug_str.contains("segment-debug"));
    assert!(debug_str.contains("42"));
}

#[test]
fn handles_numeric_segment_ids() {
    let id = SegmentZoneId::new("segment-12345".to_string(), 0);
    let (normalized, _) = id.normalized();
    assert_eq!(normalized, "12345");
}

#[test]
fn handles_segment_id_with_multiple_hyphens() {
    let id = SegmentZoneId::new("segment-test-multi-hyphen".to_string(), 1);
    let (normalized, _) = id.normalized();
    // Should only strip the first "segment-" prefix
    assert_eq!(normalized, "test-multi-hyphen");
}

#[test]
fn handles_segment_word_in_middle() {
    // Should NOT strip "segment-" if it's not a prefix
    let id = SegmentZoneId::new("data-segment-123".to_string(), 1);
    let (normalized, _) = id.normalized();
    assert_eq!(normalized, "data-segment-123");
}

#[test]
fn batch_normalization_consistency() {
    // Test that normalizing multiple IDs works consistently
    let ids = vec![
        SegmentZoneId::new("segment-a".to_string(), 1),
        SegmentZoneId::new("segment-b".to_string(), 2),
        SegmentZoneId::new("c".to_string(), 3),
        SegmentZoneId::new("segment-d".to_string(), 4),
    ];

    let normalized: Vec<(&str, u32)> = ids.iter().map(|id| id.normalized()).collect();

    assert_eq!(normalized[0], ("a", 1));
    assert_eq!(normalized[1], ("b", 2));
    assert_eq!(normalized[2], ("c", 3));
    assert_eq!(normalized[3], ("d", 4));
}

#[test]
fn from_zone_with_different_prefixes() {
    let zone1 = CandidateZone::new(10, "segment-test".to_string());
    let zone2 = CandidateZone::new(20, "test".to_string());

    let id1 = SegmentZoneId::from_zone(&zone1);
    let id2 = SegmentZoneId::from_zone(&zone2);

    // They should normalize to the same segment ID
    assert_eq!(id1.normalized().0, id2.normalized().0);
    assert_ne!(id1, id2); // But not be equal as objects
}
