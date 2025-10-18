use crate::engine::core::{CandidateZone, SegmentZoneId};
use std::collections::HashSet;

#[test]
fn creates_new_segment_zone_id() {
    let id = SegmentZoneId::new("00123".to_string(), 42);
    assert_eq!(id.segment_id(), "00123");
    assert_eq!(id.zone_id(), 42);
}

#[test]
fn creates_from_tuple() {
    let id = SegmentZoneId::from_tuple(("00456".to_string(), 10));
    assert_eq!(id.segment_id(), "00456");
    assert_eq!(id.zone_id(), 10);
}

#[test]
fn creates_from_str_ref_tuple() {
    let id: SegmentZoneId = ("00789", 5).into();
    assert_eq!(id.segment_id(), "00789");
    assert_eq!(id.zone_id(), 5);
}

#[test]
fn creates_from_candidate_zone() {
    let zone = CandidateZone::new(15, "00999".to_string());
    let id = SegmentZoneId::from_zone(&zone);
    assert_eq!(id.segment_id(), "00999");
    assert_eq!(id.zone_id(), 15);
}

#[test]
fn normalized_is_identity() {
    let id = SegmentZoneId::new("00042".to_string(), 7);
    let (seg, zone) = id.normalized();
    assert_eq!(seg, "00042");
    assert_eq!(zone, 7);
}

#[test]
fn equality_works_correctly() {
    let id1 = SegmentZoneId::new("00100".to_string(), 5);
    let id2 = SegmentZoneId::new("00100".to_string(), 5);
    let id3 = SegmentZoneId::new("00200".to_string(), 5);
    let id4 = SegmentZoneId::new("00100".to_string(), 10);

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
    assert_ne!(id1, id4);
}

#[test]
fn works_in_hashset() {
    let mut set = HashSet::new();
    let id1 = SegmentZoneId::new("00100".to_string(), 5);
    let id2 = SegmentZoneId::new("00100".to_string(), 5);
    let id3 = SegmentZoneId::new("00200".to_string(), 5);

    assert!(set.insert(id1.clone()));
    assert!(!set.insert(id2)); // duplicate, should return false
    assert!(set.insert(id3));

    assert_eq!(set.len(), 2);
    assert!(set.contains(&id1));
}

#[test]
fn clone_works_correctly() {
    let id1 = SegmentZoneId::new("00011".to_string(), 99);
    let id2 = id1.clone();

    assert_eq!(id1, id2);
    assert_eq!(id1.segment_id(), id2.segment_id());
    assert_eq!(id1.zone_id(), id2.zone_id());
}

#[test]
fn debug_format_is_readable() {
    let id = SegmentZoneId::new("00077".to_string(), 42);
    let debug_str = format!("{:?}", id);
    assert!(debug_str.contains("00077"));
    assert!(debug_str.contains("42"));
}
