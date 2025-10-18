use crate::engine::core::{CandidateZone, SegmentZoneId, ZoneFilter};
use std::collections::HashSet;

#[test]
fn creates_new_zone_filter() {
    let mut allowed = HashSet::new();
    allowed.insert(SegmentZoneId::new("00001".to_string(), 0));
    allowed.insert(SegmentZoneId::new("00001".to_string(), 1));

    let filter = ZoneFilter::new(allowed);
    assert_eq!(filter.allowed_count(), 2);
}

#[test]
fn creates_from_tuples() {
    let tuples = vec![
        ("00001".to_string(), 0),
        ("00001".to_string(), 1),
        ("00002".to_string(), 0),
    ];

    let filter = ZoneFilter::from_tuples(tuples);
    assert_eq!(filter.allowed_count(), 3);
}

#[test]
fn creates_empty_filter() {
    let filter = ZoneFilter::new(HashSet::new());
    assert_eq!(filter.allowed_count(), 0);
    assert!(filter.is_empty());
}

#[test]
fn allows_zone_in_filter() {
    let tuples = vec![("00001".to_string(), 0), ("00001".to_string(), 1)];
    let filter = ZoneFilter::from_tuples(tuples);

    let zone = CandidateZone::new(0, "00001".to_string());
    assert!(filter.allows(&zone));
}

#[test]
fn rejects_zone_not_in_filter() {
    let tuples = vec![("00001".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);

    let zone = CandidateZone::new(1, "00001".to_string());
    assert!(!filter.allows(&zone));
}

#[test]
fn allows_id_directly() {
    let tuples = vec![("00077".to_string(), 7)];
    let filter = ZoneFilter::from_tuples(tuples);

    let id = SegmentZoneId::new("00077".to_string(), 7);
    assert!(filter.allows_id(&id));
}

#[test]
fn rejects_id_not_in_filter() {
    let tuples = vec![("00077".to_string(), 7)];
    let filter = ZoneFilter::from_tuples(tuples);

    let id = SegmentZoneId::new("00078".to_string(), 7);
    assert!(!filter.allows_id(&id));
}

#[test]
fn apply_filters_zones_correctly() {
    let tuples = vec![
        ("00001".to_string(), 0),
        ("00001".to_string(), 2),
        ("00002".to_string(), 1),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = vec![
        CandidateZone::new(0, "00001".to_string()),
        CandidateZone::new(1, "00001".to_string()),
        CandidateZone::new(2, "00001".to_string()),
        CandidateZone::new(1, "00002".to_string()),
        CandidateZone::new(0, "00003".to_string()),
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 3);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(zones[0].segment_id, "00001");
    assert_eq!(zones[1].zone_id, 2);
    assert_eq!(zones[1].segment_id, "00001");
    assert_eq!(zones[2].zone_id, 1);
    assert_eq!(zones[2].segment_id, "00002");
}

#[test]
fn apply_handles_empty_zones_list() {
    let tuples = vec![("00001".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones: Vec<CandidateZone> = vec![];
    filter.apply(&mut zones);

    assert_eq!(zones.len(), 0);
}

#[test]
fn apply_with_empty_filter_removes_all_zones() {
    let filter = ZoneFilter::new(HashSet::new());

    let mut zones = vec![
        CandidateZone::new(0, "00001".to_string()),
        CandidateZone::new(1, "00001".to_string()),
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 0);
}

#[test]
fn allowed_zones_returns_reference() {
    let tuples = vec![("00001".to_string(), 0), ("00002".to_string(), 1)];
    let filter = ZoneFilter::from_tuples(tuples);

    let allowed = filter.allowed_zones();
    assert_eq!(allowed.len(), 2);
}

#[test]
fn debug_format_is_readable() {
    let tuples = vec![("00001".to_string(), 0), ("00002".to_string(), 1)];
    let filter = ZoneFilter::from_tuples(tuples);

    let debug_str = format!("{:?}", filter);
    assert!(debug_str.contains("ZoneFilter"));
    assert!(debug_str.contains("allowed_count"));
    assert!(debug_str.contains("2"));
}

#[test]
fn is_empty_returns_true_for_empty_filter() {
    let filter = ZoneFilter::new(HashSet::new());
    assert!(filter.is_empty());
}

#[test]
fn is_empty_returns_false_for_non_empty_filter() {
    let tuples = vec![("00001".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);
    assert!(!filter.is_empty());
}

#[test]
fn apply_retains_order_of_allowed_zones() {
    let tuples = vec![
        ("00001".to_string(), 0),
        ("00001".to_string(), 2),
        ("00001".to_string(), 4),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = vec![
        CandidateZone::new(0, "00001".to_string()),
        CandidateZone::new(1, "00001".to_string()),
        CandidateZone::new(2, "00001".to_string()),
        CandidateZone::new(3, "00001".to_string()),
        CandidateZone::new(4, "00001".to_string()),
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 3);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(zones[1].zone_id, 2);
    assert_eq!(zones[2].zone_id, 4);
}

#[test]
fn filter_with_duplicate_tuples_deduplicates() {
    let tuples = vec![
        ("00001".to_string(), 0),
        ("00001".to_string(), 0),
        ("00001".to_string(), 1),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    assert_eq!(filter.allowed_count(), 2);
}

#[test]
fn large_filter_performance() {
    let mut tuples = Vec::new();
    for i in 0..1000 {
        tuples.push((format!("{:05}", i), i % 10));
    }
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = Vec::new();
    for i in 0..2000 {
        zones.push(CandidateZone::new(i % 10, format!("{:05}", i)));
    }

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 1000);
}
