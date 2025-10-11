use crate::engine::core::{CandidateZone, SegmentZoneId, ZoneFilter};
use std::collections::HashSet;

#[test]
fn creates_new_zone_filter() {
    let mut allowed = HashSet::new();
    allowed.insert(SegmentZoneId::new("segment-1".to_string(), 0));
    allowed.insert(SegmentZoneId::new("segment-1".to_string(), 1));

    let filter = ZoneFilter::new(allowed);
    assert_eq!(filter.allowed_count(), 2);
}

#[test]
fn creates_from_tuples() {
    let tuples = vec![
        ("segment-1".to_string(), 0),
        ("segment-1".to_string(), 1),
        ("segment-2".to_string(), 0),
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
    let tuples = vec![("segment-1".to_string(), 0), ("segment-1".to_string(), 1)];
    let filter = ZoneFilter::from_tuples(tuples);

    let zone = CandidateZone::new(0, "segment-1".to_string());
    assert!(filter.allows(&zone));
}

#[test]
fn rejects_zone_not_in_filter() {
    let tuples = vec![("segment-1".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);

    let zone = CandidateZone::new(1, "segment-1".to_string()); // zone_id 1 not allowed
    assert!(!filter.allows(&zone));
}

#[test]
fn allows_zone_with_normalized_segment_id() {
    // Filter has "segment-" prefix
    let tuples = vec![("segment-abc".to_string(), 5)];
    let filter = ZoneFilter::from_tuples(tuples);

    // Zone without prefix should still match after normalization
    let zone = CandidateZone::new(5, "abc".to_string());
    assert!(filter.allows(&zone));
}

#[test]
fn allows_zone_with_prefix_when_filter_has_no_prefix() {
    // Filter without "segment-" prefix
    let tuples = vec![("xyz".to_string(), 10)];
    let filter = ZoneFilter::from_tuples(tuples);

    // Zone with prefix should still match after normalization
    let zone = CandidateZone::new(10, "segment-xyz".to_string());
    assert!(filter.allows(&zone));
}

#[test]
fn allows_id_directly() {
    let tuples = vec![("segment-test".to_string(), 7)];
    let filter = ZoneFilter::from_tuples(tuples);

    let id = SegmentZoneId::new("test".to_string(), 7);
    assert!(filter.allows_id(&id));
}

#[test]
fn rejects_id_not_in_filter() {
    let tuples = vec![("segment-test".to_string(), 7)];
    let filter = ZoneFilter::from_tuples(tuples);

    let id = SegmentZoneId::new("other".to_string(), 7);
    assert!(!filter.allows_id(&id));
}

#[test]
fn apply_filters_zones_correctly() {
    let tuples = vec![
        ("segment-1".to_string(), 0),
        ("segment-1".to_string(), 2),
        ("segment-2".to_string(), 1),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = vec![
        CandidateZone::new(0, "segment-1".to_string()), // allowed
        CandidateZone::new(1, "segment-1".to_string()), // NOT allowed
        CandidateZone::new(2, "segment-1".to_string()), // allowed
        CandidateZone::new(1, "segment-2".to_string()), // allowed
        CandidateZone::new(0, "segment-3".to_string()), // NOT allowed
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 3);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(zones[0].segment_id, "segment-1");
    assert_eq!(zones[1].zone_id, 2);
    assert_eq!(zones[1].segment_id, "segment-1");
    assert_eq!(zones[2].zone_id, 1);
    assert_eq!(zones[2].segment_id, "segment-2");
}

#[test]
fn apply_handles_empty_zones_list() {
    let tuples = vec![("segment-1".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones: Vec<CandidateZone> = vec![];
    filter.apply(&mut zones);

    assert_eq!(zones.len(), 0);
}

#[test]
fn apply_with_empty_filter_removes_all_zones() {
    let filter = ZoneFilter::new(HashSet::new());

    let mut zones = vec![
        CandidateZone::new(0, "segment-1".to_string()),
        CandidateZone::new(1, "segment-1".to_string()),
    ];

    filter.apply(&mut zones);

    // Empty filter means nothing is allowed
    assert_eq!(zones.len(), 0);
}

#[test]
fn apply_preserves_zones_with_normalized_ids() {
    // Filter has segment IDs without prefix
    let tuples = vec![("abc".to_string(), 1), ("def".to_string(), 2)];
    let filter = ZoneFilter::from_tuples(tuples);

    // Zones have segment IDs with prefix
    let mut zones = vec![
        CandidateZone::new(1, "segment-abc".to_string()), // should match
        CandidateZone::new(2, "segment-def".to_string()), // should match
        CandidateZone::new(3, "segment-ghi".to_string()), // should NOT match
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 2);
    assert!(zones.iter().any(|z| z.zone_id == 1));
    assert!(zones.iter().any(|z| z.zone_id == 2));
}

#[test]
fn apply_handles_mixed_prefix_formats() {
    // Filter with mixed formats
    let tuples = vec![
        ("segment-with-prefix".to_string(), 0),
        ("without-prefix".to_string(), 1),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    // Zones also with mixed formats
    let mut zones = vec![
        CandidateZone::new(0, "with-prefix".to_string()),     // should match first tuple
        CandidateZone::new(1, "segment-without-prefix".to_string()), // should match second tuple
        CandidateZone::new(2, "something-else".to_string()),  // should NOT match
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 2);
}

#[test]
fn allowed_zones_returns_reference() {
    let tuples = vec![("segment-1".to_string(), 0), ("segment-2".to_string(), 1)];
    let filter = ZoneFilter::from_tuples(tuples);

    let allowed = filter.allowed_zones();
    assert_eq!(allowed.len(), 2);
}

#[test]
fn debug_format_is_readable() {
    let tuples = vec![("segment-1".to_string(), 0), ("segment-2".to_string(), 1)];
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
    let tuples = vec![("segment-1".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);
    assert!(!filter.is_empty());
}

#[test]
fn apply_retains_order_of_allowed_zones() {
    let tuples = vec![
        ("segment-1".to_string(), 0),
        ("segment-1".to_string(), 2),
        ("segment-1".to_string(), 4),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = vec![
        CandidateZone::new(0, "segment-1".to_string()),
        CandidateZone::new(1, "segment-1".to_string()), // will be filtered
        CandidateZone::new(2, "segment-1".to_string()),
        CandidateZone::new(3, "segment-1".to_string()), // will be filtered
        CandidateZone::new(4, "segment-1".to_string()),
    ];

    filter.apply(&mut zones);

    assert_eq!(zones.len(), 3);
    assert_eq!(zones[0].zone_id, 0);
    assert_eq!(zones[1].zone_id, 2);
    assert_eq!(zones[2].zone_id, 4);
}

#[test]
fn filter_with_duplicate_tuples_deduplicates() {
    // HashSet should automatically deduplicate
    let tuples = vec![
        ("segment-1".to_string(), 0),
        ("segment-1".to_string(), 0), // duplicate
        ("segment-1".to_string(), 1),
    ];
    let filter = ZoneFilter::from_tuples(tuples);

    assert_eq!(filter.allowed_count(), 2); // Not 3
}

#[test]
fn allows_handles_case_sensitive_segment_ids() {
    let tuples = vec![("Segment-ABC".to_string(), 0)];
    let filter = ZoneFilter::from_tuples(tuples);

    let zone_upper = CandidateZone::new(0, "Segment-ABC".to_string());
    let zone_lower = CandidateZone::new(0, "segment-abc".to_string());

    assert!(filter.allows(&zone_upper));
    assert!(!filter.allows(&zone_lower)); // Case sensitive, should not match
}

#[test]
fn large_filter_performance() {
    // Test with a larger number of zones to ensure reasonable performance
    let mut tuples = Vec::new();
    for i in 0..1000 {
        tuples.push((format!("segment-{}", i), i % 10));
    }
    let filter = ZoneFilter::from_tuples(tuples);

    let mut zones = Vec::new();
    for i in 0..2000 {
        zones.push(CandidateZone::new(i % 10, format!("segment-{}", i)));
    }

    filter.apply(&mut zones);

    // First 1000 should match, second 1000 should not
    assert_eq!(zones.len(), 1000);
}

