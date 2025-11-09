use crate::engine::core::{LogicalOp, ZoneCombiner};
use crate::test_helpers::factories::CandidateZoneFactory;

#[test]
fn combines_with_and_returns_intersection() {
    let z1 = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-A")
        .create();
    let z2 = CandidateZoneFactory::new()
        .with("zone_id", 2)
        .with("segment_id", "seg-A")
        .create();
    let z3 = CandidateZoneFactory::new()
        .with("zone_id", 3)
        .with("segment_id", "seg-A")
        .create();

    let zones1 = vec![z1.clone(), z2.clone()];
    let zones2 = vec![z2.clone(), z3.clone()];

    let result = ZoneCombiner::new(vec![zones1, zones2], LogicalOp::And).combine();

    assert_eq!(result, vec![z2]);
}

#[test]
fn combines_with_or_returns_union() {
    let z1 = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-A")
        .create();
    let z2 = CandidateZoneFactory::new()
        .with("zone_id", 2)
        .with("segment_id", "seg-A")
        .create();
    let z3 = CandidateZoneFactory::new()
        .with("zone_id", 3)
        .with("segment_id", "seg-A")
        .create();

    let zones1 = vec![z1.clone(), z2.clone()];
    let zones2 = vec![z2.clone(), z3.clone()];

    let mut result = ZoneCombiner::new(vec![zones1, zones2], LogicalOp::Or).combine();
    result.sort_by_key(|z| z.zone_id);

    let mut expected = vec![z1, z2, z3];
    expected.sort_by_key(|z| z.zone_id);

    assert_eq!(result, expected);
}

#[test]
fn empty_input_returns_empty() {
    let result = ZoneCombiner::new(vec![], LogicalOp::Or).combine();
    assert!(result.is_empty());
}

#[test]
fn combines_with_not_returns_input_unchanged() {
    let z = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-B")
        .create();

    let result = ZoneCombiner::new(vec![vec![z.clone()]], LogicalOp::Not).combine();
    assert_eq!(result, vec![z]);
}

#[test]
fn combines_with_not_returns_union() {
    // For NOT operations with multiple inputs, we return the union of all zones
    // and let the condition evaluator handle the filtering
    let z1 = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-A")
        .create();
    let z2 = CandidateZoneFactory::new()
        .with("zone_id", 2)
        .with("segment_id", "seg-A")
        .create();
    let z3 = CandidateZoneFactory::new()
        .with("zone_id", 3)
        .with("segment_id", "seg-A")
        .create();

    let zones1 = vec![z1.clone(), z2.clone()];
    let zones2 = vec![z2.clone(), z3.clone()];

    let mut result = ZoneCombiner::new(vec![zones1, zones2], LogicalOp::Not).combine();
    result.sort_by_key(|z| z.zone_id);

    let mut expected = vec![z1, z2, z3];
    expected.sort_by_key(|z| z.zone_id);

    assert_eq!(result, expected);
}

#[test]
fn deduplicates_single_input() {
    let z = CandidateZoneFactory::new()
        .with("zone_id", 9)
        .with("segment_id", "seg-C")
        .create();

    let zones = vec![z.clone(), z.clone()];
    let result = ZoneCombiner::new(vec![zones], LogicalOp::Or).combine();

    assert_eq!(result, vec![z]);
}
