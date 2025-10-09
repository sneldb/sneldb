use crate::engine::core::ConditionEvaluator;
use crate::engine::core::filter::condition::*;
use crate::test_helpers::factories::CandidateZoneFactory;
use std::collections::HashMap;

#[test]
fn evaluates_zone_with_mixed_conditions() {
    // Zone with 3 events
    let mut values = HashMap::new();
    values.insert(
        "timestamp".to_string(),
        vec!["100".into(), "200".into(), "300".into()],
    );
    values.insert(
        "context_id".to_string(),
        vec!["ctx1".into(), "ctx2".into(), "ctx3".into()],
    );
    values.insert(
        "event_type".to_string(),
        vec!["login".into(), "login".into(), "logout".into()],
    );
    values.insert(
        "device".to_string(),
        vec!["ios".into(), "android".into(), "ios".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 42)
        .with("segment_id", "1")
        .with_values(values)
        .create();

    // Conditions: timestamp >= 200 AND device == ios
    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_numeric_condition("timestamp".into(), CompareOp::Gte, 200);
    evaluator.add_string_condition("device".into(), CompareOp::Eq, "ios".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    // Should match only the 3rd event (timestamp: 300, device: ios)
    assert_eq!(results.len(), 1);

    let event = &results[0];
    assert_eq!(event.get_field_value("timestamp"), "300");
    assert_eq!(event.get_field_value("device"), "ios");
    assert_eq!(event.get_field_value("context_id"), "ctx3");
}

#[test]
fn evaluate_row_at_works_with_accessor() {
    use crate::engine::core::filter::condition::PreparedAccessor;
    // Build a small zone-like columns map with two rows
    let mut values = HashMap::new();
    values.insert("timestamp".to_string(), vec!["100".into(), "200".into()]);
    values.insert("kind".to_string(), vec!["a".into(), "b".into()]);

    // Convert to ColumnValues via CandidateZoneFactory so PreparedAccessor can index
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 7)
        .with("segment_id", "seg")
        .with_values(values)
        .create();

    let accessor = PreparedAccessor::new(&zone.values);
    let mut evaluator = ConditionEvaluator::new();
    // timestamp >= 150 AND kind == b
    evaluator.add_numeric_condition("timestamp".into(), CompareOp::Gte, 150);
    evaluator.add_string_condition("kind".into(), CompareOp::Eq, "b".into());

    // Row 0 should fail (100, a)
    assert!(!evaluator.evaluate_row_at(&accessor, 0));
    // Row 1 should pass (200, b)
    assert!(evaluator.evaluate_row_at(&accessor, 1));
}

#[test]
fn evaluates_zones_with_limit_zero_returns_empty() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["ok".into(), "ok".into(), "ok".into()],
    );
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-a")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone], Some(0));
    assert_eq!(results.len(), 0);
}

#[test]
fn evaluates_zones_with_limit_one_across_multiple_zones() {
    // Zone A: 2 matches
    let mut a_vals = HashMap::new();
    a_vals.insert("kind".to_string(), vec!["x".into(), "x".into()]);
    let zone_a = CandidateZoneFactory::new()
        .with("zone_id", 10)
        .with("segment_id", "seg-a")
        .with_values(a_vals)
        .create();

    // Zone B: 3 matches
    let mut b_vals = HashMap::new();
    b_vals.insert("kind".to_string(), vec!["x".into(), "x".into(), "x".into()]);
    let zone_b = CandidateZoneFactory::new()
        .with("zone_id", 11)
        .with("segment_id", "seg-b")
        .with_values(b_vals)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("kind".into(), CompareOp::Eq, "x".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone_a, zone_b], Some(1));
    assert_eq!(results.len(), 1);
}

#[test]
fn evaluates_zones_with_limit_exact_boundary_stops_mid_zone() {
    // 5 rows, all matching; set limit to 3
    let mut values = HashMap::new();
    values.insert(
        "flag".to_string(),
        vec!["t".into(), "t".into(), "t".into(), "t".into(), "t".into()],
    );
    values.insert(
        "seq".to_string(),
        vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
    );
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 99)
        .with("segment_id", "seg-x")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("flag".into(), CompareOp::Eq, "t".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone], Some(3));
    assert_eq!(results.len(), 3);
    // Ensure first three rows in order were captured
    assert_eq!(results[0].get_field_value("seq"), "1");
    assert_eq!(results[1].get_field_value("seq"), "2");
    assert_eq!(results[2].get_field_value("seq"), "3");
}
