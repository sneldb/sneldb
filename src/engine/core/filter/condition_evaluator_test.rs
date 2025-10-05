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
