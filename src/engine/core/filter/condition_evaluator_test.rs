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
fn evaluates_zone_multiple_matches_with_mask() {
    // Zone with 5 events
    let mut values = HashMap::new();
    values.insert(
        "timestamp".to_string(),
        vec![
            "10".into(),
            "20".into(),
            "30".into(),
            "40".into(),
            "50".into(),
        ],
    );
    values.insert(
        "device".to_string(),
        vec![
            "ios".into(),
            "ios".into(),
            "android".into(),
            "ios".into(),
            "web".into(),
        ],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 7)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    // Conditions: timestamp >= 20 AND device != android
    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_numeric_condition("timestamp".into(), CompareOp::Gte, 20);
    evaluator.add_string_condition("device".into(), CompareOp::Neq, "android".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    // Should match indices 1 (20, ios) and 3 (40, ios) and 4 (50, web)
    assert_eq!(results.len(), 3);
    let ts: Vec<String> = results
        .iter()
        .map(|e| e.get_field_value("timestamp"))
        .collect();
    assert_eq!(ts, vec!["20", "40", "50"]);
}
