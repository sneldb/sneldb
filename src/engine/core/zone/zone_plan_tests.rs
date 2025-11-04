use crate::engine::core::ZonePlan;
use crate::test_helpers::factory::Factory;
use serde_json::{Number, Value, json};
use std::collections::HashMap;

#[test]
fn test_build_all_divides_events_into_zones() {
    let events = Factory::event().create_list(5);
    let zones = ZonePlan::build_all(&events, 2, "test_uid".into(), 42).unwrap();
    assert_eq!(zones.len(), 3);
    assert_eq!(zones[0].events.len(), 2);
    assert_eq!(zones[2].events.len(), 1);
    assert_eq!(zones[0].id, 0);
    assert_eq!(zones[1].id, 1);
    assert_eq!(zones[2].id, 2);
}

#[test]
fn test_group_by_event_type() {
    let mut events = Factory::event().create_list(4);
    events[1].event_type = "signup".into();
    events[2].event_type = "signup".into();
    let plan = ZonePlan::build_all(&events, 4, "test_uid".into(), 42)
        .unwrap()
        .remove(0);
    let grouped = plan.group_by(|e| e.event_type.clone());

    assert_eq!(grouped["test_event"].len(), 2);
    assert_eq!(grouped["signup"].len(), 2);
}

#[test]
fn test_collect_unique_field_values() {
    let mut events = Factory::event().create_list(3);
    events[0].payload = serde_json::json!({"plan": "free", "region": "eu"});
    events[1].payload = serde_json::json!({"plan": "pro", "region": "us"});
    events[2].payload = serde_json::json!({"plan": "free", "region": "us"});

    let plan = ZonePlan::build_all(&events, 3, "uid-001".into(), 10)
        .unwrap()
        .remove(0);
    let values = plan.collect_unique_field_values();

    assert_eq!(values[&("uid-001".into(), "plan".into())].len(), 2);
    assert_eq!(values[&("uid-001".into(), "region".into())].len(), 2);
}

#[test]
fn test_collect_field_values_across_zones() {
    let events1 = vec![
        Factory::event()
            .with("payload", json!({"plan": "free"}))
            .create(),
        Factory::event()
            .with("payload", json!({"plan": "pro"}))
            .create(),
    ];

    let events2 = vec![
        Factory::event()
            .with("payload", json!({"plan": "premium"}))
            .create(),
        Factory::event()
            .with("payload", json!({"plan": "pro"}))
            .create(),
    ];

    let zone1 = ZonePlan::build_all(&events1, 2, "u1".into(), 1)
        .unwrap()
        .remove(0);
    let zone2 = ZonePlan::build_all(&events2, 2, "u2".into(), 2)
        .unwrap()
        .remove(0);
    let combined = vec![zone1, zone2];

    let all_values = ZonePlan::collect_field_values(&combined);

    // Only check 'plan' fields
    assert_eq!(
        all_values[&("u1".to_string(), "plan".to_string())],
        ["free".to_string(), "pro".to_string()]
            .iter()
            .cloned()
            .collect()
    );

    assert_eq!(
        all_values[&("u2".to_string(), "plan".to_string())],
        ["premium".to_string(), "pro".to_string()]
            .iter()
            .cloned()
            .collect()
    );
}
#[test]
fn test_from_rows_constructs_zone_plan() {
    let rows = vec![
        Factory::zone_row().with_context_id("ctx1").create(),
        Factory::zone_row().with_context_id("ctx2").create(),
    ];
    let plan = ZonePlan::from_rows(rows, "u99".into(), 77, 3, 0).unwrap();

    assert_eq!(plan.uid, "u99");
    assert_eq!(plan.segment_id, 77);
    assert_eq!(plan.id, 3);
    assert_eq!(plan.start_index, 0);
    assert_eq!(plan.end_index, 1);
    assert_eq!(plan.events.len(), 2);
}

#[test]
fn test_collect_unique_field_values_includes_empty_for_missing() {
    // Test that collect_unique_field_values includes empty strings for missing fields
    // This is critical for columnar storage alignment
    let mut events = Factory::event().create_list(3);

    // Event 0: has 'country' and 'score'
    events[0].payload = json!({"country": "US", "score": 100});

    // Event 1: has 'score' but missing 'country'
    events[1].payload = json!({"score": 200});

    // Event 2: has 'country' but missing 'score'
    events[2].payload = json!({"country": "UK"});

    let plan = ZonePlan::build_all(&events, 3, "uid-test".into(), 1)
        .unwrap()
        .remove(0);

    let values = plan.collect_unique_field_values();

    // 'country' should have 3 unique values: "US", "UK", and "" (empty for event 1)
    let country_values = &values[&("uid-test".into(), "country".into())];
    assert_eq!(
        country_values.len(),
        3,
        "country should have 3 unique values"
    );
    assert!(country_values.contains("US"));
    assert!(country_values.contains("UK"));
    assert!(
        country_values.contains(""),
        "should contain empty string for missing field"
    );

    // 'score' should have 3 unique values: "100", "200", and "" (empty for event 2)
    let score_values = &values[&("uid-test".into(), "score".into())];
    assert_eq!(score_values.len(), 3, "score should have 3 unique values");
    assert!(score_values.contains("100"));
    assert!(score_values.contains("200"));
    assert!(
        score_values.contains(""),
        "should contain empty string for missing field"
    );
}

#[test]
fn test_from_rows_preserves_typed_payload() {
    let mut payload = HashMap::new();
    payload.insert("score".to_string(), Value::Number(Number::from(123)));
    payload.insert("region".to_string(), Value::String("eu".into()));

    let rows = vec![
        Factory::zone_row()
            .with_context_id("ctx-numeric")
            .with_event_type("signup")
            .with_payload_map(payload)
            .create(),
    ];

    let plan = ZonePlan::from_rows(rows, "uid-typed".into(), 55, 0, 0).unwrap();
    let event = &plan.events[0];

    assert_eq!(
        event.payload.get("score").and_then(|v| v.as_i64()),
        Some(123)
    );
    assert_eq!(
        event.payload.get("region").and_then(|v| v.as_str()),
        Some("eu")
    );
}
