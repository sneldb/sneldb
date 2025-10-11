use crate::engine::core::{Event, FieldComparator};
use crate::test_helpers::factory::Factory;
use serde_json::json;
use std::cmp::Ordering;

#[test]
fn compares_by_context_id() {
    let e1 = Factory::event().with("context_id", "ctx-001").create();
    let e2 = Factory::event().with("context_id", "ctx-002").create();

    assert_eq!(
        FieldComparator::compare(&e1, &e2, "context_id"),
        Ordering::Less
    );
    assert_eq!(
        FieldComparator::compare(&e2, &e1, "context_id"),
        Ordering::Greater
    );
    assert_eq!(
        FieldComparator::compare(&e1, &e1, "context_id"),
        Ordering::Equal
    );
}

#[test]
fn compares_by_event_type() {
    let e1 = Factory::event().with("event_type", "click").create();
    let e2 = Factory::event().with("event_type", "purchase").create();

    assert_eq!(
        FieldComparator::compare(&e1, &e2, "event_type"),
        Ordering::Less
    );
    assert_eq!(
        FieldComparator::compare(&e2, &e1, "event_type"),
        Ordering::Greater
    );
}

#[test]
fn compares_by_timestamp() {
    let e1 = Factory::event().with("timestamp", 1000_u64).create();
    let e2 = Factory::event().with("timestamp", 2000_u64).create();

    assert_eq!(
        FieldComparator::compare(&e1, &e2, "timestamp"),
        Ordering::Less
    );
    assert_eq!(
        FieldComparator::compare(&e2, &e1, "timestamp"),
        Ordering::Greater
    );
    assert_eq!(
        FieldComparator::compare(&e1, &e1, "timestamp"),
        Ordering::Equal
    );
}

#[test]
fn compares_payload_field_as_u64() {
    let payload1 = json!({"amount": 100});
    let payload2 = json!({"amount": 200});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "amount"), Ordering::Less);
}

#[test]
fn compares_payload_field_as_i64() {
    let payload1 = json!({"balance": -50});
    let payload2 = json!({"balance": 100});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(
        FieldComparator::compare(&e1, &e2, "balance"),
        Ordering::Less
    );
}

#[test]
fn compares_payload_field_as_f64() {
    let payload1 = json!({"price": 19.99});
    let payload2 = json!({"price": 29.99});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "price"), Ordering::Less);
}

#[test]
fn compares_payload_field_as_string() {
    let payload1 = json!({"region": "EU"});
    let payload2 = json!({"region": "US"});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "region"), Ordering::Less);
}

#[test]
fn compares_missing_payload_fields() {
    let payload1 = json!({"field1": "value"});
    let payload2 = json!({"field2": "value"});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // Both missing the same field should be equal (both return empty string)
    let result = FieldComparator::compare(&e1, &e2, "missing_field");
    assert_eq!(result, Ordering::Equal);
}

#[test]
fn compares_one_missing_payload_field() {
    let payload1 = json!({"score": 100});
    let payload2 = json!({});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // e1 has field (returns "100"), e2 doesn't (returns "")
    // String comparison: "" < "100"
    let result = FieldComparator::compare(&e2, &e1, "score");
    assert_eq!(result, Ordering::Less);
}

#[test]
fn numeric_comparison_with_different_magnitudes() {
    let payload1 = json!({"value": 9});
    let payload2 = json!({"value": 100});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // Should compare numerically: 9 < 100 (not lexicographically where "9" > "100")
    assert_eq!(FieldComparator::compare(&e1, &e2, "value"), Ordering::Less);
}

#[test]
fn string_comparison_for_mixed_alphanumeric() {
    let payload1 = json!({"code": "abc123"});
    let payload2 = json!({"code": "xyz456"});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // Should fall back to string comparison
    assert_eq!(FieldComparator::compare(&e1, &e2, "code"), Ordering::Less);
}

#[test]
fn handles_zero_values() {
    let payload1 = json!({"count": 0});
    let payload2 = json!({"count": 1});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "count"), Ordering::Less);
    assert_eq!(FieldComparator::compare(&e1, &e1, "count"), Ordering::Equal);
}

#[test]
fn handles_negative_numbers() {
    let payload1 = json!({"temp": -10});
    let payload2 = json!({"temp": -5});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "temp"), Ordering::Less);
}

#[test]
fn handles_large_numbers() {
    let payload1 = json!({"big": 9999999999_u64});
    let payload2 = json!({"big": 10000000000_u64});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "big"), Ordering::Less);
}

#[test]
fn handles_decimal_precision() {
    let payload1 = json!({"rate": 0.001});
    let payload2 = json!({"rate": 0.002});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "rate"), Ordering::Less);
}

#[test]
fn sort_by_field_ascending() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"score": 30}))
            .create(),
        Factory::event()
            .with("payload", json!({"score": 10}))
            .create(),
        Factory::event()
            .with("payload", json!({"score": 20}))
            .create(),
    ];

    FieldComparator::sort_by_field(&mut events, "score", true);

    assert_eq!(events[0].get_field_value("score"), "10");
    assert_eq!(events[1].get_field_value("score"), "20");
    assert_eq!(events[2].get_field_value("score"), "30");
}

#[test]
fn sort_by_field_descending() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"score": 10}))
            .create(),
        Factory::event()
            .with("payload", json!({"score": 30}))
            .create(),
        Factory::event()
            .with("payload", json!({"score": 20}))
            .create(),
    ];

    FieldComparator::sort_by_field(&mut events, "score", false);

    assert_eq!(events[0].get_field_value("score"), "30");
    assert_eq!(events[1].get_field_value("score"), "20");
    assert_eq!(events[2].get_field_value("score"), "10");
}

#[test]
fn sort_by_timestamp() {
    let mut events = vec![
        Factory::event().with("timestamp", 3000_u64).create(),
        Factory::event().with("timestamp", 1000_u64).create(),
        Factory::event().with("timestamp", 2000_u64).create(),
    ];

    FieldComparator::sort_by_field(&mut events, "timestamp", true);

    assert_eq!(events[0].timestamp, 1000);
    assert_eq!(events[1].timestamp, 2000);
    assert_eq!(events[2].timestamp, 3000);
}

#[test]
fn sort_by_context_id() {
    let mut events = vec![
        Factory::event().with("context_id", "ctx-c").create(),
        Factory::event().with("context_id", "ctx-a").create(),
        Factory::event().with("context_id", "ctx-b").create(),
    ];

    FieldComparator::sort_by_field(&mut events, "context_id", true);

    assert_eq!(events[0].context_id, "ctx-a");
    assert_eq!(events[1].context_id, "ctx-b");
    assert_eq!(events[2].context_id, "ctx-c");
}

#[test]
fn sort_empty_list() {
    let mut events: Vec<Event> = vec![];
    FieldComparator::sort_by_field(&mut events, "timestamp", true);
    assert_eq!(events.len(), 0);
}

#[test]
fn sort_single_element() {
    let mut events = vec![Factory::event().create()];
    FieldComparator::sort_by_field(&mut events, "timestamp", true);
    assert_eq!(events.len(), 1);
}

#[test]
fn sort_with_duplicate_values() {
    let mut events = vec![
        Factory::event()
            .with("payload", json!({"priority": 2}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 1}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 2}))
            .create(),
        Factory::event()
            .with("payload", json!({"priority": 1}))
            .create(),
    ];

    FieldComparator::sort_by_field(&mut events, "priority", true);

    // First two should be priority 1, last two should be priority 2
    assert_eq!(events[0].get_field_value("priority"), "1");
    assert_eq!(events[1].get_field_value("priority"), "1");
    assert_eq!(events[2].get_field_value("priority"), "2");
    assert_eq!(events[3].get_field_value("priority"), "2");
}

#[test]
fn handles_boolean_fields_as_strings() {
    let payload1 = json!({"active": false});
    let payload2 = json!({"active": true});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // "false" < "true" lexicographically
    assert_eq!(FieldComparator::compare(&e1, &e2, "active"), Ordering::Less);
}

#[test]
fn handles_empty_string_values() {
    let payload1 = json!({"name": ""});
    let payload2 = json!({"name": "something"});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    assert_eq!(FieldComparator::compare(&e1, &e2, "name"), Ordering::Less);
}

#[test]
fn case_sensitive_string_comparison() {
    let payload1 = json!({"status": "Active"});
    let payload2 = json!({"status": "active"});

    let e1 = Factory::event().with("payload", payload1).create();
    let e2 = Factory::event().with("payload", payload2).create();

    // "Active" < "active" (uppercase letters come before lowercase in ASCII)
    assert_eq!(FieldComparator::compare(&e1, &e2, "status"), Ordering::Less);
}
