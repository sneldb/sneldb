use crate::engine::core::{Event, EventId};
use crate::engine::errors::StoreError;
use serde_json::{Number, Value, json};
use std::collections::HashSet;

fn create_test_event() -> Event {
    let mut event = Event {
        event_type: "test_event".to_string(),
        context_id: "test_context".to_string(),
        timestamp: 1234567890,
        id: EventId::from(99),
        payload: Default::default(),
    };
    event.set_payload_json(json!({
        "string_field": "value",
        "number_field": 42,
        "float_field": 3.14,
        "bool_field": true
    }));
    event
}

#[test]
fn test_validate() {
    let valid_event = create_test_event();
    assert!(valid_event.validate().is_ok());

    let invalid_context = Event {
        context_id: "".to_string(),
        ..create_test_event()
    };
    assert!(matches!(
        invalid_context.validate(),
        Err(StoreError::InvalidContextId)
    ));

    let invalid_type = Event {
        event_type: "".to_string(),
        ..create_test_event()
    };
    assert!(matches!(
        invalid_type.validate(),
        Err(StoreError::InvalidEventType)
    ));
}

#[test]
fn test_get_field() {
    let event = create_test_event();

    // Test fixed fields
    assert_eq!(
        event.get_field("context_id"),
        Some(Value::String("test_context".to_string()))
    );
    assert_eq!(
        event.get_field("event_type"),
        Some(Value::String("test_event".to_string()))
    );
    assert_eq!(
        event.get_field("timestamp"),
        Some(Value::Number(1234567890.into()))
    );
    assert_eq!(event.get_field("event_id"), Some(Value::Number(99.into())));

    // Test payload fields
    assert_eq!(
        event.get_field("string_field"),
        Some(Value::String("value".to_string()))
    );
    assert_eq!(
        event.get_field("number_field"),
        Some(Value::Number(42.into()))
    );
    assert_eq!(
        event.get_field("float_field"),
        Some(Value::Number(Number::from_f64(3.14).unwrap()))
    );
    assert_eq!(event.get_field("bool_field"), Some(Value::Bool(true)));

    // Test non-existent field
    assert_eq!(event.get_field("non_existent"), None);
}

#[test]
fn test_get_field_value() {
    let event = create_test_event();

    // Test fixed fields
    assert_eq!(event.get_field_value("context_id"), "test_context");
    assert_eq!(event.get_field_value("event_type"), "test_event");
    assert_eq!(event.get_field_value("timestamp"), "1234567890");
    assert_eq!(event.get_field_value("event_id"), "99");

    // Test payload fields
    assert_eq!(event.get_field_value("string_field"), "value");
    assert_eq!(event.get_field_value("number_field"), "42");
    assert_eq!(event.get_field_value("float_field"), "3.14");
    assert_eq!(event.get_field_value("bool_field"), "true");

    // Test non-existent field
    assert_eq!(event.get_field_value("non_existent"), "");
}

#[test]
fn test_collect_all_fields() {
    let event = create_test_event();
    let fields = event.collect_all_fields();

    let expected_fields: HashSet<String> = [
        "context_id".to_string(),
        "event_type".to_string(),
        "timestamp".to_string(),
        "event_id".to_string(),
        "string_field".to_string(),
        "number_field".to_string(),
        "float_field".to_string(),
        "bool_field".to_string(),
    ]
    .into_iter()
    .collect();

    assert_eq!(fields, expected_fields);
}

#[test]
fn test_order_by() {
    let events = vec![
        Event {
            event_type: "event_b".to_string(),
            timestamp: 200,
            id: EventId::from(200),
            ..create_test_event()
        },
        Event {
            event_type: "event_a".to_string(),
            timestamp: 100,
            id: EventId::from(100),
            ..create_test_event()
        },
    ];

    // Test timestamp ordering
    let sorted_by_timestamp = Event::order_by(&events, "timestamp");
    assert_eq!(sorted_by_timestamp[0].timestamp, 100);
    assert_eq!(sorted_by_timestamp[1].timestamp, 200);

    // Test event_type ordering
    let sorted_by_type = Event::order_by(&events, "event_type");
    assert_eq!(sorted_by_type[0].event_type, "event_a");
    assert_eq!(sorted_by_type[1].event_type, "event_b");
}

#[test]
fn test_group_by() {
    let events = vec![
        Event {
            context_id: "context1".to_string(),
            event_type: "type_a".to_string(),
            ..create_test_event()
        },
        Event {
            context_id: "context1".to_string(),
            event_type: "type_b".to_string(),
            ..create_test_event()
        },
        Event {
            context_id: "context2".to_string(),
            event_type: "type_a".to_string(),
            ..create_test_event()
        },
    ];

    // Test grouping by context_id
    let grouped_by_context = Event::group_by(&events, "context_id");
    assert_eq!(grouped_by_context["context1"].len(), 2);
    assert_eq!(grouped_by_context["context2"].len(), 1);

    // Test grouping by event_type
    let grouped_by_type = Event::group_by(&events, "event_type");
    assert_eq!(grouped_by_type["type_a"].len(), 2);
    assert_eq!(grouped_by_type["type_b"].len(), 1);
}
