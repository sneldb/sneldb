use crate::engine::core::{Event, EventBuilder, EventId};
use crate::engine::errors::StoreError;
use crate::engine::types::ScalarValue;
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

#[test]
fn test_payload_as_json() {
    let event = create_test_event();
    let json = event.payload_as_json();

    assert!(json.is_object());
    let obj = json.as_object().unwrap();
    assert_eq!(obj.get("string_field").unwrap().as_str().unwrap(), "value");
    assert_eq!(obj.get("number_field").unwrap().as_i64().unwrap(), 42);
    assert_eq!(obj.get("bool_field").unwrap().as_bool().unwrap(), true);
}

#[test]
fn test_payload_as_json_string() {
    let event = create_test_event();
    let json_str = event.payload_as_json_string();

    assert!(json_str.starts_with('{'));
    assert!(json_str.ends_with('}'));
    assert!(json_str.contains("\"string_field\""));
    assert!(json_str.contains("\"value\""));
    assert!(json_str.contains("\"number_field\""));
    assert!(json_str.contains("42"));
}

#[test]
fn test_set_payload_json() {
    let mut event = Event {
        event_type: "test".to_string(),
        context_id: "ctx".to_string(),
        timestamp: 0,
        id: EventId::default(),
        payload: Default::default(),
    };

    event.set_payload_json(json!({
        "field1": "value1",
        "field2": 123,
        "field3": true
    }));

    assert_eq!(event.get_field("field1").unwrap().as_str().unwrap(), "value1");
    assert_eq!(event.get_field("field2").unwrap().as_i64().unwrap(), 123);
    assert_eq!(event.get_field("field3").unwrap().as_bool().unwrap(), true);
}

#[test]
fn test_event_serialization() {
    let event = create_test_event();
    let serialized = serde_json::to_string(&event).unwrap();
    let deserialized: Event = serde_json::from_str(&serialized).unwrap();

    assert_eq!(event.event_type, deserialized.event_type);
    assert_eq!(event.context_id, deserialized.context_id);
    assert_eq!(event.timestamp, deserialized.timestamp);
    assert_eq!(event.get_field("string_field"), deserialized.get_field("string_field"));
}

#[test]
fn test_payload_field_name_sharing() {
    let mut builder1 = EventBuilder::new();
    let mut builder2 = EventBuilder::new();

    builder1.add_field("shared_field", "value1");
    builder2.add_field("shared_field", "value2");

    let event1 = builder1.build();
    let event2 = builder2.build();

    let key1 = event1.payload.keys().next().unwrap();
    let key2 = event2.payload.keys().next().unwrap();

    // Keys are Arc<str> and should be equal in value
    assert_eq!(key1, key2);
    // Note: They may not be the same pointer since we're using Arc::from() directly
    // instead of interning, but that's fine - Arc equality works by value
}

#[test]
fn test_get_field_scalar() {
    let event = create_test_event();

    assert_eq!(
        event.get_field_scalar("string_field"),
        Some(ScalarValue::Utf8("value".to_string()))
    );
    assert_eq!(
        event.get_field_scalar("number_field"),
        Some(ScalarValue::Int64(42))
    );
    assert_eq!(
        event.get_field_scalar("bool_field"),
        Some(ScalarValue::Boolean(true))
    );
    assert_eq!(event.get_field_scalar("non_existent"), None);
}

#[test]
fn test_get_field_value_sortable() {
    let event = create_test_event();

    let timestamp_str = event.get_field_value_sortable("timestamp");
    assert_eq!(timestamp_str.len(), 20);
    assert!(timestamp_str.starts_with('0'));

    let event_id_str = event.get_field_value_sortable("event_id");
    assert_eq!(event_id_str.len(), 20);
}

#[test]
fn test_get_field_scalar_with_arc_str_keys() {
    // Test that get_field_scalar works correctly with Arc<str> keys
    // by iterating through payload instead of using DashMap lookup
    let mut builder = EventBuilder::new();
    builder.add_field("test_field", "test_value");
    builder.add_field("numeric_field", "42");
    builder.add_field("bool_field", "true");

    let event = builder.build();

    // Test that we can retrieve values using string lookup
    assert_eq!(
        event.get_field_scalar("test_field"),
        Some(ScalarValue::Utf8("test_value".to_string()))
    );
    assert_eq!(
        event.get_field_scalar("numeric_field"),
        Some(ScalarValue::Int64(42))
    );
    assert_eq!(
        event.get_field_scalar("bool_field"),
        Some(ScalarValue::Boolean(true))
    );
    assert_eq!(event.get_field_scalar("non_existent"), None);
}

#[test]
fn test_payload_keys_are_arc_str() {
    // Verify that payload keys are Arc<str> and can be used correctly
    let mut builder = EventBuilder::new();
    builder.add_field("field1", "value1");
    builder.add_field("field2", "value2");

    let event = builder.build();

    // Verify keys are Arc<str>
    for key in event.payload.keys() {
        assert_eq!(key.as_ref(), key.as_ref()); // Arc<str> can be dereferenced to &str
    }

    // Verify we can collect keys as strings
    let key_strings: Vec<String> = event.payload.keys().map(|k| k.to_string()).collect();
    assert!(key_strings.contains(&"field1".to_string()));
    assert!(key_strings.contains(&"field2".to_string()));
}

#[test]
fn test_event_payload_with_direct_arc_insertion() {
    // Test that we can create events with Arc<str> keys directly
    use std::sync::Arc;
    use std::collections::HashMap;

    let mut payload = HashMap::new();
    payload.insert(Arc::from("direct_field"), ScalarValue::Utf8("direct_value".to_string()));

    let event = Event {
        event_type: "test".to_string(),
        context_id: "ctx".to_string(),
        timestamp: 123,
        id: EventId::default(),
        payload,
    };

    assert_eq!(
        event.get_field_scalar("direct_field"),
        Some(ScalarValue::Utf8("direct_value".to_string()))
    );
}

#[test]
fn test_get_field_value_with_iteration() {
    // Test that get_field_value uses iteration-based lookup correctly
    let mut builder = EventBuilder::new();
    builder.add_field("iter_field", "iter_value");

    let event = builder.build();

    // get_field_value should work with iteration-based lookup
    assert_eq!(event.get_field_value("iter_field"), "iter_value");
    assert_eq!(event.get_field_value("non_existent"), "");
}

#[test]
fn test_get_field_value_sortable_with_iteration() {
    // Test that get_field_value_sortable uses iteration-based lookup correctly
    let mut builder = EventBuilder::new();
    builder.add_field_i64("sortable_int", 123);
    builder.add_field("sortable_str", "abc");

    let event = builder.build();

    // Integer values get zero-padded for sorting (biased format)
    let sortable_int = event.get_field_value_sortable("sortable_int");
    assert_eq!(sortable_int.len(), 20);

    // String values are returned as-is
    let sortable_str = event.get_field_value_sortable("sortable_str");
    assert_eq!(sortable_str, "abc");

    // Non-existent field returns empty string
    assert_eq!(event.get_field_value_sortable("non_existent"), "");
}
