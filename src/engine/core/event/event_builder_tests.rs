use crate::engine::core::EventBuilder;
use serde_json::{Number, Value};

#[test]
fn test_new_event_builder() {
    let builder = EventBuilder::new();
    assert_eq!(builder.event_type, "");
    assert_eq!(builder.context_id, "");
    assert_eq!(builder.timestamp, 0);
    assert!(builder.payload.is_empty());
}

#[test]
fn test_build_event() {
    let mut builder = EventBuilder::new();
    builder.add_field("event_type", "test_event");
    builder.add_field("context_id", "123");
    builder.add_field("timestamp", "1234567890");
    builder.add_field("custom_field", "test_value");

    let event = builder.build();
    assert_eq!(event.event_type, "test_event");
    assert_eq!(event.context_id, "123");
    assert_eq!(event.timestamp, 1234567890);

    // Test payload
    if let Value::Object(payload) = event.payload {
        assert_eq!(payload.get("custom_field").unwrap(), "test_value");
    } else {
        panic!("Expected payload to be an Object");
    }
}

#[test]
fn test_add_field_with_numbers() {
    let mut builder = EventBuilder::new();

    // Test integer
    builder.add_field("integer_field", "42");
    assert_eq!(
        builder.payload.get("integer_field").unwrap(),
        &Value::Number(42.into())
    );

    // Test float
    builder.add_field("float_field", "3.14");
    assert_eq!(
        builder.payload.get("float_field").unwrap(),
        &Value::Number(Number::from_f64(3.14).unwrap())
    );

    // Test string
    builder.add_field("string_field", "hello");
    assert_eq!(
        builder.payload.get("string_field").unwrap(),
        &Value::String("hello".to_string())
    );
}

#[test]
fn test_add_field_with_special_fields() {
    let mut builder = EventBuilder::new();

    builder.add_field("event_type", "test_event");
    builder.add_field("context_id", "test_context");
    builder.add_field("timestamp", "1234567890");

    assert_eq!(builder.event_type, "test_event");
    assert_eq!(builder.context_id, "test_context");
    assert_eq!(builder.timestamp, 1234567890);
}

#[test]
fn test_invalid_timestamp() {
    let mut builder = EventBuilder::new();
    builder.add_field("timestamp", "invalid");
    assert_eq!(builder.timestamp, 0);
}
