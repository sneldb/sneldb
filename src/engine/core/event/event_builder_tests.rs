use crate::engine::core::EventBuilder;
use crate::engine::types::ScalarValue;
use serde_json::json;

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

    assert_eq!(
        event
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "custom_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("test_value".to_string()))
    );
}

#[test]
fn test_add_field_with_numbers() {
    let mut builder = EventBuilder::new();

    // Test integer
    builder.add_field("integer_field", "42");
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "integer_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(42))
    );

    // Test float
    builder.add_field("float_field", "3.14");
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "float_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Float64(3.14))
    );

    // Test string
    builder.add_field("string_field", "hello");
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "string_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("hello".to_string()))
    );
}

#[test]
fn test_add_field_booleans_and_null() {
    let mut builder = EventBuilder::new();
    builder.add_field("b_true", "true");
    builder.add_field("b_false", "false");
    builder.add_field("n_null", "null");

    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "b_true")
            .map(|(_, v)| v),
        Some(&ScalarValue::Boolean(true))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "b_false")
            .map(|(_, v)| v),
        Some(&ScalarValue::Boolean(false))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "n_null")
            .map(|(_, v)| v),
        Some(&ScalarValue::Null)
    );
}

#[test]
fn test_add_field_whitespace_and_strings() {
    let mut builder = EventBuilder::new();
    builder.add_field("ws_int", "  7  ");
    builder.add_field("ws_bool", "  true ");
    builder.add_field("raw_preserve", "  spaced  value  ");

    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "ws_int")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(7))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "ws_bool")
            .map(|(_, v)| v),
        Some(&ScalarValue::Boolean(true))
    );
    // String fallback preserves original input (not trimmed)
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "raw_preserve")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("  spaced  value  ".to_string()))
    );
}

#[test]
fn test_add_field_large_u64_and_negative_i64() {
    let mut builder = EventBuilder::new();
    let big: u64 = u64::MAX - 5;
    builder.add_field("big", &big.to_string());
    builder.add_field("neg", "-9223372036854775808"); // i64::MIN

    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "big")
            .map(|(_, v)| v.to_json()),
        Some(json!(big))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "neg")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(i64::MIN))
    );
}

#[test]
fn test_add_field_nonfinite_float_falls_back_to_string() {
    let mut builder = EventBuilder::new();
    builder.add_field("nan", "NaN");
    builder.add_field("inf", "inf");
    builder.add_field("ninf", "-infinity");

    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "nan")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("NaN".into()))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "inf")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("inf".into()))
    );
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "ninf")
            .map(|(_, v)| v),
        Some(&ScalarValue::Utf8("-infinity".into()))
    );
}

#[test]
fn test_overrides() {
    let mut builder = EventBuilder::new();
    builder.add_field("event_type", "a");
    builder.add_field("event_type", "b");
    builder.add_field("x", "1");
    builder.add_field("x", "2");

    assert_eq!(builder.event_type, "b");
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "x")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(2))
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

#[test]
fn test_with_capacity() {
    let builder = EventBuilder::with_capacity(32);
    assert_eq!(builder.event_type, "");
    assert_eq!(builder.context_id, "");
    assert_eq!(builder.timestamp, 0);
    assert!(builder.payload.is_empty());
}

#[test]
fn test_add_field_u64() {
    let mut builder = EventBuilder::new();
    builder.add_field_u64("test_field", 42);
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "test_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(42))
    );
}

#[test]
fn test_add_field_i64() {
    let mut builder = EventBuilder::new();
    builder.add_field_i64("test_field", -42);
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "test_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Int64(-42))
    );
}

#[test]
fn test_add_field_f64() {
    let mut builder = EventBuilder::new();
    builder.add_field_f64("test_field", 3.14);
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "test_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Float64(3.14))
    );
}

#[test]
fn test_add_field_bool() {
    let mut builder = EventBuilder::new();
    builder.add_field_bool("test_field", true);
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "test_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Boolean(true))
    );
}

#[test]
fn test_add_field_null() {
    let mut builder = EventBuilder::new();
    builder.add_field_null("test_field");
    assert_eq!(
        builder
            .payload
            .iter()
            .find(|(k, _)| k.as_ref() == "test_field")
            .map(|(_, v)| v),
        Some(&ScalarValue::Null)
    );
}

#[test]
fn test_field_name_arc_str_keys() {
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
