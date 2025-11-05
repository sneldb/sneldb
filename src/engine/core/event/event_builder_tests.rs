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
        event.payload.get("custom_field"),
        Some(&ScalarValue::Utf8("test_value".to_string()))
    );
}

#[test]
fn test_add_field_with_numbers() {
    let mut builder = EventBuilder::new();

    // Test integer
    builder.add_field("integer_field", "42");
    assert_eq!(
        builder.payload.get("integer_field"),
        Some(&ScalarValue::Int64(42))
    );

    // Test float
    builder.add_field("float_field", "3.14");
    assert_eq!(
        builder.payload.get("float_field"),
        Some(&ScalarValue::Float64(3.14))
    );

    // Test string
    builder.add_field("string_field", "hello");
    assert_eq!(
        builder.payload.get("string_field"),
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
        builder.payload.get("b_true"),
        Some(&ScalarValue::Boolean(true))
    );
    assert_eq!(
        builder.payload.get("b_false"),
        Some(&ScalarValue::Boolean(false))
    );
    assert_eq!(builder.payload.get("n_null"), Some(&ScalarValue::Null));
}

#[test]
fn test_add_field_whitespace_and_strings() {
    let mut builder = EventBuilder::new();
    builder.add_field("ws_int", "  7  ");
    builder.add_field("ws_bool", "  true ");
    builder.add_field("raw_preserve", "  spaced  value  ");

    assert_eq!(builder.payload.get("ws_int"), Some(&ScalarValue::Int64(7)));
    assert_eq!(
        builder.payload.get("ws_bool"),
        Some(&ScalarValue::Boolean(true))
    );
    // String fallback preserves original input (not trimmed)
    assert_eq!(
        builder.payload.get("raw_preserve"),
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
        builder.payload.get("big").map(|v| v.to_json()),
        Some(json!(big))
    );
    assert_eq!(
        builder.payload.get("neg"),
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
        builder.payload.get("nan"),
        Some(&ScalarValue::Utf8("NaN".into()))
    );
    assert_eq!(
        builder.payload.get("inf"),
        Some(&ScalarValue::Utf8("inf".into()))
    );
    assert_eq!(
        builder.payload.get("ninf"),
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
    assert_eq!(builder.payload.get("x"), Some(&ScalarValue::Int64(2)));
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
