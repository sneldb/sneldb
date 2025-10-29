use crate::engine::core::ZoneCursor;
use serde_json::{Number, Value};
use std::collections::HashMap;

#[test]
fn test_zone_cursor_navigation() {
    let mut payload_fields = HashMap::new();
    payload_fields.insert(
        "plan".to_string(),
        vec![
            Value::String("free".to_string()),
            Value::String("pro".to_string()),
        ],
    );
    payload_fields.insert(
        "region".to_string(),
        vec![
            Value::String("eu".to_string()),
            Value::String("us".to_string()),
        ],
    );
    payload_fields.insert(
        "score".to_string(),
        vec![
            Value::Number(Number::from(10)),
            Value::Number(Number::from(20)),
        ],
    );

    let mut cursor = ZoneCursor {
        segment_id: 42,
        zone_id: 7,
        context_ids: vec!["ctx1".into(), "ctx2".into()],
        timestamps: vec!["100".into(), "200".into()],
        event_types: vec!["signup".into(), "upgrade".into()],
        payload_fields,
        pos: 0,
    };

    // Initial state
    assert_eq!(cursor.len(), 2);
    assert!(!cursor.is_empty());
    assert_eq!(cursor.peek_context_id(), Some("ctx1"));

    // First row
    let row1 = cursor.next_row().expect("Expected row1");
    assert_eq!(row1.context_id, "ctx1");
    assert_eq!(row1.timestamp, "100");
    assert_eq!(row1.event_type, "signup");
    assert_eq!(
        row1.payload.get("plan").unwrap(),
        &Value::String("free".into())
    );
    assert_eq!(
        row1.payload.get("region").unwrap(),
        &Value::String("eu".into())
    );
    assert_eq!(row1.payload.get("score").and_then(|v| v.as_i64()), Some(10));

    // After one step
    assert_eq!(cursor.peek_context_id(), Some("ctx2"));

    // Second row
    let row2 = cursor.next_row().expect("Expected row2");
    assert_eq!(row2.context_id, "ctx2");
    assert_eq!(row2.timestamp, "200");
    assert_eq!(row2.event_type, "upgrade");
    assert_eq!(
        row2.payload.get("plan").unwrap(),
        &Value::String("pro".into())
    );
    assert_eq!(
        row2.payload.get("region").unwrap(),
        &Value::String("us".into())
    );
    assert_eq!(row2.payload.get("score").and_then(|v| v.as_i64()), Some(20));

    // End of data
    assert!(cursor.next_row().is_none());
    assert_eq!(cursor.peek_context_id(), None);
}
