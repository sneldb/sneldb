use crate::engine::core::ZoneCursor;
use std::collections::HashMap;

#[test]
fn test_zone_cursor_navigation() {
    let mut payload_fields = HashMap::new();
    payload_fields.insert(
        "plan".to_string(),
        vec!["free".to_string(), "pro".to_string()],
    );
    payload_fields.insert(
        "region".to_string(),
        vec!["eu".to_string(), "us".to_string()],
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
    assert_eq!(row1.payload.get("plan").unwrap(), "free");
    assert_eq!(row1.payload.get("region").unwrap(), "eu");

    // After one step
    assert_eq!(cursor.peek_context_id(), Some("ctx2"));

    // Second row
    let row2 = cursor.next_row().expect("Expected row2");
    assert_eq!(row2.context_id, "ctx2");
    assert_eq!(row2.timestamp, "200");
    assert_eq!(row2.event_type, "upgrade");
    assert_eq!(row2.payload.get("plan").unwrap(), "pro");
    assert_eq!(row2.payload.get("region").unwrap(), "us");

    // End of data
    assert!(cursor.next_row().is_none());
    assert_eq!(cursor.peek_context_id(), None);
}
