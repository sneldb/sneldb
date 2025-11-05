use crate::engine::types::ScalarValue;
use crate::test_helpers::factory::Factory;

#[test]
fn test_zone_cursor_from_rows() {
    let row1 = Factory::zone_row()
        .with_context_id("ctx1")
        .with_timestamp("100")
        .with_event_type("login")
        .with_payload_field("plan", "pro")
        .create();

    let row2 = Factory::zone_row()
        .with_context_id("ctx2")
        .with_timestamp("101")
        .with_event_type("login")
        .with_payload_field("plan", "premium")
        .create();

    let mut cursor = Factory::zone_cursor()
        .with_zone_id(5)
        .with_segment_id(999)
        .with_rows(vec![row1.clone(), row2.clone()])
        .create();

    assert_eq!(cursor.zone_id, 5);
    assert_eq!(cursor.segment_id, 999);
    assert_eq!(cursor.len(), 2);
    assert_eq!(
        cursor.payload_fields["plan"],
        vec![
            ScalarValue::Utf8("pro".to_string()),
            ScalarValue::Utf8("premium".to_string())
        ]
    );

    let first = cursor.next_row().unwrap();
    assert_eq!(first.context_id, "ctx1");
    assert_eq!(
        first.payload.get("plan").and_then(|v| v.as_str()),
        Some("pro")
    );

    let second = cursor.next_row().unwrap();
    assert_eq!(second.context_id, "ctx2");
    assert_eq!(
        second.payload.get("plan").and_then(|v| v.as_str()),
        Some("premium")
    );

    assert!(cursor.next_row().is_none()); // end of cursor
}
