use crate::test_helpers::factories::WalEntryFactory;
use serde_json::json;

#[test]
fn creates_wal_entry_with_defaults() {
    let entry = WalEntryFactory::new().create();

    assert_eq!(entry.context_id, "ctx1");
    assert_eq!(entry.timestamp, 123456);
    assert_eq!(entry.event_type, "test_event");
    assert_eq!(entry.payload, json!({ "key": "value" }));
}

#[test]
fn creates_wal_entry_with_overrides() {
    let entry = WalEntryFactory::new()
        .with("context_id", "ctx99")
        .with("timestamp", 987654)
        .with("event_type", "custom_event")
        .with("payload", json!({ "custom": "data" }))
        .create();

    assert_eq!(entry.context_id, "ctx99");
    assert_eq!(entry.timestamp, 987654);
    assert_eq!(entry.event_type, "custom_event");
    assert_eq!(entry.payload, json!({ "custom": "data" }));
}

#[test]
fn creates_multiple_wal_entries() {
    let entries = WalEntryFactory::new().create_list(3);
    assert_eq!(entries.len(), 3);

    for (i, entry) in entries.iter().enumerate() {
        assert!(entry.context_id.starts_with("ctx"));
        assert!(entry.timestamp >= 123456);
        assert_eq!(entry.event_type, "test_event");

        let payload = entry.payload.as_object().unwrap();
        assert_eq!(payload.get("index").unwrap(), &json!(i));
    }
}
