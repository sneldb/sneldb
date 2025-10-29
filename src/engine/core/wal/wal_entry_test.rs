use crate::engine::core::{Event, EventId, WalEntry};
use serde_json::json;

#[test]
fn converts_event_to_wal_entry() {
    // Arrange
    let event = Event {
        context_id: "ctx42".to_string(),
        timestamp: 168000,
        event_type: "user_registered".to_string(),
        id: EventId::from(42),
        payload: json!({ "plan": "pro", "age": 30 }),
    };

    // Act
    let wal_entry = WalEntry::from_event(&event);

    // Assert
    assert_eq!(wal_entry.context_id, "ctx42");
    assert_eq!(wal_entry.timestamp, 168000);
    assert_eq!(wal_entry.event_type, "user_registered");
    assert_eq!(wal_entry.payload, json!({ "plan": "pro", "age": 30 }));
    assert_eq!(wal_entry.event_id.raw(), 42);
}
