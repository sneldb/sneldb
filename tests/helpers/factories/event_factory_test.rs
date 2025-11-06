use crate::test_helpers::factory::Factory;
use serde_json::json;

#[cfg(test)]
#[test]
fn test_event_factory() {
    let event = Factory::event()
        .with("event_type", "login")
        .with("context_id", "ctx42")
        .with("payload", json!({ "age": 30 }))
        .create();

    assert_eq!(event.event_type, "login");
    assert_eq!(event.context_id, "ctx42");
    assert_eq!(event.payload_as_json()["age"], json!(30));
}
