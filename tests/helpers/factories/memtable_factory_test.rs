use crate::test_helpers::factories::{EventFactory, MemTableFactory};

#[test]
fn test_memtable_factory_with_event_factory() {
    let events = EventFactory::new()
        .with("event_type", "signup")
        .with("payload", serde_json::json!({ "id": 1 }))
        .create_list(3);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events.clone())
        .create()
        .expect("MemTable should build");

    assert_eq!(memtable.len(), 3);

    let collected_contexts: Vec<_> = memtable.iter().map(|e| e.context_id.clone()).collect();
    for (i, expected) in events.iter().enumerate() {
        assert_eq!(collected_contexts[i], expected.context_id);
    }
}
