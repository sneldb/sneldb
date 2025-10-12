use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::replay::scan::scan;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn scan_replay_with_context_id_and_time_logic() {
    use crate::logging::init_for_tests;
    init_for_tests();

    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let segment_ids = Arc::new(std::sync::RwLock::new(vec![]));

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("test_event", &[("key", "string"), ("value", "int")])
        .await
        .unwrap();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let earlier = now - 1000;
    let later = now + 1000;

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("timestamp", earlier)
            .with("payload", json!({"key": "a", "value": 1}))
            .create(),
        EventFactory::new()
            .with("event_type", "another_event")
            .with("context_id", "ctx2")
            .with("timestamp", now)
            .with("payload", json!({"key": "a", "value": 1}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx2")
            .with("timestamp", now)
            .with("payload", json!({"key": "b", "value": 5}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx3")
            .with("timestamp", later)
            .with("payload", json!({"key": "c", "value": 9}))
            .create(),
    ];
    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let passive_buffers = Arc::new(PassiveBufferSet::new(8));

    // 1. Replay ctx1 with no `since` (should include)
    let cmd = CommandFactory::replay().with_context_id("ctx1").create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].context_id, "ctx1");

    // 2. Replay ctx2 with since == now - 1 (should include)
    let since = format!("{}", now - 1);
    let cmd = CommandFactory::replay()
        .with_context_id("ctx2")
        .with_since(&since)
        .create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].context_id, "ctx2");

    // 3. Replay ctx2 with since == now + 1 (should exclude)
    let since = format!("{}", now + 1);
    let cmd = CommandFactory::replay()
        .with_context_id("ctx2")
        .with_since(&since)
        .create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 0);

    // 4. Replay ctx3 with since far in the past (should include future event)
    let since = format!("{}", now - 5000);
    let cmd = CommandFactory::replay()
        .with_context_id("ctx3")
        .with_since(&since)
        .create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].context_id, "ctx3");

    // 5. Replay ctx1 with since after its timestamp (should exclude)
    let since = format!("{}", now);
    let cmd = CommandFactory::replay()
        .with_context_id("ctx1")
        .with_since(&since)
        .create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 0);

    // 6. Replay another_event with no since (should include)
    let cmd = CommandFactory::replay()
        .with_event_type("another_event")
        .with_context_id("ctx2")
        .create();
    let result = scan(
        &cmd,
        &registry,
        dir.path(),
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].context_id, "ctx2");
    assert_eq!(result[0].event_type, "another_event");
}
