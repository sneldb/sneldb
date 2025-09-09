use crate::engine::shard::ShardManager;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, SchemaRegistryFactory, ShardMessageFactory,
};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_store_flush_query_lifecycle() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // === Temporary environment setup ===
    let tmp_dir = tempdir().expect("Failed to create temp dir");

    // === Schema setup ===
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("payment", &[("user_id", "string"), ("amount", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();

    let wal_dir = tempdir().unwrap().into_path();

    // === ShardManager setup ===
    let manager = ShardManager::new(
        1,
        Arc::clone(&registry),
        tmp_dir.path().to_path_buf(),
        wal_dir,
    )
    .await;
    let shard = manager.get_shard("ctx-42");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // === Event creation and store ===
    let event = EventFactory::new()
        .with("event_type", "payment")
        .with("context_id", "ctx-42")
        .with(
            "payload",
            serde_json::json!({ "user_id": "max", "amount": 100 }),
        )
        .create();

    let store_msg = message_factory.store(event.clone());
    shard
        .tx
        .send(store_msg)
        .await
        .expect("Failed to send store message");

    // === Flush message ===
    let (flush_tx, _flush_rx) = tokio::sync::mpsc::channel(1);
    let flush_msg = message_factory.flush(flush_tx);
    shard
        .tx
        .send(flush_msg)
        .await
        .expect("Failed to send flush message");

    // Wait for flush completion
    sleep(Duration::from_millis(500)).await;

    // === Query message ===
    let query_cmd = CommandFactory::query()
        .with_event_type("payment")
        .with_context_id("ctx-42")
        .create();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let query_msg = message_factory.query(query_cmd, tx);
    shard
        .tx
        .send(query_msg)
        .await
        .expect("Failed to send query message");

    let results = rx.recv().await.expect("No results received");

    // === Validation ===
    assert_eq!(results.len(), 1);
    let result = &results[0];
    assert_eq!(result.context_id, "ctx-42");
    assert_eq!(result.event_type, "payment");
    assert_eq!(result.payload["user_id"], "max");
    assert_eq!(result.payload["amount"], 100);
}
