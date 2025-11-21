use crate::engine::shard::ShardManager;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, SchemaRegistryFactory, ShardMessageFactory,
};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_query_stream_message() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");

    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();

    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-1");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Store an event first
    let event = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx-1")
        .with("payload", serde_json::json!({ "value": 42 }))
        .create();

    let store_msg = message_factory.store(event);
    shard
        .tx
        .send(store_msg)
        .await
        .expect("Failed to send store message");

    // Query stream
    let query_cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx-1")
        .create();

    let (query_stream_msg, response_rx) = message_factory.query_stream(query_cmd);
    shard
        .tx
        .send(query_stream_msg)
        .await
        .expect("Failed to send query stream message");

    // Wait for response
    let result = response_rx.await.expect("Response channel dropped");
    assert!(result.is_ok(), "Query stream should succeed");
    let handle = result.unwrap();
    assert!(std::mem::size_of_val(&handle) > 0);
}

#[tokio::test]
async fn test_replay_message() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");

    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();

    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-replay");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Store some events - same pattern as working test
    for i in 0..3 {
        let event = EventFactory::new()
            .with("event_type", "test_event")
            .with("context_id", "ctx-replay")
            .with("payload", serde_json::json!({ "value": i }))
            .create();

        let store_msg = message_factory.store(event);
        shard
            .tx
            .send(store_msg)
            .await
            .expect("Failed to send store message");
    }

    // Note: REPLAY now uses the streaming query path via QueryStream messages,
    // so it's tested through the query_stream integration tests.
}

#[tokio::test]
async fn test_shutdown_message() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");

    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();

    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-shutdown");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Store an event first
    let event = EventFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx-shutdown")
        .with("payload", serde_json::json!({ "value": 1 }))
        .create();

    let store_msg = message_factory.store(event);
    shard
        .tx
        .send(store_msg)
        .await
        .expect("Failed to send store message");

    // Send shutdown
    let (shutdown_msg, shutdown_rx) = message_factory.shutdown();
    shard
        .tx
        .send(shutdown_msg)
        .await
        .expect("Failed to send shutdown message");

    // Wait for shutdown completion
    let result = shutdown_rx
        .await
        .expect("Shutdown response channel dropped");
    assert!(result.is_ok(), "Shutdown should succeed");
}

#[tokio::test]
async fn test_query_stream_handles_empty_results() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");

    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();

    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-empty");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Query stream with no matching events
    let query_cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx-nonexistent")
        .create();

    let (query_stream_msg, response_rx) = message_factory.query_stream(query_cmd);
    shard
        .tx
        .send(query_stream_msg)
        .await
        .expect("Failed to send query stream message");

    let result = response_rx.await.expect("Response channel dropped");
    assert!(
        result.is_ok(),
        "Query stream should succeed even with empty results"
    );
}

#[tokio::test]
async fn test_await_flush_idle_shard_returns_immediately() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();
    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-await");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Idle shard - no flushes pending
    let (await_msg, await_rx) = message_factory.await_flush();
    shard
        .tx
        .send(await_msg)
        .await
        .expect("Failed to send await flush message");

    // Should return immediately for idle shard
    let result = await_rx.await.expect("Response channel dropped");
    assert!(
        result.is_ok(),
        "AwaitFlush should return immediately for idle shard"
    );
}

#[tokio::test]
async fn test_await_flush_waits_for_pending_flushes() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Failed to create temp dir");
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("value", "u64")])
        .await
        .expect("Failed to define schema");

    let registry = schema_factory.registry();
    let wal_dir = tempdir().unwrap().into_path();
    let manager = ShardManager::new(1, tmp_dir.path().to_path_buf(), wal_dir).await;
    let shard = manager.get_shard("ctx-wait");
    let message_factory = ShardMessageFactory::new(Arc::clone(&registry));

    // Store events to trigger flush
    for i in 0..10 {
        let event = EventFactory::new()
            .with("event_type", "test_event")
            .with("context_id", "ctx-wait")
            .with("payload", serde_json::json!({ "value": i }))
            .create();

        let store_msg = message_factory.store(event);
        shard
            .tx
            .send(store_msg)
            .await
            .expect("Failed to send store message");
    }

    // Trigger flush - don't wait for completion, just trigger it
    let (flush_msg, _flush_rx) = message_factory.flush();
    shard
        .tx
        .send(flush_msg)
        .await
        .expect("Failed to send flush message");

    // Give flush a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Immediately request await flush (should wait)
    let (await_msg, await_rx) = message_factory.await_flush();
    shard
        .tx
        .send(await_msg)
        .await
        .expect("Failed to send await flush message");

    // Should eventually complete when flush finishes
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), await_rx)
        .await
        .expect("AwaitFlush timed out")
        .expect("Response channel dropped");

    assert!(
        result.is_ok(),
        "AwaitFlush should complete when flush finishes"
    );
}
