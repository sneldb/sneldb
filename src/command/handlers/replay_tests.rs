use crate::command::handlers::{replay::handle, store};
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};

#[tokio::test]
async fn test_replay_returns_matching_event() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("replay_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // First, store the event
    let store_cmd = CommandFactory::store()
        .with_event_type("replay_event")
        .with_context_id("ctx42")
        .with_payload(serde_json::json!({ "id": 99 }))
        .create();

    let (mut _r, mut w) = duplex(1024);
    store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Replay the event
    let replay_cmd = CommandFactory::replay()
        .with_event_type("replay_event")
        .with_context_id("ctx42")
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(
        &replay_cmd,
        &shard_manager,
        &registry,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Streaming format returns: schema + batch + end messages
    // The batch rows are arrays: [context_id, event_type, timestamp, event_id, id]
    // We expect the id field (value 99) to be present
    assert!(
        body.contains("99"),
        "Expected id value 99 in streaming response, got: {}",
        body
    );
    assert!(
        body.contains("ctx42"),
        "Expected context_id ctx42 in streaming response, got: {}",
        body
    );
    assert!(
        body.contains("replay_event"),
        "Expected event_type replay_event in streaming response, got: {}",
        body
    );
}

#[tokio::test]
async fn test_replay_returns_error_for_empty_context_id() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::replay()
        .with_event_type("replay_event")
        .with_context_id("") // Invalid
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 512];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("context_id cannot be empty"));
}

#[tokio::test]
async fn test_replay_returns_no_results() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("replay_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::replay()
        .with_event_type("replay_event")
        .with_context_id("not_exist_ctx")
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Streaming format returns row_count: 0 in the end message
    assert!(
        body.contains("\"row_count\":0"),
        "Expected empty result set with row_count:0, got: {}",
        body
    );
}

#[tokio::test]
async fn test_replay_handles_wildcard_event_type() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event1", &[("id", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event2", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events of different types
    let store_cmd1 = CommandFactory::store()
        .with_event_type("event1")
        .with_context_id("ctx-wildcard")
        .with_payload(serde_json::json!({ "id": 1 }))
        .create();

    let store_cmd2 = CommandFactory::store()
        .with_event_type("event2")
        .with_context_id("ctx-wildcard")
        .with_payload(serde_json::json!({ "id": 2 }))
        .create();

    let (mut _r1, mut w1) = duplex(1024);
    store::handle(
        &store_cmd1,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w1,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let (mut _r2, mut w2) = duplex(1024);
    store::handle(
        &store_cmd2,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w2,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Replay with wildcard event type
    let replay_cmd = CommandFactory::replay()
        .with_event_type("*") // Wildcard
        .with_context_id("ctx-wildcard")
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(&replay_cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return events from both event types
    assert!(
        body.contains("ctx-wildcard"),
        "Expected context_id in response, got: {}",
        body
    );
    // Response should contain data (not an error)
    assert!(
        !body.contains("\"status\":\"error\""),
        "Expected successful response, got error: {}",
        body
    );
}

#[tokio::test]
async fn test_replay_handles_whitespace_only_context_id() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Test with whitespace-only context_id (should be treated as empty)
    let cmd = CommandFactory::replay()
        .with_event_type("test_event")
        .with_context_id("   ") // Whitespace only
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 512];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("context_id cannot be empty"),
        "Expected error for whitespace-only context_id, got: {}",
        body
    );
}

#[tokio::test]
async fn test_replay_with_since_timestamp() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("timestamped_event", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store an event
    let store_cmd = CommandFactory::store()
        .with_event_type("timestamped_event")
        .with_context_id("ctx-time")
        .with_payload(serde_json::json!({ "value": 42 }))
        .create();

    let (mut _r, mut w) = duplex(1024);
    store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Replay with since timestamp
    let replay_cmd = CommandFactory::replay()
        .with_event_type("timestamped_event")
        .with_context_id("ctx-time")
        .with_since("2020-01-01T00:00:00Z")
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(&replay_cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should handle since parameter without error
    assert!(
        !body.contains("\"status\":\"error\""),
        "Expected successful response with since parameter, got error: {}",
        body
    );
}
