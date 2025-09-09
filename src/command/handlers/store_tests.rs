use crate::command::handlers::store::handle;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tracing::info;

#[tokio::test]
async fn test_store_handle_valid_event_is_routed() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Prepare the schema
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Create shard manager
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    // Build valid store command
    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    // Setup duplex writer
    let (mut _reader, mut writer) = duplex(1024);

    // Call the handler
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not fail");

    // Confirm message was routed to shard
    let shard = shard_manager.get_shard("ctx1");
    let (tx, mut rx) = mpsc::channel(1);

    shard
        .tx
        .send(ShardMessage::Query(
            CommandFactory::query()
                .with_event_type("test_event")
                .with_context_id("ctx1")
                .create(),
            tx,
            registry.clone(),
        ))
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    assert_eq!(result[0].payload["id"], 123);
}

#[tokio::test]
async fn test_store_handle_rejects_empty_event_type() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("") // empty
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("event_type cannot be empty"));
}

#[tokio::test]
async fn test_store_handle_rejects_empty_context_id() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_context_id("") // empty
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("context_id cannot be empty")); // same message as context_id case
}

#[tokio::test]
async fn test_store_handle_rejects_undefined_schema() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("unknown_event")
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("No schema defined for event type"));
}

#[tokio::test]
async fn test_store_handle_rejects_missing_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int"), ("name", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123 })) // missing "name"
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("Missing field 'name'"));
}

#[tokio::test]
async fn test_store_handle_rejects_extra_fields_in_payload() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int"), ("name", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123, "name": "John", "invalid_field": "a string" }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    info!("msg: {}", msg);
    assert!(msg.contains("Payload contains fields not defined in schema"));
}

#[tokio::test]
async fn test_store_handle_accepts_missing_optional_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("opt_event", &[("id", "int"), ("name", "string | null")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, registry.clone(), base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("opt_event")
        .with_payload(serde_json::json!({ "id": 42 })) // missing optional "name"
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not fail");

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("OK"));
    assert!(msg.contains("Event accepted for storage"));
}
