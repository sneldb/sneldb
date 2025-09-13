use crate::command::handlers::query::handle;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};

#[tokio::test]
async fn test_query_returns_no_results_when_nothing_matches() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Prepare registry
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Start shard manager
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // No STORE command beforehand → should result in no matches
    let cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx_missing") // does not exist
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not fail");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("No matching events found"),
        "Expected message about missing results, got: {}",
        body
    );
}

#[tokio::test]
async fn test_query_returns_matching_event_as_json() {
    use crate::command::handlers::query::handle;
    use crate::logging::init_for_tests;
    use crate::shared::response::JsonRenderer;
    use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
    use tokio::io::{AsyncReadExt, duplex};

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    init_for_tests();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store a matching event
    let store_cmd = CommandFactory::store()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({ "id": 42 }))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    // Query for it
    let query_cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .create();
    let (mut reader, mut writer) = duplex(1024);
    handle(
        &query_cmd,
        &shard_manager,
        &registry,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut buf = vec![0; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"id\":42"));
}

#[tokio::test]
async fn test_query_returns_error_for_empty_event_type() {
    use crate::command::handlers::query::handle;
    use crate::logging::init_for_tests;
    use crate::shared::response::JsonRenderer;
    use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
    use tokio::io::{AsyncReadExt, duplex};

    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::query()
        .with_event_type("") // Invalid
        .with_context_id("ctx1")
        .create();
    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 512];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("event_type cannot be empty"));
}
