use crate::command::handlers::store::handle;
use crate::engine::auth::AuthManager;
use crate::engine::core::read::result::QueryResult;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::json;
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Build valid store command
    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    // Setup duplex writer
    let (mut _reader, mut writer) = duplex(1024);

    // Call the handler
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    // Confirm message was routed to shard
    let shard = shard_manager.get_shard("ctx1");
    let (tx, mut rx) = mpsc::channel(1);

    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("test_event")
                .with_context_id("ctx1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            assert_eq!(selection.rows[0][3].to_json()["id"], json!(123))
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_normalizes_datetime_rfc3339_to_epoch_seconds() {
    use crate::logging::init_for_tests;
    use chrono::{TimeZone, Utc};
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "evt_time_norm",
            &[("id", "int"), ("created_at", "datetime")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let ts_str = "2025-09-07T12:34:56Z";
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 12, 34, 56)
        .single()
        .unwrap()
        .timestamp();

    let cmd = CommandFactory::store()
        .with_event_type("evt_time_norm")
        .with_context_id("ctx-time-norm-1")
        .with_payload(serde_json::json!({ "id": 1, "created_at": ts_str }))
        .create();

    let (mut _reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let shard = shard_manager.get_shard("ctx-time-norm-1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("evt_time_norm")
                .with_context_id("ctx-time-norm-1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            let payload = &selection.rows[0][3];
            assert_eq!(payload.to_json()["created_at"], serde_json::json!(expected));
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_normalizes_datetime_integer_units() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("evt_units", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // seconds
    let cmd1 = CommandFactory::store()
        .with_event_type("evt_units")
        .with_context_id("ctx-units-1")
        .with_payload(serde_json::json!({ "id": 1, "created_at": 1_600_000_000 }))
        .create();
    let (mut _r1, mut w1) = duplex(1024);
    handle(
        &cmd1,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w1,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // milliseconds
    let cmd2 = CommandFactory::store()
        .with_event_type("evt_units")
        .with_context_id("ctx-units-2")
        .with_payload(serde_json::json!({ "id": 2, "created_at": 1_600_000_000_000u64 }))
        .create();
    let (mut _r2, mut w2) = duplex(1024);
    handle(
        &cmd2,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w2,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // microseconds
    let cmd3 = CommandFactory::store()
        .with_event_type("evt_units")
        .with_context_id("ctx-units-3")
        .with_payload(serde_json::json!({ "id": 3, "created_at": 1_600_000_000_000_000u64 }))
        .create();
    let (mut _r3, mut w3) = duplex(1024);
    handle(
        &cmd3,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w3,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // nanoseconds
    let cmd4 = CommandFactory::store()
        .with_event_type("evt_units")
        .with_context_id("ctx-units-4")
        .with_payload(serde_json::json!({ "id": 4, "created_at": 1_600_000_000_000_000_000u64 }))
        .create();
    let (mut _r4, mut w4) = duplex(1024);
    handle(
        &cmd4,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w4,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // float seconds (floor)
    let cmd5 = CommandFactory::store()
        .with_event_type("evt_units")
        .with_context_id("ctx-units-5")
        .with_payload(serde_json::json!({ "id": 5, "created_at": 1_600_000_000.9 }))
        .create();
    let (mut _r5, mut w5) = duplex(1024);
    handle(
        &cmd5,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w5,
        &JsonRenderer,
    )
    .await
    .unwrap();

    // Query and verify normalization results
    for (ctx, expected) in [
        ("ctx-units-1", 1_600_000_000),
        ("ctx-units-2", 1_600_000_000),
        ("ctx-units-3", 1_600_000_000),
        ("ctx-units-4", 1_600_000_000),
        ("ctx-units-5", 1_600_000_000),
    ] {
        let shard = shard_manager.get_shard(ctx);
        let (tx, mut rx) = mpsc::channel(1);
        shard
            .tx
            .send(ShardMessage::Query {
                command: CommandFactory::query()
                    .with_event_type("evt_units")
                    .with_context_id(ctx)
                    .create(),
                metadata: None,
                tx,
                registry: registry.clone(),
            })
            .await
            .unwrap();

        let result = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout waiting for result")
            .expect("no data");

        match result {
            QueryResult::Selection(selection) => {
                let payload = &selection.rows[0][3];
                assert_eq!(payload.to_json()["created_at"], serde_json::json!(expected));
            }
            _ => panic!("Expected selection result, got {:?}", result),
        }
    }
}

#[tokio::test]
async fn test_store_normalizes_date_string_to_midnight() {
    use crate::logging::init_for_tests;
    use chrono::{TimeZone, Utc};
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("evt_date_norm", &[("id", "int"), ("birthdate", "date")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let d_str = "2025-09-07";
    let expected = Utc
        .with_ymd_and_hms(2025, 9, 7, 0, 0, 0)
        .single()
        .unwrap()
        .timestamp();

    let cmd = CommandFactory::store()
        .with_event_type("evt_date_norm")
        .with_context_id("ctx-date-norm-1")
        .with_payload(serde_json::json!({ "id": 2, "birthdate": d_str }))
        .create();

    let (mut _reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let shard = shard_manager.get_shard("ctx-date-norm-1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("evt_date_norm")
                .with_context_id("ctx-date-norm-1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            let payload = &selection.rows[0][3];
            assert_eq!(payload.to_json()["birthdate"], serde_json::json!(expected));
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_optional_datetime_null_passes() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "evt_optional_time",
            &[("id", "int"), ("delivered_at", "datetime | null")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("evt_optional_time")
        .with_context_id("ctx-optional-1")
        .with_payload(serde_json::json!({ "id": 3, "delivered_at": null }))
        .create();

    let (mut _reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let shard = shard_manager.get_shard("ctx-optional-1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("evt_optional_time")
                .with_context_id("ctx-optional-1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            let payload = &selection.rows[0][3];
            assert!(payload.to_json().get("delivered_at").unwrap().is_null());
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_rejects_invalid_time_string() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("evt_bad_time", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("evt_bad_time")
        .with_context_id("ctx-bad-1")
        .with_payload(serde_json::json!({ "id": 4, "created_at": "not-a-time" }))
        .create();

    let (mut reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    assert!(msg.contains("Invalid time string"));
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("") // empty
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_context_id("") // empty
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("unknown_event")
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123 })) // missing "name"
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_payload(serde_json::json!({ "id": 123, "name": "John", "invalid_field": "a string" }))
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
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
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::store()
        .with_event_type("opt_event")
        .with_payload(serde_json::json!({ "id": 42 })) // missing optional "name"
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let mut response = vec![0u8; 1024];
    let n = reader.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);

    assert!(msg.contains("OK"));
    assert!(msg.contains("Event accepted for storage"));
}

#[tokio::test]
async fn test_store_handle_accepts_datetime_string_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Define schema with a logical datetime field
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("evt_time", &[("id", "int"), ("created_at", "datetime")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // RFC3339 datetime string
    let ts_str = "2025-09-07T12:34:56Z";
    let cmd = CommandFactory::store()
        .with_event_type("evt_time")
        .with_context_id("ctx-time-1")
        .with_payload(serde_json::json!({ "id": 1, "created_at": ts_str }))
        .create();

    let (mut _reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    // Query back and ensure the row exists and carries the field
    let shard = shard_manager.get_shard("ctx-time-1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("evt_time")
                .with_context_id("ctx-time-1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            assert_eq!(selection.rows.len(), 1);
            let payload = &selection.rows[0][3];
            assert!(payload.to_json().get("created_at").is_some());
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_handle_accepts_date_string_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Define schema with a logical date field
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("evt_date", &[("id", "int"), ("birthdate", "date")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Date-only string
    let d_str = "2025-09-07";
    let cmd = CommandFactory::store()
        .with_event_type("evt_date")
        .with_context_id("ctx-date-1")
        .with_payload(serde_json::json!({ "id": 2, "birthdate": d_str }))
        .create();

    let (mut _reader, mut writer) = duplex(1024);
    handle(
        &cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    let shard = shard_manager.get_shard("ctx-date-1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("evt_date")
                .with_context_id("ctx-date-1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            assert_eq!(selection.rows.len(), 1);
            let payload = &selection.rows[0][3];
            assert!(payload.to_json().get("birthdate").is_some());
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_handler_bypass_auth_allows_storage() {
    use crate::logging::init_for_tests;
    use std::sync::Arc;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("bypass_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(Arc::clone(&shard_manager)));

    let cmd = CommandFactory::store()
        .with_event_type("bypass_event")
        .with_payload(serde_json::json!({ "id": 999 }))
        .create();

    let (_reader, mut writer) = duplex(1024);

    // Test with bypass user (should succeed even without write permission)
    handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("bypass"),
        &mut writer,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");

    // Verify event was stored
    let shard = shard_manager.get_shard("ctx1");
    let (tx, mut rx) = mpsc::channel(1);
    shard
        .tx
        .send(ShardMessage::Query {
            command: CommandFactory::query()
                .with_event_type("bypass_event")
                .with_context_id("ctx1")
                .create(),
            metadata: None,
            tx,
            registry: registry.clone(),
        })
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout waiting for result")
        .expect("no data");

    match result {
        QueryResult::Selection(selection) => {
            assert_eq!(selection.rows[0][3].to_json()["id"], json!(999));
        }
        _ => panic!("Expected selection result, got {:?}", result),
    }
}

#[tokio::test]
async fn test_store_handler_bypass_auth_vs_regular_user() {
    use crate::logging::init_for_tests;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let auth_manager = Arc::new(AuthManager::new(Arc::clone(&shard_manager)));

    // Create a regular user without write permission
    auth_manager
        .create_user("regular_user".to_string(), Some("secret".to_string()))
        .await
        .unwrap();

    let cmd = CommandFactory::store()
        .with_event_type("test_event")
        .with_payload(serde_json::json!({ "id": 123 }))
        .create();

    // Test with regular user (should fail - no write permission)
    let (mut reader1, mut writer1) = duplex(1024);
    handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("regular_user"),
        &mut writer1,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut response = vec![0u8; 1024];
    let n = reader1.read(&mut response).await.unwrap();
    let msg = String::from_utf8_lossy(&response[..n]);
    assert!(msg.contains("403") || msg.contains("Forbidden"));
    assert!(msg.contains("Write permission denied"));

    // Test with bypass user (should succeed)
    let (_reader2, mut writer2) = duplex(1024);
    handle(
        &cmd,
        shard_manager.as_ref(),
        &registry,
        Some(&auth_manager),
        Some("bypass"),
        &mut writer2,
        &JsonRenderer,
    )
    .await
    .expect("handler should not fail");
}
