use crate::command::handlers::query::QueryCommandHandler;
use crate::command::handlers::store;
use crate::engine::auth::AuthManager;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};
use tokio::time::{Duration, sleep};
use tracing::info;

/// Helper function to parse streaming JSON response frames
/// Returns (all_rows, row_count_from_end_frame, column_names)
fn parse_streaming_response(body: &str) -> (Vec<Vec<JsonValue>>, usize, Vec<String>) {
    let frames: Vec<&str> = body.lines().filter(|line| !line.is_empty()).collect();
    let mut all_rows = Vec::new();
    let mut row_count = 0;
    let mut column_names = Vec::new();

    for frame_str in frames {
        if let Ok(frame) = serde_json::from_str::<JsonValue>(frame_str) {
            if let Some(frame_type) = frame.get("type").and_then(|t| t.as_str()) {
                match frame_type {
                    "schema" => {
                        if let Some(columns) = frame.get("columns").and_then(|c| c.as_array()) {
                            column_names = columns
                                .iter()
                                .filter_map(|col| {
                                    col.get("name")
                                        .and_then(|n| n.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect();
                        }
                    }
                    "batch" => {
                        if let Some(rows) = frame.get("rows").and_then(|r| r.as_array()) {
                            for row in rows {
                                if let Some(row_array) = row.as_array() {
                                    all_rows.push(row_array.clone());
                                }
                            }
                        }
                    }
                    "row" => {
                        if let Some(values) = frame.get("values").and_then(|v| v.as_object()) {
                            // Convert object to array preserving column order from schema
                            let mut row = Vec::new();
                            if !column_names.is_empty() {
                                for col_name in &column_names {
                                    if let Some(value) = values.get(col_name) {
                                        row.push(value.clone());
                                    }
                                }
                            } else {
                                // Fallback: just collect values
                                for (_, value) in values {
                                    row.push(value.clone());
                                }
                            }
                            all_rows.push(row);
                        }
                    }
                    "end" => {
                        if let Some(count) = frame.get("row_count").and_then(|c| c.as_u64()) {
                            row_count = count as usize;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    (all_rows, row_count, column_names)
}

async fn query_and_get_payload(
    query_cmd: &crate::command::types::Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<crate::engine::schema::SchemaRegistry>>,
) -> Vec<JsonValue> {
    let (mut reader, mut writer) = duplex(4096);
    QueryCommandHandler::new(
        query_cmd,
        shard_manager,
        Arc::clone(registry),
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .handle()
    .await
    .expect("query should succeed");

    let mut buf = vec![0u8; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let (rows, _, column_names) = parse_streaming_response(&body);

    // Build payload objects from row data
    // Skip standard columns: context_id, event_type, timestamp, event_id
    // Everything else is payload fields
    let standard_cols = ["context_id", "event_type", "timestamp", "event_id"];
    let payload_col_indices: Vec<usize> = column_names
        .iter()
        .enumerate()
        .filter_map(|(idx, name)| {
            if standard_cols.contains(&name.as_str()) {
                None
            } else {
                Some(idx)
            }
        })
        .collect();

    rows.into_iter()
        .map(|row| {
            let mut payload_obj = serde_json::Map::new();
            for &col_idx in &payload_col_indices {
                if let Some(col_name) = column_names.get(col_idx) {
                    if let Some(value) = row.get(col_idx) {
                        payload_obj.insert(col_name.clone(), value.clone());
                    }
                }
            }
            JsonValue::Object(payload_obj)
        })
        .collect()
}

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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    // Query back to verify data was stored
    let query_cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .create();

    let (mut reader, mut writer) = duplex(1024);
    QueryCommandHandler::new(
        &query_cmd,
        &shard_manager,
        Arc::clone(&registry),
        None,
        None,
        &mut writer,
        &JsonRenderer,
    )
    .handle()
    .await
    .expect("query should succeed");

    let mut buf = vec![0u8; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Parse streaming response to find the stored data
    let (rows, _, column_names) = parse_streaming_response(&body);

    let mut found_id = false;
    for row in &rows {
        // Find the "id" column index
        if let Some(id_col_idx) = column_names.iter().position(|name| name == "id") {
            if let Some(id_val) = row.get(id_col_idx) {
                if id_val.as_i64() == Some(123) {
                    found_id = true;
                    break;
                }
            }
        }
        // Also check payload column if it exists (for cases where payload is a JSON object)
        if let Some(payload_col_idx) = column_names.iter().position(|name| name == "payload") {
            if let Some(payload_val) = row.get(payload_col_idx) {
                // Try as object first
                if let Some(payload_obj) = payload_val.as_object() {
                    if let Some(id_val) = payload_obj.get("id") {
                        if id_val.as_i64() == Some(123) {
                            found_id = true;
                            break;
                        }
                    }
                }
                // Try as string (JSON stringified)
                if let Some(payload_str) = payload_val.as_str() {
                    if let Ok(payload_json) = serde_json::from_str::<JsonValue>(payload_str) {
                        if let Some(id_val) = payload_json.get("id") {
                            if id_val.as_i64() == Some(123) {
                                found_id = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(found_id, "Should find stored event with id=123. Rows: {:?}, Columns: {:?}", rows, column_names);
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    let query_cmd = CommandFactory::query()
        .with_event_type("evt_time_norm")
        .with_context_id("ctx-time-norm-1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
    assert!(!payloads.is_empty(), "Should find stored event");
    let payload = &payloads[0];
    assert_eq!(payload["created_at"], json!(expected));
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
    store::handle(
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
    store::handle(
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
    store::handle(
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
    store::handle(
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    // Query and verify normalization results
    for (ctx, expected) in [
        ("ctx-units-1", 1_600_000_000),
        ("ctx-units-2", 1_600_000_000),
        ("ctx-units-3", 1_600_000_000),
        ("ctx-units-4", 1_600_000_000),
        ("ctx-units-5", 1_600_000_000),
    ] {
        let query_cmd = CommandFactory::query()
            .with_event_type("evt_units")
            .with_context_id(ctx)
            .create();
        let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
        assert!(!payloads.is_empty(), "Should find stored event for {}", ctx);
        let payload = &payloads[0];
        assert_eq!(payload["created_at"], json!(expected));
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    let query_cmd = CommandFactory::query()
        .with_event_type("evt_date_norm")
        .with_context_id("ctx-date-norm-1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
    assert!(!payloads.is_empty(), "Should find stored event");
    let payload = &payloads[0];
    assert_eq!(payload["birthdate"], json!(expected));
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    let query_cmd = CommandFactory::query()
        .with_event_type("evt_optional_time")
        .with_context_id("ctx-optional-1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
    assert!(!payloads.is_empty(), "Should find stored event");
    let payload = &payloads[0];
    assert!(payload.get("delivered_at").unwrap().is_null());
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
    store::handle(
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

    store::handle(
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

    store::handle(
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

    store::handle(
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

    store::handle(
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

    store::handle(
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

    store::handle(
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    // Query back and ensure the row exists and carries the field
    let query_cmd = CommandFactory::query()
        .with_event_type("evt_time")
        .with_context_id("ctx-time-1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
    assert_eq!(payloads.len(), 1, "Should find one stored event");
    let payload = &payloads[0];
    assert!(payload.get("created_at").is_some());
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    // Query back and ensure the row exists and carries the field
    let query_cmd = CommandFactory::query()
        .with_event_type("evt_date")
        .with_context_id("ctx-date-1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, &shard_manager, &registry).await;
    assert_eq!(payloads.len(), 1, "Should find one stored event");
    let payload = &payloads[0];
    assert!(payload.get("birthdate").is_some());
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
    store::handle(
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

    sleep(Duration::from_millis(100)).await;

    // Verify event was stored
    let query_cmd = CommandFactory::query()
        .with_event_type("bypass_event")
        .with_context_id("ctx1")
        .create();
    let payloads = query_and_get_payload(&query_cmd, shard_manager.as_ref(), &registry).await;
    assert!(!payloads.is_empty(), "Should find stored event");
    let payload = &payloads[0];
    assert_eq!(payload["id"], json!(999));
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
    store::handle(
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
    store::handle(
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
