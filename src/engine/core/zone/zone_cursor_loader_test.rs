use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::zone::zone_cursor_loader::LoadedZoneCursors;
use crate::engine::core::{Flusher, ZoneCursorLoader};
use crate::shared::config::CONFIG;
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_zone_cursor_loader_e2e() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("tempdir failed");
    let base_dir = tmp_dir.path().join("shard-0");
    let segment_id = "001";
    let segment_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&segment_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields(
            "user_deleted",
            &[
                ("context_id", "string"),
                ("name", "string"),
                ("score", "i64"),
            ],
        )
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid("user_deleted")
        .unwrap()
        .to_string();

    let event1 = EventFactory::new()
        .with("event_type", "user_deleted")
        .with("context_id", "ctx1")
        .with("timestamp", 123456)
        .with("payload", json!({ "name": "Alice", "score": 10 }))
        .create();

    let event2 = EventFactory::new()
        .with("event_type", "user_deleted")
        .with("context_id", "ctx2")
        .with("timestamp", 123457)
        .with("payload", json!({ "name": "Bob", "score": 20 }))
        .create();

    let event3 = EventFactory::new()
        .with("event_type", "user_deleted")
        .with("context_id", "ctx3")
        .with("timestamp", 123458)
        .with("payload", json!({ "name": "Charlie", "score": 30 }))
        .create();

    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event1, event2, event3])
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    let loader = ZoneCursorLoader::new(
        uid.clone(),
        vec![segment_id.to_string()],
        registry,
        base_dir.clone(),
    );
    let LoadedZoneCursors {
        cursors,
        type_catalog,
    } = loader.load_all().await.expect("load_all failed");

    let num_events: usize = 3;
    let zone_size: usize = CONFIG.engine.event_per_zone;
    let expected_zones: usize = (num_events + zone_size - 1) / zone_size;
    assert_eq!(cursors.len(), expected_zones);

    let mut all_rows = vec![];
    for mut cursor in cursors.clone() {
        while let Some(row) = cursor.next_row() {
            all_rows.push(row);
        }
    }

    println!("all_rows: {:?}", all_rows);
    let names: Vec<_> = all_rows
        .iter()
        .map(|r| {
            r.payload
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));

    let scores: Vec<_> = all_rows
        .iter()
        .map(|r| r.payload.get("score").and_then(|v| v.as_i64()).unwrap())
        .collect();
    assert!(scores.contains(&10));
    assert!(scores.contains(&20));
    assert!(scores.contains(&30));

    assert_eq!(all_rows.len(), 3);
    assert_eq!(all_rows[0].context_id, "ctx1");
    assert_eq!(all_rows[1].context_id, "ctx2");

    assert_eq!(all_rows[0].timestamp, "123456");
    assert_eq!(all_rows[1].timestamp, "123457");

    assert_eq!(cursors.len(), expected_zones);

    let timestamp_key = ("user_deleted".to_string(), "timestamp".to_string());
    assert_eq!(type_catalog.get(&timestamp_key), Some(PhysicalType::I64));
    let score_key = ("user_deleted".to_string(), "score".to_string());
    assert_eq!(type_catalog.get(&score_key), Some(PhysicalType::I64));
    let name_key = ("user_deleted".to_string(), "name".to_string());
    assert_eq!(type_catalog.get(&name_key), Some(PhysicalType::VarBytes));
}

#[tokio::test]
async fn test_zone_cursor_loader_prefills_type_catalog_from_schema() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("tempdir failed");

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields(
            "orders",
            &[
                ("context_id", "string"),
                ("timestamp", "timestamp"),
                ("user_id", "u64"),
                ("price", "i64"),
                ("discount", "i64 | null"),
                ("active", "bool"),
                ("note", "string"),
            ],
        )
        .await
        .unwrap();

    let uid = registry.read().await.get_uid("orders").unwrap().to_string();

    let loader = ZoneCursorLoader::new(
        uid.clone(),
        Vec::new(),
        registry,
        tmp_dir.path().to_path_buf(),
    );
    let LoadedZoneCursors {
        cursors,
        type_catalog,
    } = loader.load_all().await.expect("load_all failed");

    assert!(cursors.is_empty());

    let event_type = "orders".to_string();
    let context_key = (event_type.clone(), "context_id".to_string());
    let timestamp_key = (event_type.clone(), "timestamp".to_string());
    let user_id_key = (event_type.clone(), "user_id".to_string());
    let price_key = (event_type.clone(), "price".to_string());
    let discount_key = (event_type.clone(), "discount".to_string());
    let active_key = (event_type.clone(), "active".to_string());
    let note_key = (event_type.clone(), "note".to_string());
    let event_type_key = (event_type.clone(), "event_type".to_string());

    assert_eq!(type_catalog.get(&context_key), Some(PhysicalType::VarBytes));
    assert_eq!(type_catalog.get(&timestamp_key), Some(PhysicalType::I64));
    assert_eq!(type_catalog.get(&user_id_key), Some(PhysicalType::U64));
    assert_eq!(type_catalog.get(&price_key), Some(PhysicalType::I64));
    assert_eq!(type_catalog.get(&discount_key), Some(PhysicalType::I64));
    assert_eq!(type_catalog.get(&active_key), Some(PhysicalType::Bool));
    assert_eq!(type_catalog.get(&note_key), Some(PhysicalType::VarBytes));
    assert_eq!(
        type_catalog.get(&event_type_key),
        Some(PhysicalType::VarBytes)
    );
}
