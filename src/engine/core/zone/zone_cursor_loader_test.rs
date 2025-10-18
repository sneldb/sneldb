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
            &[("context_id", "string"), ("name", "string")],
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
        .with("payload", json!({ "name": "Alice" }))
        .create();

    let event2 = EventFactory::new()
        .with("event_type", "user_deleted")
        .with("context_id", "ctx2")
        .with("timestamp", 123457)
        .with("payload", json!({ "name": "Bob" }))
        .create();

    let event3 = EventFactory::new()
        .with("event_type", "user_deleted")
        .with("context_id", "ctx3")
        .with("timestamp", 123458)
        .with("payload", json!({ "name": "Charlie" }))
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
    let cursors = loader.load_all().await.expect("load_all failed");

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
        .map(|r| r.payload.get("name").unwrap().to_string())
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));

    assert_eq!(all_rows.len(), 3);
    assert_eq!(all_rows[0].context_id, "ctx1");
    assert_eq!(all_rows[1].context_id, "ctx2");

    assert_eq!(all_rows[0].timestamp, "123456");
    assert_eq!(all_rows[1].timestamp, "123457");

    assert_eq!(cursors.len(), expected_zones);
}
