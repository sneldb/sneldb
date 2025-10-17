use crate::engine::core::{ColumnReader, ColumnWriter, ZonePlan};
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn reads_column_values_for_zone() {
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().to_path_buf();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Define schema for "login"
    registry_factory
        .define_with_fields(
            "login",
            &[
                ("device", "string"),
                ("ip", "string"),
                ("timestamp", "u64"),
                ("event_type", "string"),
                ("context_id", "string"),
            ],
        )
        .await
        .unwrap();

    // Prepare zone with 2 events
    let events = EventFactory::new()
        .with("event_type", "login")
        .with(
            "payload",
            json!({ "device": "laptop", "ip": "192.168.0.1" }),
        )
        .create_list(2);

    let uid = registry.read().await.get_uid("login").unwrap();

    let zone = ZonePlan {
        id: 101,
        start_index: 0,
        end_index: 1,
        events,
        uid: uid.clone(),
        event_type: "login".into(),
        segment_id: 7,
    };

    // Write columns
    let writer = ColumnWriter::new(segment_dir.clone(), Arc::clone(&registry));
    writer
        .write_all(&[zone.clone()])
        .await
        .expect("write failed");

    // Read back device field for the zone
    let values = ColumnReader::load_for_zone(
        &segment_dir,
        "7",      // segment_id
        &uid,     // uid
        "device", // field
        101,      // zone_id
    )
    .expect("read failed");

    assert_eq!(values.len(), 2);
    assert!(values.iter().all(|v| v == "laptop"));
}

#[tokio::test]
async fn zero_copy_numeric_and_bounds_checks() {
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().to_path_buf();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "metrics",
            &[
                ("value", "i64"),
                ("event_type", "string"),
                ("context_id", "string"),
            ],
        )
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", "metrics")
        .with("payload", json!({ "value": 123 }))
        .create_list(1);

    let uid = registry.read().await.get_uid("metrics").unwrap();
    let zone = ZonePlan {
        id: 1,
        start_index: 0,
        end_index: 0,
        events,
        uid: uid.clone(),
        event_type: "metrics".into(),
        segment_id: 1,
    };
    let writer = ColumnWriter::new(segment_dir.clone(), Arc::clone(&registry));
    writer.write_all(&[zone]).await.unwrap();

    // Zero-copy path
    let vals = ColumnReader::load_for_zone_with_cache(&segment_dir, "1", &uid, "value", 1, None)
        .expect("zero-copy");
    assert_eq!(vals.len(), 1);
    assert_eq!(vals.get_i64_at(0), Some(123));
}

#[tokio::test]
async fn legacy_vec_string_path_varbytes() {
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().to_path_buf();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "test",
            &[
                ("name", "string"),
                ("event_type", "string"),
                ("context_id", "string"),
            ],
        )
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", "test")
        .with("payload", json!({ "name": "alice" }))
        .create_list(1);

    let uid = registry.read().await.get_uid("test").unwrap();
    let zone = ZonePlan {
        id: 5,
        start_index: 0,
        end_index: 0,
        events,
        uid: uid.clone(),
        event_type: "test".into(),
        segment_id: 3,
    };
    let writer = ColumnWriter::new(segment_dir.clone(), Arc::clone(&registry));
    writer.write_all(&[zone]).await.unwrap();

    let out = ColumnReader::load_for_zone(&segment_dir, "3", &uid, "name", 5).unwrap();
    assert_eq!(out, vec!["alice".to_string()]);
}
