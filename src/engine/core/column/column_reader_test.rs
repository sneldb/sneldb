use crate::engine::core::column::format::PhysicalType;
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
        created_at: 0,
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
        created_at: 0,
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
        created_at: 0,
    };
    let writer = ColumnWriter::new(segment_dir.clone(), Arc::clone(&registry));
    writer.write_all(&[zone]).await.unwrap();

    let out = ColumnReader::load_for_zone(&segment_dir, "3", &uid, "name", 5).unwrap();
    assert_eq!(out, vec!["alice".to_string()]);
}

#[tokio::test]
async fn load_snapshot_reports_physical_types() {
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().to_path_buf();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "metrics",
            &[
                ("metric", "i64"),
                ("score", "f64"),
                ("flag", "bool"),
                ("note", "string"),
                ("event_type", "string"),
                ("context_id", "string"),
            ],
        )
        .await
        .unwrap();

    let event1 = EventFactory::new()
        .with("event_type", "metrics")
        .with("context_id", "ctx1")
        .with("timestamp", 1_694_000_000u64)
        .with(
            "payload",
            json!({
                "metric": 10,
                "score": 1.25,
                "flag": true,
                "note": "first"
            }),
        )
        .create();
    let event2 = EventFactory::new()
        .with("event_type", "metrics")
        .with("context_id", "ctx2")
        .with("timestamp", 1_694_000_100u64)
        .with(
            "payload",
            json!({
                "metric": -5,
                "score": 0.0,
                "flag": null,
                "note": "second"
            }),
        )
        .create();

    let uid = registry.read().await.get_uid("metrics").unwrap();
    let zone = ZonePlan {
        id: 42,
        start_index: 0,
        end_index: 1,
        events: vec![event1, event2],
        uid: uid.clone(),
        event_type: "metrics".into(),
        segment_id: 8,
        created_at: 0,
    };
    let writer = ColumnWriter::new(segment_dir.clone(), Arc::clone(&registry));
    writer.write_all(&[zone]).await.unwrap();

    let metric_snapshot =
        ColumnReader::load_for_zone_snapshot(&segment_dir, "8", &uid, "metric", 42, None).unwrap();
    assert_eq!(metric_snapshot.physical_type(), PhysicalType::I64);
    assert_eq!(metric_snapshot.to_strings(), vec!["10", "-5"]);
    let metric_values = metric_snapshot.into_values();
    assert_eq!(metric_values.get_i64_at(0), Some(10));
    assert_eq!(metric_values.get_i64_at(1), Some(-5));

    let score_snapshot =
        ColumnReader::load_for_zone_snapshot(&segment_dir, "8", &uid, "score", 42, None).unwrap();
    assert_eq!(score_snapshot.physical_type(), PhysicalType::F64);
    assert_eq!(score_snapshot.to_strings(), vec!["1.25", "0"]);
    let score_values = score_snapshot.into_values();
    assert_eq!(score_values.get_f64_at(0), Some(1.25));
    assert_eq!(score_values.get_f64_at(1), Some(0.0));

    let flag_snapshot =
        ColumnReader::load_for_zone_snapshot(&segment_dir, "8", &uid, "flag", 42, None).unwrap();
    assert_eq!(flag_snapshot.physical_type(), PhysicalType::Bool);
    assert_eq!(flag_snapshot.to_strings(), vec!["true", ""]);

    let note_snapshot =
        ColumnReader::load_for_zone_snapshot(&segment_dir, "8", &uid, "note", 42, None).unwrap();
    assert_eq!(note_snapshot.physical_type(), PhysicalType::VarBytes);
    assert_eq!(note_snapshot.to_strings(), vec!["first", "second"]);
}
