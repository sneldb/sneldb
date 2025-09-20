use crate::engine::core::ColumnReader;
use crate::engine::core::ColumnWriter;
use crate::engine::core::ZonePlan;
use crate::engine::core::ZoneWriter;
use crate::test_helpers::factory::EventFactory;
use crate::test_helpers::factory::SchemaRegistryFactory;
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
    writer.write_all(&[zone.clone()]).await.expect("write failed");

    let writer = ZoneWriter::new("uid-login", &segment_dir, Arc::clone(&registry));
    writer.write_all(&[zone]).await.unwrap();

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
