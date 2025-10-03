use crate::engine::core::{ColumnLoader, ZoneWriter};
use crate::test_helpers::factories::{
    CandidateZoneFactory, EventFactory, SchemaRegistryFactory, ZonePlannerFactory,
};
use serde_json::json;
use std::fs::create_dir_all;
use tempfile::tempdir;

#[tokio::test]
async fn loads_column_values_for_zone() {
    crate::logging::init_for_tests();

    // Setup test segment dir and schema registry
    let dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    registry_factory
        .define_with_fields(
            "login",
            &[("ip", "string"), ("device", "string"), ("timestamp", "u64")],
        )
        .await
        .unwrap();

    // Create events and zone plans
    let event_1 = EventFactory::new()
        .with("event_type", "login")
        .with(
            "payload",
            json!({
                "ip": "127.0.0.1",
                "device": "mobile",
                "timestamp": 123456
            }),
        )
        .create();

    let event_2 = EventFactory::new()
        .with("event_type", "login")
        .with(
            "payload",
            json!({
                "ip": "127.0.0.1",
                "device": "web",
                "timestamp": 123456
            }),
        )
        .create();

    let events = vec![event_1, event_2];
    let uid = registry.read().await.get_uid("login").unwrap();

    let planner = ZonePlannerFactory::new(events, &uid).with_uid(&uid);
    let zones = planner.plan();

    let segment_dir = dir.path().join("00001");
    create_dir_all(&segment_dir).unwrap();

    // Write columns to disk
    let writer = ZoneWriter::new(&uid, &segment_dir, registry.clone());
    writer.write_all(&zones).await.unwrap();

    // Create a candidate zone for reading
    let candidate = CandidateZoneFactory::new()
        .with("zone_id", 0)
        .with("segment_id", "00001")
        .create();

    // Load columns
    let loader = ColumnLoader::new(dir.path().to_path_buf(), uid.clone());
    let result = loader.load_all_columns(&candidate, &["device".into()]);

    // Assertions
    assert!(result.contains_key("device"));

    let devices = result.get("device").unwrap();

    assert_eq!(devices.len(), 2);
    assert!(devices.get_str_at(0).unwrap() == "mobile");
    assert!(devices.get_str_at(1).unwrap() == "web");
}
