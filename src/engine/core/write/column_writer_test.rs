use crate::engine::core::{ColumnWriter, ZonePlan};
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;
use tempfile::tempdir;
#[tokio::test]
async fn writes_columns_and_returns_offsets() {
    // Setup temporary dir and registry
    let dir = tempdir().unwrap();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Define schema for "login" event type
    registry_factory
        .define_with_fields(
            "login",
            &[
                ("device", "string"),
                ("ip", "string"),
                ("timestamp", "u64"),
                ("event_type", "string"),
                ("context_id", "string"),
                ("index", "u32"),
            ],
        )
        .await
        .expect("Failed to define schema");

    // Generate events and wrap in a ZonePlan
    let events = EventFactory::new()
        .with("event_type", "login")
        .with("payload", json!({ "device": "mobile", "ip": "127.0.0.1" }))
        .create_list(2);

    let zone = ZonePlan {
        id: 42,
        start_index: 0,
        end_index: 1,
        events,
        uid: "uid-login".into(),
        event_type: "login".into(),
        segment_id: 7,
    };

    let writer = ColumnWriter::new(dir.path().to_path_buf(), registry);

    // Run the writer
    let offsets = writer.write_all(&[zone]).await.expect("write failed");

    // There should be offsets per field
    assert!(!offsets.as_map().is_empty());

    // Should have 6 fields × 2 events = 12 offsets
    let total_offsets: usize = offsets.as_flat_map().values().map(|v| v.len()).sum();
    assert_eq!(total_offsets, 12);

    // Verify some expected files exist
    let file_names = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert!(file_names.iter().any(|f| f.ends_with("_device.col")));
    assert!(file_names.iter().any(|f| f.ends_with("_ip.col")));
    assert!(file_names.iter().any(|f| f.ends_with("_timestamp.col")));
}
