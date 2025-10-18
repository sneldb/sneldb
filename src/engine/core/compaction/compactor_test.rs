use crate::engine::core::Flusher;
use crate::engine::core::{Compactor, SegmentIndex, ZoneMeta};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_compactor_merges_segments_successfully() {
    // --- Setup input segments ---
    let base_dir = tempdir().unwrap();
    let input_dir = base_dir.path().join("input");
    let output_dir = base_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "user_created",
            &[("context_id", "string"), ("email", "string")],
        )
        .await
        .unwrap();

    let uid = registry.read().await.get_uid("user_created").unwrap();

    // Flush 2 input segments
    for segment_id in 0..=1 {
        let events = vec![
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 1))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 1))
                .with(
                    "payload",
                    json!({ "email": format!("ctx{}@example.com", segment_id * 3 + 1), "name": "User A" }),
                )
                .create(),
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 2))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 2))
                .with(
                    "payload",
                    json!({ "email": format!("ctx{}@example.com", segment_id * 3 + 2), "name": "User B" }),
                )
                .create(),
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 3))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 3))
                .with(
                    "payload",
                    json!({ "email": format!("ctx{}@example.com", segment_id * 3 + 3), "name": "User C" }),
                )
                .create(),
        ];

        let memtable = MemTableFactory::new().with_events(events).create().unwrap();

        let segment_dir = input_dir.join(format!("{:05}", segment_id));
        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            Arc::clone(&registry),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        flusher.flush().await.expect("Flush failed");
    }

    // --- Run Compactor ---
    let compactor = Compactor::new(
        uid.clone(),
        vec!["00000".to_string(), "00001".to_string()],
        42,
        input_dir.clone(),
        output_dir.clone(),
        Arc::clone(&registry),
    );

    compactor.run().await.expect("Compactor run failed");

    // --- Validate outputs ---
    let zones_path = output_dir.join(format!("{}.zones", uid));
    let zone_meta = ZoneMeta::load(&zones_path).expect("Load zone meta failed");
    assert!(!zone_meta.is_empty(), "Zone meta should not be empty");

    let col_path = output_dir.join(format!("{}_{}.col", uid, "context_id"));
    assert!(col_path.exists(), ".col file for uid should exist");

    let col_path = output_dir.join(format!("{}_{}.col", uid, "email"));
    assert!(col_path.exists(), ".col file for uid should exist");

    let xf_path = output_dir.join(format!("{}_{}.xf", uid, "context_id"));
    assert!(xf_path.exists(), ".xf file should exist");

    let xf_path = output_dir.join(format!("{}_{}.xf", uid, "email"));
    assert!(xf_path.exists(), ".xf file should exist");

    let index_path = output_dir.join(format!("{}.idx", uid));
    assert!(index_path.exists(), ".idx file should exist");

    let index = SegmentIndex::load(&output_dir)
        .await
        .expect("Load index failed");
    let matching = index.list_for_uid(&uid);
    assert!(
        !matching.is_empty(),
        "Segment index should contain entry for uid"
    );
}
