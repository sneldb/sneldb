use crate::engine::core::{CompactionWorker, Flusher, SegmentIndex};
use crate::test_helpers::factories::*;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn compacts_l0_segments_into_l1() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Step 1: Define schemas
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_logged_in";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("score", "int")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Step 2: Create and flush events for 2 segments (L0)
    for segment_id in 1..=2 {
        let segment_dir = shard_dir.join(format!("{:05}", segment_id));
        std::fs::create_dir_all(&segment_dir).unwrap();

        let events = vec![
            EventFactory::new()
                .with("event_type", event_type)
                .with("context_id", format!("ctx{}", segment_id))
                .with("payload", json!({ "score": segment_id * 10 }))
                .create(),
        ];

        let memtable = MemTableFactory::new()
            .with_capacity(2)
            .with_events(events)
            .create()
            .unwrap();

        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        flusher.flush().await.unwrap();
    }

    // Step 3: Run CompactionWorker
    let worker = CompactionWorker::new(0, shard_dir.clone(), registry.clone());
    worker.run().await.unwrap();

    // Step 4: Check that a segment-L1-* exists and only one entry in SegmentIndex
    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    assert_eq!(index.entries.len(), 1);
    let entry = &index.entries[0];
    assert!(entry.id >= 10_000);
    assert_eq!(entry.uids, vec![uid]);
}
