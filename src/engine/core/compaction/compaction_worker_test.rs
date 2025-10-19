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

    // Step 2: Create and flush events for 4 segments (L0). With compaction_k=2 in test config,
    // this should create exactly 2 L1 outputs.
    for segment_id in 1..=4 {
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

    // Step 3: Run CompactionWorker (k-way policy groups by uid with k=2)
    let worker = CompactionWorker::new(0, shard_dir.clone(), registry.clone());
    worker.run().await.unwrap();

    // Step 4: Validate index contents and on-disk artifacts
    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    // Only two L1 entries should remain for this uid; no L0 leftovers
    assert_eq!(
        index.entries.len(),
        2,
        "Only compacted outputs should remain"
    );
    for entry in &index.entries {
        assert!(
            entry.id >= 10_000,
            "All remaining entries must be L1 or higher"
        );
        assert_eq!(entry.uids, vec![uid.clone()]);
    }

    // Read back context_id values across both outputs and verify global sort and total count
    use crate::engine::core::{ColumnReader, ZoneMeta};
    let mut all_ctx = Vec::new();
    for entry in &index.entries {
        let label = format!("{:05}", entry.id);
        let out_dir = shard_dir.join(&label);
        let zones_path = out_dir.join(format!("{}.zones", uid));
        let zones = ZoneMeta::load(&zones_path).unwrap();
        for z in &zones {
            let ctx_ids =
                ColumnReader::load_for_zone(&out_dir, &label, &uid, "context_id", z.zone_id)
                    .unwrap();
            all_ctx.extend(ctx_ids);
        }
    }
    assert_eq!(all_ctx.len(), 4, "Merged outputs must contain all 4 events");
    let mut sorted = all_ctx.clone();
    sorted.sort();
    assert_eq!(all_ctx, sorted, "context_id values must be globally sorted");
}

#[tokio::test]
async fn compaction_leaves_leftover_l0_when_not_multiple_of_k() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Define schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_logged_in";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("score", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Create 5 L0 segments (k=2 -> expect 2 L1 + 1 leftover L0)
    for segment_id in 1..=5 {
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

    let worker = CompactionWorker::new(0, shard_dir.clone(), registry.clone());
    worker.run().await.unwrap();

    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    // Expect 3 entries total: 2 L1 outputs + 1 L0 leftover
    assert_eq!(index.entries.len(), 3);
    let l1_count = index.entries.iter().filter(|e| e.id >= 10_000).count();
    let l0_count = index.entries.iter().filter(|e| e.id < 10_000).count();
    assert_eq!(l1_count, 2);
    assert_eq!(l0_count, 1);
    // All entries must be for the same uid
    assert!(index.entries.iter().all(|e| e.uids == vec![uid.clone()]));
}
