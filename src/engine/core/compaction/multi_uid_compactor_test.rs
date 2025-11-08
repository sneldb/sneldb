use super::multi_uid_compactor::MultiUidCompactor;
use super::merge_plan::MergePlan;
use super::segment_batch::{SegmentBatch, UidPlan};
use crate::engine::core::compaction::handover::{CompactionHandover, SegmentCache};
use crate::engine::core::{CompactionWorker, Flusher, SegmentEntry, SegmentIndex};
use crate::engine::core::ZoneMeta;
use crate::test_helpers::factories::*;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::tempdir;

type StdRwLock<T> = std::sync::RwLock<T>;

#[derive(Clone, Default)]
struct StubCache {
    calls: Arc<StdRwLock<Vec<String>>>,
    name: &'static str,
}

impl StubCache {
    fn new(name: &'static str) -> Self {
        Self {
            calls: Arc::new(StdRwLock::new(Vec::new())),
            name,
        }
    }

    fn recorded(&self) -> Vec<String> {
        self.calls.read().unwrap().clone()
    }
}

impl SegmentCache for StubCache {
    fn invalidate_segment(&self, segment_label: &str) {
        self.calls
            .write()
            .unwrap()
            .push(format!("{}:{}", self.name, segment_label));
    }
}

#[tokio::test]
async fn compacts_multiple_uids_from_shared_segments_in_single_pass() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-multi");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Define three schemas
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_a = "event_a";
    let event_b = "event_b";
    let event_c = "event_c";
    schema_factory
        .define_with_fields(event_a, &[("context_id", "string"), ("value", "int")])
        .await
        .unwrap();
    schema_factory
        .define_with_fields(event_b, &[("context_id", "string"), ("amount", "int")])
        .await
        .unwrap();
    schema_factory
        .define_with_fields(event_c, &[("context_id", "string"), ("count", "int")])
        .await
        .unwrap();

    let uid_a = registry.read().await.get_uid(event_a).unwrap();
    let uid_b = registry.read().await.get_uid(event_b).unwrap();
    let uid_c = registry.read().await.get_uid(event_c).unwrap();

    // Flush segments with all three event types
    let segment_ids = Arc::new(StdRwLock::new(Vec::new()));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    for segment_id in 1..=4_u64 {
        let label = format!("{:05}", segment_id);
        let segment_dir = shard_dir.join(&label);
        std::fs::create_dir_all(&segment_dir).unwrap();

        let events = vec![
            EventFactory::new()
                .with("event_type", event_a)
                .with("context_id", format!("ctx-{}", segment_id))
                .with("payload", json!({ "value": segment_id as i64 * 10 }))
                .create(),
            EventFactory::new()
                .with("event_type", event_b)
                .with("context_id", format!("ctx-{}", segment_id))
                .with("payload", json!({ "amount": segment_id as i64 * 20 }))
                .create(),
            EventFactory::new()
                .with("event_type", event_c)
                .with("context_id", format!("ctx-{}", segment_id))
                .with("payload", json!({ "count": segment_id as i64 * 30 }))
                .create(),
        ];

        let memtable = MemTableFactory::new()
            .with_capacity(6)
            .with_events(events)
            .create()
            .unwrap();

        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::clone(&flush_lock),
        );
        flusher.flush().await.unwrap();

        let mut ids = segment_ids.write().unwrap();
        ids.push(label);
        ids.sort();
    }

    // Verify segments contain all three UIDs
    let index_before = SegmentIndex::load(&shard_dir).await.unwrap();
    for entry in index_before.iter_all() {
        assert_eq!(entry.uids.len(), 3);
        assert!(entry.uids.contains(&uid_a));
        assert!(entry.uids.contains(&uid_b));
        assert!(entry.uids.contains(&uid_c));
    }

    // Run compaction
    let column_stub = StubCache::new("column_handle");
    let zone_stub = StubCache::new("zone_surf");
    let zone_index_stub = StubCache::new("zone_index");
    let catalog_stub = StubCache::new("index_catalog");
    let column_block_stub = StubCache::new("column_block");
    let handover = Arc::new(CompactionHandover::with_caches(
        0,
        shard_dir.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
        Arc::new(column_stub.clone()),
        Arc::new(zone_stub.clone()),
        Arc::new(zone_index_stub.clone()),
        Arc::new(catalog_stub.clone()),
        Arc::new(column_block_stub.clone()),
    ));

    let worker = CompactionWorker::new(
        0,
        shard_dir.clone(),
        registry.clone(),
        Arc::clone(&handover),
    );
    worker.run().await.unwrap();

    // Verify all three UIDs were compacted
    let index_after = SegmentIndex::load(&shard_dir).await.unwrap();
    let mut seen_uids = HashSet::new();
    for entry in index_after.iter_all() {
        assert_eq!(entry.uids.len(), 1, "Compacted entries should have one UID");
        assert!(
            entry.id >= 10_000,
            "Compacted entries must be L1 or higher"
        );
        let uid = entry.uids.first().unwrap();
        seen_uids.insert(uid.clone());

        // Verify zones file exists
        let label = entry.label();
        let segment_dir = shard_dir.join(&label);
        let zones_path = segment_dir.join(format!("{}.zones", uid));
        assert!(
            zones_path.exists(),
            "Zones file for {uid} should exist at {:?}",
            zones_path
        );
        let zones = ZoneMeta::load(&zones_path).unwrap();
        assert!(!zones.is_empty(), "Compaction for {uid} should emit zones");
    }

    assert_eq!(seen_uids.len(), 3, "All three UIDs should be compacted");
    assert!(seen_uids.contains(&uid_a));
    assert!(seen_uids.contains(&uid_b));
    assert!(seen_uids.contains(&uid_c));

    // Verify original segments were deleted
    let ids = segment_ids.read().unwrap().clone();
    assert!(
        ids.iter().all(|label| label.parse::<u32>().unwrap() >= 10_000),
        "Only compacted L1 segments should remain"
    );
}

#[tokio::test]
async fn handles_batch_with_many_uids_efficiently() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-many");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Create 10 different event types
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let mut uids = Vec::new();
    for i in 0..10 {
        let event_type = format!("event_{}", i);
        schema_factory
            .define_with_fields(&event_type, &[("context_id", "string"), ("value", "int")])
            .await
            .unwrap();
        let uid = registry.read().await.get_uid(&event_type).unwrap();
        uids.push(uid);
    }

    // Flush segments with all event types
    let segment_ids = Arc::new(StdRwLock::new(Vec::new()));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    for segment_id in 1..=4_u64 {
        let label = format!("{:05}", segment_id);
        let segment_dir = shard_dir.join(&label);
        std::fs::create_dir_all(&segment_dir).unwrap();

        let mut events = Vec::new();
        for (i, _uid) in uids.iter().enumerate() {
            let event_type = format!("event_{}", i);
            events.push(
                EventFactory::new()
                    .with("event_type", event_type)
                    .with("context_id", format!("ctx-{}", segment_id))
                    .with("payload", json!({ "value": segment_id as i64 * (i as i64 + 1) }))
                    .create(),
            );
        }

        let memtable = MemTableFactory::new()
            .with_capacity(20)
            .with_events(events)
            .create()
            .unwrap();

        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::clone(&flush_lock),
        );
        flusher.flush().await.unwrap();

        let mut ids = segment_ids.write().unwrap();
        ids.push(label);
        ids.sort();
    }

    // Run compaction - should process all 10 UIDs efficiently
    let handover = Arc::new(CompactionHandover::new(
        0,
        shard_dir.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
    ));

    let worker = CompactionWorker::new(
        0,
        shard_dir.clone(),
        registry.clone(),
        Arc::clone(&handover),
    );
    worker.run().await.unwrap();

    // Verify all UIDs were compacted
    let index = SegmentIndex::load(&shard_dir).await.unwrap();
    let mut seen_uids = HashSet::new();
    for entry in index.iter_all() {
        assert_eq!(entry.uids.len(), 1);
        assert!(entry.id >= 10_000);
        seen_uids.insert(entry.uids.first().unwrap().clone());
    }

    assert_eq!(seen_uids.len(), 10, "All 10 UIDs should be compacted");
    for uid in &uids {
        assert!(seen_uids.contains(uid), "UID {uid} should be compacted");
    }
}

#[tokio::test]
async fn processes_empty_batch_gracefully() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-empty");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".to_string()],
        uid_plans: vec![],
    };

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let compactor = MultiUidCompactor::new(
        batch,
        shard_dir.clone(),
        shard_dir.clone(),
        registry,
    );

    let results = compactor.run().await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn handles_missing_segments_gracefully() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-missing");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "test_event";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    let batch = SegmentBatch {
        input_segment_labels: vec!["99999".to_string()], // Non-existent segment
        uid_plans: vec![UidPlan {
            uid: uid.clone(),
            output_segment_id: 10000,
        }],
    };

    let compactor = MultiUidCompactor::new(
        batch,
        shard_dir.clone(),
        shard_dir.clone(),
        registry.clone(),
    );

    // Should handle missing segments gracefully
    let result = compactor.run().await;
    // Either succeeds with empty result or fails gracefully
    match result {
        Ok(results) => {
            assert!(results.is_empty() || results.get(&uid).is_some());
        }
        Err(_) => {
            // Graceful failure is acceptable
        }
    }
}

#[tokio::test]
async fn handles_partial_uid_retirement_correctly() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-partial");
    std::fs::create_dir_all(&shard_dir).unwrap();

    // Create segments with multiple UIDs, but only compact one UID
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_a = "event_a";
    let event_b = "event_b";
    schema_factory
        .define_with_fields(event_a, &[("context_id", "string"), ("value", "int")])
        .await
        .unwrap();
    schema_factory
        .define_with_fields(event_b, &[("context_id", "string"), ("amount", "int")])
        .await
        .unwrap();

    let uid_a = registry.read().await.get_uid(event_a).unwrap();
    let uid_b = registry.read().await.get_uid(event_b).unwrap();

    // Flush segments with both event types
    let segment_ids = Arc::new(StdRwLock::new(Vec::new()));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    for segment_id in 1..=2_u64 {
        let label = format!("{:05}", segment_id);
        let segment_dir = shard_dir.join(&label);
        std::fs::create_dir_all(&segment_dir).unwrap();

        let events = vec![
            EventFactory::new()
                .with("event_type", event_a)
                .with("context_id", format!("ctx-{}", segment_id))
                .with("payload", json!({ "value": segment_id as i64 * 10 }))
                .create(),
            EventFactory::new()
                .with("event_type", event_b)
                .with("context_id", format!("ctx-{}", segment_id))
                .with("payload", json!({ "amount": segment_id as i64 * 20 }))
                .create(),
        ];

        let memtable = MemTableFactory::new()
            .with_capacity(4)
            .with_events(events)
            .create()
            .unwrap();

        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            registry.clone(),
            Arc::clone(&flush_lock),
        );
        flusher.flush().await.unwrap();

        let mut ids = segment_ids.write().unwrap();
        ids.push(label);
        ids.sort();
    }

    // Verify segments contain both UIDs
    let index_before = SegmentIndex::load(&shard_dir).await.unwrap();
    for entry in index_before.iter_all() {
        assert_eq!(entry.uids.len(), 2);
        assert!(entry.uids.contains(&uid_a));
        assert!(entry.uids.contains(&uid_b));
    }

    // Create a batch that only compacts uid_a
    let batch = SegmentBatch {
        input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
        uid_plans: vec![UidPlan {
            uid: uid_a.clone(),
            output_segment_id: 10000,
        }],
    };

    let handover = Arc::new(CompactionHandover::new(
        0,
        shard_dir.clone(),
        Arc::clone(&segment_ids),
        Arc::clone(&flush_lock),
    ));

    // Compact only uid_a
    let compactor = MultiUidCompactor::new(
        batch.clone(),
        shard_dir.clone(),
        shard_dir.clone(),
        registry.clone(),
    );
    let results = compactor.run().await.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results.contains_key(&uid_a));

    // Commit the batch
    let new_entries = vec![SegmentEntry {
        id: 10000,
        uids: vec![uid_a.clone()],
    }];
    let drained = handover.commit_batch(&batch, new_entries).await.unwrap();

    // Verify segments still exist (not drained) because uid_b is still present
    let index_after = SegmentIndex::load(&shard_dir).await.unwrap();
    let mut found_original = false;
    let mut found_compacted = false;
    for entry in index_after.iter_all() {
        if entry.id < 10000 {
            assert!(entry.uids.contains(&uid_b), "Original segments should still have uid_b");
            found_original = true;
        } else {
            assert_eq!(entry.uids, vec![uid_a.clone()]);
            found_compacted = true;
        }
    }
    assert!(found_original, "Original segments should still exist");
    assert!(found_compacted, "Compacted segment should exist");
    assert!(drained.is_empty(), "No segments should be drained when UIDs remain");
}

#[tokio::test]
async fn handles_empty_segment_labels() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let batch = SegmentBatch {
        input_segment_labels: vec![],
        uid_plans: vec![UidPlan {
            uid: "uid1".to_string(),
            output_segment_id: 10000,
        }],
    };

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-empty-labels");

    let compactor = MultiUidCompactor::new(
        batch,
        shard_dir.clone(),
        shard_dir.clone(),
        registry,
    );

    // Should handle empty segment labels gracefully
    let result = compactor.run().await;
    match result {
        Ok(results) => {
            // Should return empty results or handle gracefully
            assert!(results.is_empty());
        }
        Err(_) => {
            // Error is acceptable for invalid input
        }
    }
}

#[tokio::test]
async fn handles_duplicate_uids_in_batch() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let plans = vec![
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid1".to_string(),
            input_segment_labels: vec!["00001".to_string()],
            output_segment_id: 10000,
        },
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid1".to_string(), // Duplicate UID
            input_segment_labels: vec!["00001".to_string()],
            output_segment_id: 10001,
        },
    ];

    let batches = SegmentBatch::group_plans(plans);
    // Should group them together since they have the same input segments
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].uid_count(), 2);
    // Both plans should be present even with duplicate UIDs
    assert_eq!(batches[0].uid_plans.len(), 2);
}

#[tokio::test]
async fn handles_overlapping_segment_sets() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let plans = vec![
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid1".to_string(),
            input_segment_labels: vec!["00001".to_string(), "00002".to_string()],
            output_segment_id: 10000,
        },
        MergePlan {
            level_from: 0,
            level_to: 1,
            uid: "uid2".to_string(),
            input_segment_labels: vec!["00002".to_string(), "00003".to_string()],
            output_segment_id: 10001,
        },
    ];

    let batches = SegmentBatch::group_plans(plans);
    // Should create separate batches since segment sets differ
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].uid_count(), 1);
    assert_eq!(batches[1].uid_count(), 1);
}
