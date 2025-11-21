use crate::engine::core::{
    FlushManager, InflightSegments, SegmentIndex, SegmentLifecycleTracker, ZoneMeta,
};
use crate::engine::shard::flush_progress::FlushProgress;
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use std::sync::{Arc, RwLock};
use tempfile::tempdir;

#[tokio::test]
async fn test_flush_manager_queues_and_flushes_memtable() {
    // Setup
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&base_dir).unwrap();

    let shard_id = 0;
    let segment_id = 1;

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "user_created";

    registry_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("email", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(3);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    let segment_ids = Arc::new(RwLock::new(vec![]));

    // Create FlushManager
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight_segments = InflightSegments::new();
    let manager = FlushManager::new(
        shard_id,
        base_dir.clone(),
        Arc::clone(&segment_ids),
        flush_lock,
        lifecycle,
        Arc::clone(&flush_progress),
        inflight_segments,
    );

    // Act: queue for flush
    let flush_id = flush_progress.next_id();

    manager
        .queue_for_flush(
            memtable,
            Arc::clone(&registry),
            segment_id,
            Arc::new(tokio::sync::Mutex::new(memtable_clone)),
            flush_id,
            None,
        )
        .await
        .expect("FlushManager failed");

    // Wait for async flush to complete
    let start = std::time::Instant::now();
    while flush_progress.completed() < flush_id
        && start.elapsed() < std::time::Duration::from_secs(2)
    {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        flush_progress.completed() >= flush_id,
        "flush should complete within timeout"
    );

    // Verify .zones file written
    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    let zones_path = segment_dir.join(format!("{}.zones", uid));
    assert!(zones_path.exists(), "Zones file should exist");

    let zones = ZoneMeta::load(&zones_path).expect("Failed to load .zones");
    assert!(!zones.is_empty(), "Zones should not be empty");

    // Verify segment index
    let segment_index = SegmentIndex::load(&base_dir).await.expect("load failed");
    assert_eq!(segment_index.len(), 1);
    let entry = segment_index.iter_all().next().expect("Missing entry");
    assert_eq!(format!("{:05}", entry.id), "00001");

    // Verify internal segment_ids updated
    let ids = manager.get_segment_ids();
    assert_eq!(ids, vec!["00001"]);
}

#[tokio::test]
async fn test_flush_manager_inserts_segment_into_inflight_on_queue() {
    // Test that FlushManager inserts segment into inflight_segments when queuing
    // This is critical - queries need to see inflight segments to access passive buffers
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path().join("shard-inflight-queue");
    std::fs::create_dir_all(&base_dir).unwrap();

    let shard_id = 0;
    let segment_id = 77;

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "inflight_queue_test";

    registry_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(2);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    let segment_ids = Arc::new(RwLock::new(vec![]));
    let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight_segments = InflightSegments::new();

    let manager = FlushManager::new(
        shard_id,
        base_dir.clone(),
        Arc::clone(&segment_ids),
        flush_lock,
        lifecycle,
        Arc::clone(&flush_progress),
        inflight_segments.clone(),
    );

    // Verify segment NOT in inflight before queue
    assert!(
        !inflight_segments.contains("00077"),
        "Segment should not be in inflight before queue_for_flush"
    );

    // Queue for flush
    let flush_id = flush_progress.next_id();
    manager
        .queue_for_flush(
            memtable,
            Arc::clone(&registry),
            segment_id,
            Arc::new(tokio::sync::Mutex::new(memtable_clone)),
            flush_id,
            None,
        )
        .await
        .expect("FlushManager failed");

    // CRITICAL: Segment should be in inflight immediately after queue
    // This happens in queue_for_flush before sending to worker
    assert!(
        inflight_segments.contains("00077"),
        "Segment should be inserted into inflight_segments when queued for flush"
    );
}
