use crate::engine::core::{FlushManager, SegmentIndex, ZoneMeta};
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
    let manager = FlushManager::new(
        shard_id,
        base_dir.clone(),
        Arc::clone(&segment_ids),
        flush_lock,
    );

    // Act: queue for flush
    manager
        .queue_for_flush(
            memtable,
            Arc::clone(&registry),
            segment_id,
            Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        )
        .await
        .expect("FlushManager failed");

    // Wait for async flush to complete
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify .zones file written
    let segment_dir = base_dir.join(format!("{:05}", segment_id));
    let zones_path = segment_dir.join(format!("{}.zones", uid));
    assert!(zones_path.exists(), "Zones file should exist");

    let zones = ZoneMeta::load(&zones_path).expect("Failed to load .zones");
    assert!(!zones.is_empty(), "Zones should not be empty");

    // Verify segment index
    let segment_index = SegmentIndex::load(&base_dir).await.expect("load failed");
    assert_eq!(segment_index.entries.len(), 1);
    assert_eq!(format!("{:05}", segment_index.entries[0].id), "00001");

    // Verify internal segment_ids updated
    let ids = manager.get_segment_ids();
    assert_eq!(ids, vec!["00001"]);
}
