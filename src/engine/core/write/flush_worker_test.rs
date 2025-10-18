use crate::engine::core::{FlushWorker, SegmentIndex, ZoneMeta};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_flush_worker_processes_memtable() {
    // Setup: base dir for segments
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-0");
    std::fs::create_dir_all(&base_path).unwrap();

    // Schema registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_created";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("email", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Create a MemTable
    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(3);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    // Channel for flush worker
    let (tx, rx) = mpsc::channel(10);
    let segment_id = 3;

    // Spawn FlushWorker
    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(0, base_path.clone(), flush_lock);
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    // Send flush request
    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to complete
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify zones file exists and is valid
    let segment_dir = base_path.join(format!("{:05}", segment_id));
    let zones_path = segment_dir.join(format!("{}.zones", uid));
    assert!(
        zones_path.exists(),
        ".zones file should exist at {:?}",
        zones_path
    );

    let zone_meta = ZoneMeta::load(&zones_path).expect("Failed to load zone meta");
    assert!(!zone_meta.is_empty(), "Zone meta should not be empty");

    // Verify segment index is updated
    let index = SegmentIndex::load(&base_path).await.expect("Load failed");
    assert_eq!(index.entries.len(), 1, "Expected 1 segment entry");
    assert_eq!(format!("{:05}", index.entries[0].id), "00003", "Segment label mismatch");
}
