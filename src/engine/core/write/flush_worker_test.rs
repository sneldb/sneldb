use crate::engine::core::{FlushWorker, SegmentIndex, SegmentLifecycleTracker, ZoneMeta};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use std::sync::{Arc, RwLock};
use tempfile::tempdir;
use tokio::sync::{mpsc, oneshot};

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

    let segment_ids = Arc::new(RwLock::new(vec![]));

    // Spawn FlushWorker
    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(0, base_path.clone(), flush_lock, Arc::clone(&segment_ids));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let worker = FlushWorker::new(0, base_path.clone(), flush_lock, Arc::clone(&lifecycle));
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    // Send flush request
    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        None,
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
    assert_eq!(index.len(), 1, "Expected 1 segment entry");
    let entry = index.iter_all().next().expect("Missing entry");
    assert_eq!(
        format!("{:05}", entry.id),
        "00003",
        "Segment label mismatch"
    );

    // Shared segment_ids should reflect the flushed segment
    let ids = segment_ids.read().unwrap().clone();
    assert_eq!(ids, vec!["00003"]);
    assert!(
        !lifecycle.can_clear_passive(segment_id).await,
        "Lifecycle entry should be cleared after successful flush"
    );
}

#[tokio::test]
async fn test_flush_worker_skips_cleanup_for_empty_memtable() {
    // Setup: base dir for segments
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-1");
    std::fs::create_dir_all(&base_path).unwrap();

    // Schema registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_created";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("email", "string")])
        .await
        .unwrap();

    // Create an empty memtable to flush
    let empty_memtable = MemTableFactory::new().with_capacity(10).create().unwrap();

    assert!(
        empty_memtable.is_empty(),
        "MemTable should be empty for this test"
    );

    // Create a passive memtable with some events (simulating data that should NOT be cleared)
    // Note: EventFactory::create_list() generates context_ids as "ctx1", "ctx2", etc.
    let passive_events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(2);

    // Verify what context_ids were actually created
    let expected_contexts: Vec<String> = passive_events
        .iter()
        .map(|e| e.context_id.clone())
        .collect();

    let passive_memtable = MemTableFactory::new()
        .with_events(passive_events.clone())
        .create()
        .unwrap();

    assert!(
        !passive_memtable.is_empty(),
        "Passive memtable should have events"
    );
    assert_eq!(
        passive_memtable.len(),
        2,
        "Passive memtable should have 2 events"
    );

    // Channel for flush worker
    let (tx, rx) = mpsc::channel(10);
    let segment_id = 5;
    let segment_ids = Arc::new(RwLock::new(vec![]));

    // Create the passive memtable Arc that will be passed to the flush worker
    // This is the same Arc that the flush worker will operate on
    let passive_memtable_arc = Arc::new(tokio::sync::Mutex::new(passive_memtable.clone()));

    // Spawn FlushWorker
    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let worker = FlushWorker::new(1, base_path.clone(), flush_lock, Arc::clone(&segment_ids), Arc::clone(&lifecycle));
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    // Create a completion channel to wait for flush completion
    let (completion_tx, completion_rx) = oneshot::channel();

    // Send flush request with empty memtable
    tx.send((
        segment_id,
        empty_memtable,
        Arc::clone(&registry),
        Arc::clone(&passive_memtable_arc),
        Some(completion_tx),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to complete
    let flush_result = completion_rx.await.expect("Completion channel closed");
    assert!(
        flush_result.is_ok(),
        "Flush should succeed even with empty memtable: {:?}",
        flush_result
    );

    // Wait a bit to ensure all async operations complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify segment directory was NOT created (empty memtable should not create files)
    let segment_dir = base_path.join(format!("{:05}", segment_id));
    assert!(
        !segment_dir.exists(),
        "Segment directory should NOT be created when memtable is empty: {:?}",
        segment_dir
    );

    // Verify segment index is empty or does not contain this segment
    let index = SegmentIndex::load(&base_path).await;
    match index {
        Ok(idx) => {
            // Index might exist but should not have entries for empty flush
            let has_segment = idx.iter_all().any(|e| e.id == segment_id as u32);
            assert!(
                !has_segment,
                "Segment index should not contain empty flush segment {}",
                segment_id
            );
        }
        Err(_) => {
            // Index doesn't exist, which is fine for empty flush
        }
    }

    // CRITICAL: Verify passive memtable still has its events (was NOT cleared)
    // This is the key test - empty memtable flush should NOT clear the passive memtable
    // Check the same Arc that was passed to the flush worker
    let pmem = passive_memtable_arc.lock().await;
    assert!(
        !pmem.is_empty(),
        "Passive memtable should NOT be empty after empty flush"
    );
    assert_eq!(
        pmem.len(),
        2,
        "Passive memtable should still have 2 events after empty flush"
    );

    // Verify the events are still present with the correct context_ids
    let event_contexts: Vec<String> = pmem.iter().map(|e| e.context_id.clone()).collect();
    assert_eq!(
        event_contexts, expected_contexts,
        "Passive memtable should still contain its original events"
    );

    // No new segment IDs should be recorded for empty flushes
    assert!(segment_ids.read().unwrap().is_empty());
    assert!(
        !lifecycle.can_clear_passive(segment_id).await,
        "Lifecycle tracker should not register empty flushes"
    );
}
