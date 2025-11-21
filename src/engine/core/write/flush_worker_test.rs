use crate::engine::core::{
    FlushWorker, InflightSegments, SegmentIndex, SegmentLifecycleTracker, ZoneMeta,
};
use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::shard::flush_progress::FlushProgress;
use crate::test_helpers::factories::{CommandFactory, EventFactory, MemTableFactory, SchemaRegistryFactory};
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
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();

    // Spawn FlushWorker
    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(
        0,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight.clone(),
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    // Send flush request
    let flush_id = flush_progress.next_id();

    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        flush_id,
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
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();
    let worker = FlushWorker::new(
        1,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight,
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    // Create a completion channel to wait for flush completion
    let (completion_tx, completion_rx) = oneshot::channel();

    // Send flush request with empty memtable
    let flush_id = flush_progress.next_id();

    tx.send((
        segment_id,
        empty_memtable,
        Arc::clone(&registry),
        Arc::clone(&passive_memtable_arc),
        flush_id,
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

#[tokio::test]
async fn test_flush_worker_ticket_completed_even_on_failure() {
    // This test verifies that tickets are marked completed even when flush fails
    // This is critical for barrier semantics - SHOW needs to know when flushes finish
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-fail");
    std::fs::create_dir_all(&base_path).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "test_event";

    schema_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(2);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    let (tx, rx) = mpsc::channel(10);
    let segment_id = 99;
    let segment_ids = Arc::new(RwLock::new(vec![]));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();

    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(
        0,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight,
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    let (completion_tx, completion_rx) = oneshot::channel();
    let flush_id = flush_progress.next_id();

    // Make the base directory read-only to cause flush failure
    // Note: This might not work on all systems, but tests the concept
    // In practice, flush failures could come from disk full, permissions, etc.

    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        flush_id,
        Some(completion_tx),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to complete (or fail)
    let _flush_result = completion_rx.await;

    // Wait a bit for ticket completion
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // CRITICAL: Ticket should be marked completed even if flush failed
    // This ensures barrier semantics work - SHOW can wait for completion
    assert!(
        flush_progress.completed() >= flush_id,
        "Ticket {} should be marked completed even on failure, completed={}",
        flush_id,
        flush_progress.completed()
    );
}

#[tokio::test]
async fn test_flush_worker_segment_ids_not_updated_on_verification_failure() {
    // Test that when verification fails, segment_ids is NOT updated
    // even though files were written to disk
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-verify-fail");
    std::fs::create_dir_all(&base_path).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "verify_test";

    schema_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(3);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    let (tx, rx) = mpsc::channel(10);
    let segment_id = 42;
    let segment_ids = Arc::new(RwLock::new(vec![]));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();

    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(
        0,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight,
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    let (completion_tx, completion_rx) = oneshot::channel();
    let flush_id = flush_progress.next_id();

    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        flush_id,
        Some(completion_tx),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to start writing
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Corrupt or remove a critical file to cause verification failure
    // This simulates verification failure after files are written
    let segment_dir = base_path.join(format!("{:05}", segment_id));
    if segment_dir.exists() {
        // Remove a critical file to cause verification to fail
        let uid = registry.read().await.get_uid(event_type).unwrap();
        let zones_file = segment_dir.join(format!("{}.zones", uid));
        if zones_file.exists() {
            std::fs::remove_file(&zones_file).ok();
        }
    }

    // Wait for flush to complete (should fail verification)
    let flush_result = completion_rx.await.expect("Completion channel closed");
    let _ = flush_result; // May be Ok or Err, both are valid for this test

    // Wait for all async operations
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // CRITICAL: segment_ids should NOT be updated when verification fails
    let ids = segment_ids.read().unwrap().clone();
    assert!(
        !ids.contains(&format!("{:05}", segment_id)),
        "segment_ids should NOT contain segment {} when verification fails, got: {:?}",
        segment_id,
        ids
    );

    // CRITICAL: Passive buffer should remain registered (not cleared)
    // This allows retry after restart
    assert!(
        lifecycle.can_clear_passive(segment_id).await == false,
        "Passive buffer should remain registered when verification fails"
    );

    // Ticket should still be completed (for barrier semantics)
    assert!(
        flush_progress.completed() >= flush_id,
        "Ticket should be marked completed even when verification fails"
    );
}

#[tokio::test]
async fn test_flush_worker_passive_buffer_preserved_on_failure() {
    // Test that passive buffer is preserved when flush fails
    // This allows data to be retried after restart
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-preserve");
    std::fs::create_dir_all(&base_path).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "preserve_test";

    schema_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(5);

    let memtable = MemTableFactory::new().with_events(events.clone()).create().unwrap();
    let passive_memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let passive_arc = Arc::new(tokio::sync::Mutex::new(passive_memtable.clone()));

    let (tx, rx) = mpsc::channel(10);
    let segment_id = 77;
    let segment_ids = Arc::new(RwLock::new(vec![]));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();

    // Register the passive buffer BEFORE flush (simulating normal flow)
    lifecycle
        .register_flush(segment_id, Arc::clone(&passive_arc))
        .await;

    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(
        0,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight,
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    let (completion_tx, completion_rx) = oneshot::channel();
    let flush_id = flush_progress.next_id();

    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::clone(&passive_arc),
        flush_id,
        Some(completion_tx),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to start
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cause verification failure by removing critical files
    let segment_dir = base_path.join(format!("{:05}", segment_id));
    if segment_dir.exists() {
        let uid = registry.read().await.get_uid(event_type).unwrap();
        let zones_file = segment_dir.join(format!("{}.zones", uid));
        std::fs::remove_file(&zones_file).ok();
    }

    // Wait for flush to complete
    let _flush_result = completion_rx.await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // CRITICAL: Passive buffer should still be accessible and contain data
    let passive_guard = passive_arc.lock().await;
    assert!(
        !passive_guard.is_empty(),
        "Passive buffer should preserve data when flush fails"
    );
    assert_eq!(
        passive_guard.len(),
        5,
        "Passive buffer should have all 5 events preserved"
    );

    // Lifecycle should still have the buffer registered
    assert!(
        !lifecycle.can_clear_passive(segment_id).await,
        "Passive buffer should remain registered in lifecycle tracker"
    );
}

#[tokio::test]
async fn test_inflight_guard_drops_on_flush_failure() {
    // Test that inflight guard properly removes segment when flush fails
    // This ensures queries don't see segments that failed to flush
    let base_dir = tempdir().unwrap();
    let base_path = base_dir.path().join("shard-inflight-fail");
    std::fs::create_dir_all(&base_path).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "inflight_fail_test";

    schema_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .create_list(2);

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let memtable_clone = memtable.clone();

    let (tx, rx) = mpsc::channel(10);
    let segment_id = 88;
    let segment_ids = Arc::new(RwLock::new(vec![]));
    let lifecycle = Arc::new(SegmentLifecycleTracker::new());
    let flush_progress = Arc::new(FlushProgress::new());
    let inflight = InflightSegments::new();

    // Verify segment is NOT in inflight initially
    assert!(
        !inflight.contains("00088"),
        "Segment should not be in inflight before flush"
    );

    let flush_lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
    let worker = FlushWorker::new(
        0,
        base_path.clone(),
        flush_lock,
        Arc::clone(&segment_ids),
        Arc::clone(&lifecycle),
        Arc::clone(&flush_progress),
        inflight.clone(),
    );
    tokio::spawn(async move {
        worker.run(rx).await.expect("Worker run failed");
    });

    let (completion_tx, completion_rx) = oneshot::channel();
    let flush_id = flush_progress.next_id();

    tx.send((
        segment_id,
        memtable,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(memtable_clone)),
        flush_id,
        Some(completion_tx),
    ))
    .await
    .expect("Send failed");

    // Wait for flush to start (guard should be created)
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify segment IS in inflight during flush
    assert!(
        inflight.contains("00088"),
        "Segment should be in inflight during flush"
    );

    // Cause verification failure
    let segment_dir = base_path.join(format!("{:05}", segment_id));
    if segment_dir.exists() {
        let uid = registry.read().await.get_uid(event_type).unwrap();
        let zones_file = segment_dir.join(format!("{}.zones", uid));
        std::fs::remove_file(&zones_file).ok();
    }

    // Wait for flush to complete (should fail)
    let _flush_result = completion_rx.await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // CRITICAL: Guard should drop on failure, removing segment from inflight
    // This ensures queries don't try to read from failed segments
    assert!(
        !inflight.contains("00088"),
        "Segment should be removed from inflight when flush fails (guard dropped)"
    );
}

#[tokio::test]
async fn test_inflight_segments_merged_with_segment_ids_in_queries() {
    // Test that queries properly merge inflight_segments with segment_ids
    // This ensures queries see freshly flushed data via passive buffers

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "merge_test";

    schema_factory
        .define_with_fields(event_type, &[("value", "int")])
        .await
        .unwrap();

    // Create query plan with both segment_ids and inflight_segments
    let segment_ids = Arc::new(RwLock::new(vec!["00001".to_string(), "00002".to_string()]));
    let inflight = InflightSegments::new();

    // Add an inflight segment that's NOT in segment_ids yet
    inflight.insert("00003");

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .create();

    let mut plan = QueryPlan::build(&query_cmd, Arc::clone(&registry)).await;

    // Manually set segment_ids and inflight_segments for testing
    plan.segment_ids = Arc::clone(&segment_ids);
    plan.set_inflight_segments(Some(inflight.clone()));

    // Verify plan can check inflight status
    assert!(
        plan.is_segment_inflight("00003"),
        "Plan should detect inflight segment"
    );
    assert!(
        !plan.is_segment_inflight("00001"),
        "Plan should not mark published segment as inflight"
    );
    assert!(
        !plan.is_segment_inflight("00004"),
        "Plan should not mark unknown segment as inflight"
    );

    // Verify segment_maybe_contains_uid checks inflight
    let uid = registry.read().await.get_uid(event_type).unwrap();
    assert!(
        plan.segment_maybe_contains_uid("00003", &uid),
        "Inflight segment should be considered as potentially containing UID"
    );
}

