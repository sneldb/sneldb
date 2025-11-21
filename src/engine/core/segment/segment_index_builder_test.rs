use crate::engine::core::SegmentIndex;
use crate::engine::core::SegmentIndexBuilder;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_segment_index_builder_adds_segment_entry() {
    // Setup: temp dir to simulate segment directory
    let tmp_dir = tempdir().expect("Failed to create temp dir");
    let shard_dir = tmp_dir.path();
    let segment_dir = shard_dir.join("00042");
    std::fs::create_dir(&segment_dir).expect("Failed to create segment dir");

    // Given: UID list
    let uids = vec!["uid_1".to_string(), "uid_2".to_string()];
    let segment_id = 42;

    // When: we build and add the segment entry
    let builder = SegmentIndexBuilder {
        segment_id,
        segment_dir: &segment_dir,
        event_type_uids: uids.clone(),
        flush_coordination_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };

    builder
        .add_segment_entry(None)
        .await
        .expect("Failed to add entry");

    // Then: verify segments.idx file is present in shard_dir
    let idx_path = shard_dir.join("segments.idx");
    assert!(
        idx_path.exists(),
        "Expected segments.idx to exist in shard directory"
    );

    // Load index and check contents
    let index = SegmentIndex::load(shard_dir)
        .await
        .expect("Failed to load index");
    assert_eq!(index.len(), 1, "Expected one entry in SegmentIndex");

    let entry = index.iter_all().next().expect("Missing entry");
    assert_eq!(entry.id, 42);
    assert_eq!(format!("{:05}", entry.id), "00042");
    assert_eq!(entry.uids, uids);
}

#[tokio::test]
async fn test_flush_coordination_lock_prevents_concurrent_index_updates() {
    // Test that flush_coordination_lock serializes concurrent segment index updates
    // This prevents index catalog corruption when multiple flushes happen simultaneously

    let tmp_dir = tempdir().expect("Failed to create temp dir");
    let shard_dir = tmp_dir.path();

    // Shared lock across all builders
    let coordination_lock = Arc::new(Mutex::new(()));

    // Create multiple segment directories
    let segment_dir1 = shard_dir.join("00001");
    let segment_dir2 = shard_dir.join("00002");
    let segment_dir3 = shard_dir.join("00003");
    std::fs::create_dir_all(&segment_dir1).expect("Failed to create segment dir");
    std::fs::create_dir_all(&segment_dir2).expect("Failed to create segment dir");
    std::fs::create_dir_all(&segment_dir3).expect("Failed to create segment dir");

    let uids1 = vec!["uid_a".to_string()];
    let uids2 = vec!["uid_b".to_string()];
    let uids3 = vec!["uid_c".to_string()];

    // Spawn concurrent builders that will compete for the lock
    let lock1 = Arc::clone(&coordination_lock);
    let lock2 = Arc::clone(&coordination_lock);
    let lock3 = Arc::clone(&coordination_lock);
    let shard1 = shard_dir.to_path_buf();
    let shard2 = shard_dir.to_path_buf();
    let shard3 = shard_dir.to_path_buf();

    let handle1 = tokio::spawn(async move {
        let builder = SegmentIndexBuilder {
            segment_id: 1,
            segment_dir: &segment_dir1,
            event_type_uids: uids1,
            flush_coordination_lock: lock1,
        };
        builder.add_segment_entry(Some(&shard1)).await
    });

    let handle2 = tokio::spawn(async move {
        // Small delay to ensure they're truly concurrent
        sleep(Duration::from_millis(10)).await;
        let builder = SegmentIndexBuilder {
            segment_id: 2,
            segment_dir: &segment_dir2,
            event_type_uids: uids2,
            flush_coordination_lock: lock2,
        };
        builder.add_segment_entry(Some(&shard2)).await
    });

    let handle3 = tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        let builder = SegmentIndexBuilder {
            segment_id: 3,
            segment_dir: &segment_dir3,
            event_type_uids: uids3,
            flush_coordination_lock: lock3,
        };
        builder.add_segment_entry(Some(&shard3)).await
    });

    // Wait for all concurrent updates
    let result1 = handle1.await.expect("Task 1 panicked");
    let result2 = handle2.await.expect("Task 2 panicked");
    let result3 = handle3.await.expect("Task 3 panicked");

    assert!(result1.is_ok(), "First builder should succeed");
    assert!(result2.is_ok(), "Second builder should succeed");
    assert!(result3.is_ok(), "Third builder should succeed");

    // Verify index contains all three segments (no corruption or lost entries)
    let final_index = SegmentIndex::load(shard_dir)
        .await
        .expect("Failed to load final index");

    assert_eq!(
        final_index.len(),
        3,
        "Index should contain all 3 segments, got {}",
        final_index.len()
    );

    // Verify all segments are present
    let mut found_ids: Vec<u32> = final_index.iter_all().map(|e| e.id).collect();
    found_ids.sort();
    assert_eq!(found_ids, vec![1, 2, 3], "All segment IDs should be present");
}

#[tokio::test]
async fn test_flush_coordination_lock_released_on_failure() {
    // Test that lock is released even if index update fails
    // This prevents deadlock if a flush fails
    let tmp_dir = tempdir().expect("Failed to create temp dir");

    // Use a non-existent shard directory to cause failure
    let bad_shard_dir = tmp_dir.path().join("nonexistent");
    let segment_dir = bad_shard_dir.join("00001");

    let coordination_lock = Arc::new(Mutex::new(()));

    let builder = SegmentIndexBuilder {
        segment_id: 1,
        segment_dir: &segment_dir,
        event_type_uids: vec!["uid_test".to_string()],
        flush_coordination_lock: Arc::clone(&coordination_lock),
    };

    // This should fail (directory doesn't exist)
    let result = builder.add_segment_entry(Some(&bad_shard_dir)).await;
    assert!(result.is_err(), "Should fail with non-existent directory");

    // CRITICAL: Lock should be released (guard dropped)
    // Verify we can acquire lock immediately (would deadlock if not released)
    let lock_result = tokio::time::timeout(
        Duration::from_millis(100),
        coordination_lock.lock(),
    )
    .await;

    assert!(
        lock_result.is_ok(),
        "Lock should be released after failure, allowing immediate re-acquisition"
    );
}
