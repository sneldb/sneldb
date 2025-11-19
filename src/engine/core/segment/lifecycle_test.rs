use super::lifecycle::SegmentLifecycleTracker;
use crate::test_helpers::factories::MemTableFactory;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_register_flush_creates_new_state() {
    let tracker = SegmentLifecycleTracker::new();
    let memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
    let passive = Arc::new(Mutex::new(memtable));

    tracker.register_flush(1, Arc::clone(&passive)).await;

    assert!(
        tracker.can_clear_passive(1).await == false,
        "Newly registered flush should not be clearable"
    );
}

#[tokio::test]
async fn test_lifecycle_phase_progression() {
    let tracker = SegmentLifecycleTracker::new();
    let memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
    let passive = Arc::new(Mutex::new(memtable));
    let segment_id = 42;

    // Register
    tracker
        .register_flush(segment_id, Arc::clone(&passive))
        .await;
    assert!(
        !tracker.can_clear_passive(segment_id).await,
        "Should not be clearable in Flushing phase"
    );

    // Mark as written
    tracker.mark_written(segment_id).await;
    assert!(
        !tracker.can_clear_passive(segment_id).await,
        "Should not be clearable in Written phase"
    );

    // Mark as verified
    tracker.mark_verified(segment_id).await;
    assert!(
        tracker.can_clear_passive(segment_id).await,
        "Should be clearable in Verified phase"
    );
}

#[tokio::test]
async fn test_clear_and_complete_removes_state() {
    let tracker = SegmentLifecycleTracker::new();
    let memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
    let passive = Arc::new(Mutex::new(memtable));
    let segment_id = 100;

    tracker
        .register_flush(segment_id, Arc::clone(&passive))
        .await;
    tracker.mark_verified(segment_id).await;

    let cleared = tracker.clear_and_complete(segment_id).await;
    assert!(cleared.is_some(), "Should return passive buffer");

    // After clearing, segment should not exist
    assert!(
        !tracker.can_clear_passive(segment_id).await,
        "Segment should be removed after clear_and_complete"
    );
}

#[tokio::test]
async fn test_can_clear_passive_returns_false_for_nonexistent_segment() {
    let tracker = SegmentLifecycleTracker::new();
    assert!(
        !tracker.can_clear_passive(999).await,
        "Non-existent segment should not be clearable"
    );
}

#[tokio::test]
async fn test_multiple_segments_tracked_independently() {
    let tracker = SegmentLifecycleTracker::new();

    let seg1_memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
    let seg1_passive = Arc::new(Mutex::new(seg1_memtable));

    let seg2_memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
    let seg2_passive = Arc::new(Mutex::new(seg2_memtable));

    // Register two segments
    tracker.register_flush(1, seg1_passive).await;
    tracker.register_flush(2, seg2_passive).await;

    // Verify segment 1 only
    tracker.mark_written(1).await;
    tracker.mark_verified(1).await;

    assert!(
        tracker.can_clear_passive(1).await,
        "Segment 1 should be clearable"
    );
    assert!(
        !tracker.can_clear_passive(2).await,
        "Segment 2 should not be clearable"
    );
}

#[tokio::test]
async fn test_mark_operations_on_nonexistent_segment_dont_panic() {
    let tracker = SegmentLifecycleTracker::new();

    // These should not panic even though segment doesn't exist
    tracker.mark_written(999).await;
    tracker.mark_verified(999).await;

    assert!(
        !tracker.can_clear_passive(999).await,
        "Non-existent segment should not be clearable"
    );
}

#[tokio::test]
async fn test_clear_and_complete_returns_none_for_nonexistent() {
    let tracker = SegmentLifecycleTracker::new();
    let result = tracker.clear_and_complete(999).await;
    assert!(result.is_none(), "Should return None for non-existent segment");
}

#[tokio::test]
async fn test_concurrent_operations_on_different_segments() {
    let tracker = Arc::new(SegmentLifecycleTracker::new());

    let mut handles = vec![];

    for seg_id in 0..10 {
        let tracker_clone = Arc::clone(&tracker);
        let handle = tokio::spawn(async move {
            let memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
            let passive = Arc::new(Mutex::new(memtable));

            tracker_clone.register_flush(seg_id, passive).await;
            tracker_clone.mark_written(seg_id).await;
            tracker_clone.mark_verified(seg_id).await;

            tracker_clone.can_clear_passive(seg_id).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result, "All segments should be clearable after verification");
    }
}

#[tokio::test]
async fn test_passive_buffer_reference_preserved_until_clear() {
    let tracker = SegmentLifecycleTracker::new();

    let events = crate::test_helpers::factories::EventFactory::new().create_list(3);
    let memtable = MemTableFactory::new()
        .with_events(events)
        .create()
        .unwrap();
    let passive = Arc::new(Mutex::new(memtable));
    let segment_id = 50;

    // Register and track through lifecycle
    tracker
        .register_flush(segment_id, Arc::clone(&passive))
        .await;

    // Verify passive buffer still has events before clearing
    {
        let guard = passive.lock().await;
        assert_eq!(guard.len(), 3, "Passive buffer should have 3 events");
    }

    tracker.mark_verified(segment_id).await;

    // Clear and get the buffer back
    let cleared_buffer = tracker.clear_and_complete(segment_id).await;
    assert!(cleared_buffer.is_some(), "Should return the passive buffer");

    // Verify we can still access the events through returned Arc
    let returned = cleared_buffer.unwrap();
    let guard = returned.lock().await;
    assert_eq!(
        guard.len(),
        3,
        "Returned buffer should still have 3 events"
    );
}

#[tokio::test]
async fn test_rapid_register_and_clear_sequence() {
    let tracker = Arc::new(SegmentLifecycleTracker::new());

    for i in 0..100 {
        let memtable = MemTableFactory::new().with_capacity(10).create().unwrap();
        let passive = Arc::new(Mutex::new(memtable));

        tracker.register_flush(i, passive).await;
        tracker.mark_written(i).await;
        tracker.mark_verified(i).await;
        tracker.clear_and_complete(i).await;
    }

    // All segments should be cleared
    for i in 0..100 {
        assert!(
            !tracker.can_clear_passive(i).await,
            "Segment {} should be cleared",
            i
        );
    }
}

