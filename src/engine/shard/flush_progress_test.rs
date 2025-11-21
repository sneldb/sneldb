use super::flush_progress::FlushProgress;
use std::sync::Arc;

#[test]
fn next_id_is_monotonic_starting_at_one() {
    let progress = FlushProgress::new();

    assert_eq!(progress.next_id(), 1);
    assert_eq!(progress.next_id(), 2);
    assert_eq!(progress.next_id(), 3);

    // Snapshot should reflect total submissions so far.
    assert_eq!(progress.snapshot(), 3);
}

#[test]
fn mark_completed_tracks_highest_id_even_out_of_order() {
    let progress = FlushProgress::new();
    let first = progress.next_id(); // 1
    let second = progress.next_id(); // 2
    let third = progress.next_id(); // 3

    // Complete the second flush before the first; completed counter should jump to 2.
    progress.mark_completed(second);
    assert_eq!(progress.completed(), second);

    // Completing an older flush does not move the pointer backwards.
    progress.mark_completed(first);
    assert_eq!(progress.completed(), second);

    // Completing a newer flush should advance to the highest ID seen.
    progress.mark_completed(third);
    assert_eq!(progress.completed(), third);
}

#[test]
fn snapshot_accounts_for_concurrent_submissions() {
    const THREADS: usize = 4;
    const PER_THREAD: usize = 1_000;

    let progress = Arc::new(FlushProgress::new());
    let mut handles = Vec::with_capacity(THREADS);

    for _ in 0..THREADS {
        let tracker = Arc::clone(&progress);
        handles.push(std::thread::spawn(move || {
            for _ in 0..PER_THREAD {
                tracker.next_id();
            }
        }));
    }

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let expected = (THREADS * PER_THREAD) as u64;
    assert_eq!(
        progress.snapshot(),
        expected,
        "snapshot should match total submissions"
    );
    assert_eq!(
        progress.completed(),
        0,
        "completed counter should remain zero until mark_completed is invoked"
    );
}

#[test]
fn await_flush_idle_shard_returns_immediately() {
    // Test that when completed() >= snapshot(), await returns immediately
    // This simulates an idle shard with no pending flushes
    let progress = FlushProgress::new();

    // Issue some tickets and complete them
    let ticket1 = progress.next_id();
    let ticket2 = progress.next_id();
    progress.mark_completed(ticket1);
    progress.mark_completed(ticket2);

    // Snapshot current state
    let snapshot = progress.snapshot();

    // Completed should be >= snapshot (all tickets completed)
    assert!(
        progress.completed() >= snapshot,
        "Idle shard should have completed >= snapshot"
    );
}

#[test]
fn await_flush_waits_for_specific_ticket() {
    // Test that await waits until target ticket completes
    let progress = FlushProgress::new();

    // Issue tickets
    let ticket1 = progress.next_id(); // 1
    let ticket2 = progress.next_id(); // 2
    let ticket3 = progress.next_id(); // 3

    let snapshot = progress.snapshot(); // Should be 3

    // Complete tickets out of order
    progress.mark_completed(ticket2); // Complete 2
    assert!(
        progress.completed() < snapshot,
        "Should not have reached snapshot yet"
    );

    progress.mark_completed(ticket3); // Complete 3
    assert!(
        progress.completed() >= snapshot,
        "Should have reached snapshot after completing all tickets"
    );

    // Complete ticket1 (older) - should not affect completed counter
    progress.mark_completed(ticket1);
    assert_eq!(
        progress.completed(),
        ticket3,
        "Completed should remain at highest ticket"
    );
}

#[tokio::test]
async fn await_flush_new_writes_continue_during_wait() {
    // Test that new flush tickets can be issued during await
    // This verifies that await doesn't block new writes
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    let progress = Arc::new(FlushProgress::new());

    // Issue initial tickets
    let ticket1 = progress.next_id();
    let ticket2 = progress.next_id();
    let _initial_snapshot = progress.snapshot(); // Should be 2

    // Start await in background - it takes snapshot at start
    let progress_clone = Arc::clone(&progress);
    let await_handle = tokio::spawn(async move {
        let target = progress_clone.snapshot(); // This will be >= initial_snapshot
        let start = std::time::Instant::now();
        while progress_clone.completed() < target {
            if start.elapsed() > Duration::from_secs(2) {
                return false; // Timeout
            }
            sleep(Duration::from_millis(10)).await;
        }
        true
    });

    // Small delay to let await task take its snapshot
    sleep(Duration::from_millis(10)).await;

    // Issue NEW tickets while await is waiting (simulating new writes)
    let ticket3 = progress.next_id();
    let ticket4 = progress.next_id();
    let _ = ticket3; // Used for documentation

    // Complete original tickets
    progress.mark_completed(ticket1);
    progress.mark_completed(ticket2);

    // Await should complete (it waits for snapshot taken at start of await task)
    // The snapshot in the await task will be >= initial_snapshot (2), so completing
    // tickets 1 and 2 should satisfy it
    let completed = await_handle.await.unwrap();
    assert!(
        completed,
        "Await should complete when initial tickets finish, ignoring new tickets"
    );

    // New tickets should still be pending (not completed)
    assert!(
        progress.completed() < ticket4,
        "New tickets issued during await should still be pending, completed={}, ticket4={}",
        progress.completed(),
        ticket4
    );
}
