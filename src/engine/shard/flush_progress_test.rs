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
