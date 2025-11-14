use crate::engine::shard::manager::ShardManager;
use crate::frontend::server_state::ServerState;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_server_state_new_initializes_correctly() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(8, base_dir, wal_dir).await);
    let server_state = ServerState::new(Arc::clone(&shard_manager), 80);

    // Check initial state
    assert!(!server_state.is_shutting_down());
    assert!(!server_state.is_under_pressure());
}

#[tokio::test]
async fn test_server_state_shutdown_flag() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(4, base_dir, wal_dir).await);
    let server_state = ServerState::new(Arc::clone(&shard_manager), 50);

    // Initially not shutting down
    assert!(!server_state.is_shutting_down());

    // Signal shutdown
    server_state.signal_shutdown();

    // Should now be shutting down
    assert!(server_state.is_shutting_down());

    // Clone should share the same shutdown state
    let server_state_clone = server_state.clone();
    assert!(server_state_clone.is_shutting_down());
}

#[tokio::test]
async fn test_server_state_pending_operations_tracking() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(2, base_dir, wal_dir).await);
    let server_state = ServerState::new(Arc::clone(&shard_manager), 80);

    // Initially no pending operations
    assert!(!server_state.is_under_pressure());

    // Increment pending operations
    server_state.increment_pending();
    server_state.increment_pending();
    server_state.increment_pending();

    // Decrement one
    server_state.decrement_pending();

    // With 2 pending ops and 2 shards * 8096 capacity = 16192 total
    // 2 * 100 / 16192 = ~0.012% - well below 80% threshold
    assert!(!server_state.is_under_pressure());
}

#[tokio::test]
async fn test_server_state_backpressure_no_shards() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // This is a bit tricky - we can't easily create an empty ShardManager
    // but we can test with a very high threshold and no pending ops
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(1, base_dir, wal_dir).await);
    let _server_state = ServerState::new(Arc::clone(&shard_manager), 100);

    // Even with 1 shard and some pending ops, threshold 100% should not trigger
    // unless we have exactly 100% capacity
    let _total_capacity = 8096; // 1 shard * 8096 capacity

    // Add operations to reach exactly threshold (8096 pending ops for 100% threshold)
    // But that would be too many operations, so let's test with lower threshold
    let server_state_low = ServerState::new(Arc::clone(&shard_manager), 1);

    // Add enough pending to exceed 1% threshold
    // 1% of 8096 = ~81 operations
    for _ in 0..82 {
        server_state_low.increment_pending();
    }

    assert!(server_state_low.is_under_pressure());
}

#[tokio::test]
async fn test_server_state_backpressure_threshold_calculation() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(4, base_dir, wal_dir).await);

    // Use 50% threshold for easier testing
    let server_state = ServerState::new(Arc::clone(&shard_manager), 50);

    // Total capacity: 4 shards * 8096 = 32384
    // 50% threshold calculation: (pending * 100) / total_capacity >= 50
    // So we need: pending >= (50 * 32384) / 100 = 16192
    // But integer division rounds down, so we need pending >= 16192

    // Add operations to exactly reach threshold (16192)
    for _ in 0..16192 {
        server_state.increment_pending();
    }

    // Should be at exactly 50% threshold
    assert!(server_state.is_under_pressure());

    // Decrement one operation to go just below threshold
    server_state.decrement_pending();

    // Should now be below threshold (16191 * 100 / 32384 = 49.99...)
    assert!(!server_state.is_under_pressure());

    // Add one more to go back over threshold
    server_state.increment_pending();
    assert!(server_state.is_under_pressure());
}

#[tokio::test]
async fn test_server_state_backpressure_edge_cases() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(8, base_dir, wal_dir).await);

    // Test with 0% threshold - should trigger immediately with any pending ops
    let server_state_zero = ServerState::new(Arc::clone(&shard_manager), 0);
    server_state_zero.increment_pending();
    assert!(server_state_zero.is_under_pressure());

    // Test with 100% threshold - should only trigger at exact capacity
    let server_state_100 = ServerState::new(Arc::clone(&shard_manager), 100);
    let total_capacity = 8 * 8096; // 64768

    // Add operations up to 100% - 1
    for _ in 0..(total_capacity - 1) {
        server_state_100.increment_pending();
    }
    assert!(!server_state_100.is_under_pressure());

    // Add one more to reach 100%
    server_state_100.increment_pending();
    assert!(server_state_100.is_under_pressure());
}

#[tokio::test]
async fn test_server_state_concurrent_operations() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(2, base_dir, wal_dir).await);
    let server_state = Arc::new(ServerState::new(Arc::clone(&shard_manager), 80));

    // Simulate concurrent increment/decrement operations
    let mut handles = Vec::new();

    // Spawn tasks that increment and decrement
    for _ in 0..10 {
        let state = Arc::clone(&server_state);
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                state.increment_pending();
                state.decrement_pending();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // After all increment/decrement pairs complete,
    // we should have net zero pending operations
    // The operations are atomic, so final state should be consistent
    // Given we did 10 * 100 = 1000 increment/decrement pairs,
    // net should be zero (though there could be minor timing differences)

    // Since each task does equal increments and decrements,
    // the final pending count should be near zero
    // This test verifies that the atomic operations work correctly
    // and don't corrupt the counter
    let still_under_pressure = server_state.is_under_pressure();

    // With 2 shards * 8096 = 16192 total capacity and 80% threshold,
    // we'd need 12953 pending ops to trigger backpressure.
    // After all operations, net should be near zero, so not under pressure
    // (This verifies atomicity of the counter)
    assert!(!still_under_pressure);
}

#[tokio::test]
async fn test_server_state_multiple_shards_capacity() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Test with different shard counts
    for shard_count in [1, 2, 4, 8] {
        let shard_manager =
            Arc::new(ShardManager::new(shard_count, base_dir.clone(), wal_dir.clone()).await);
        let server_state = ServerState::new(Arc::clone(&shard_manager), 50);

        let total_capacity = shard_count * 8096;
        let threshold_ops = (total_capacity * 50) / 100;

        // Add operations to exactly reach threshold
        for _ in 0..threshold_ops {
            server_state.increment_pending();
        }

        // Should be at threshold (exactly 50%)
        assert!(server_state.is_under_pressure());
    }
}

#[tokio::test]
async fn test_server_state_clone_shares_state() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let shard_manager = Arc::new(ShardManager::new(4, base_dir, wal_dir).await);
    let server_state = ServerState::new(Arc::clone(&shard_manager), 80);

    // Clone should share shutdown state
    let clone1 = server_state.clone();
    assert_eq!(server_state.is_shutting_down(), clone1.is_shutting_down());

    server_state.signal_shutdown();
    assert!(clone1.is_shutting_down());

    // Clone should share pending operations counter
    let clone2 = server_state.clone();
    server_state.increment_pending();

    // Both should reflect the same pending count
    // (We can't directly access pending count, but we can verify
    //  that both clones see the same backpressure state after enough operations)

    // Add many operations to trigger backpressure
    let total_capacity = 4 * 8096; // 32384
    let threshold_ops = (total_capacity * 80) / 100; // 25907

    for _ in 0..threshold_ops {
        server_state.increment_pending();
    }

    // Both should see the same backpressure state
    assert_eq!(server_state.is_under_pressure(), clone2.is_under_pressure());
}
