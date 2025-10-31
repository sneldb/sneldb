use crate::engine::shard::manager::ShardManager;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Manages server-wide state: shutdown flag and backpressure detection
#[derive(Clone)]
pub struct ServerState {
    shutdown: Arc<AtomicBool>,
    pending_operations: Arc<AtomicUsize>,
    shard_manager: Arc<ShardManager>,
    backpressure_threshold: u8,
    channel_capacity: usize,
}

impl ServerState {
    /// Creates a new ServerState instance
    pub fn new(shard_manager: Arc<ShardManager>, backpressure_threshold: u8) -> Self {
        // Channel capacity is 8096 based on shard creation code
        const DEFAULT_CHANNEL_CAPACITY: usize = 8096;
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            pending_operations: Arc::new(AtomicUsize::new(0)),
            shard_manager,
            backpressure_threshold,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    /// Returns true if the server is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Signals that the server should start shutting down
    pub fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Increments the pending operations counter (call before sending to shard)
    pub fn increment_pending(&self) {
        self.pending_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the pending operations counter (call after operation completes)
    pub fn decrement_pending(&self) {
        self.pending_operations.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the current count of pending operations
    pub fn pending_operations_count(&self) -> usize {
        self.pending_operations.load(Ordering::Relaxed)
    }

    /// Checks if the server is under backpressure
    /// This is a lightweight check based on pending operations count.
    /// Returns true if the estimated channel usage exceeds the threshold.
    pub fn is_under_pressure(&self) -> bool {
        let pending = self.pending_operations.load(Ordering::Relaxed);
        let shards = self.shard_manager.all_shards();

        if shards.is_empty() {
            return false;
        }

        // Calculate total channel capacity across all shards
        let total_capacity = self.channel_capacity * shards.len();

        // Estimate current usage based on pending operations
        // This is a heuristic: pending operations approximate messages in channels
        let usage_percent = if total_capacity > 0 {
            (pending * 100) / total_capacity
        } else {
            0
        };

        // Debug logging (can be removed in production)
        if pending > 0 {
            tracing::debug!(
                target: "sneldb::backpressure",
                pending = pending,
                total_capacity = total_capacity,
                usage_percent = usage_percent,
                threshold = self.backpressure_threshold,
                "Backpressure check"
            );
        }

        // Under pressure if usage exceeds threshold
        usage_percent >= self.backpressure_threshold as usize
    }
}
