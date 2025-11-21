use crate::engine::core::MemTable;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};

/// Tracks lifecycle states of segments during flush operations
#[derive(Debug)]
pub struct SegmentLifecycleTracker {
    states: RwLock<HashMap<u64, SegmentLifecycleState>>,
}

#[derive(Debug)]
struct SegmentLifecycleState {
    phase: LifecyclePhase,
    passive_buffer: Arc<Mutex<MemTable>>,
    _flushed_at: Instant,
    verified_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecyclePhase {
    Flushing,
    Written,
    Verified,
}

impl SegmentLifecycleTracker {
    pub fn new() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
        }
    }

    /// Register a segment flush with its passive buffer
    pub async fn register_flush(&self, segment_id: u64, passive_buffer: Arc<Mutex<MemTable>>) {
        let mut states = self.states.write().await;
        states.insert(
            segment_id,
            SegmentLifecycleState {
                phase: LifecyclePhase::Flushing,
                passive_buffer,
                _flushed_at: Instant::now(),
                verified_at: None,
            },
        );
    }

    /// Mark segment as written to disk
    pub async fn mark_written(&self, segment_id: u64) {
        if let Some(state) = self.states.write().await.get_mut(&segment_id) {
            state.phase = LifecyclePhase::Written;
        }
    }

    /// Mark segment as verified queryable
    pub async fn mark_verified(&self, segment_id: u64) {
        let mut states = self.states.write().await;
        if let Some(state) = states.get_mut(&segment_id) {
            state.phase = LifecyclePhase::Verified;
            state.verified_at = Some(Instant::now());
        }
    }

    /// Check if passive buffer can be cleared
    pub async fn can_clear_passive(&self, segment_id: u64) -> bool {
        self.states
            .read()
            .await
            .get(&segment_id)
            .map(|s| s.phase == LifecyclePhase::Verified)
            .unwrap_or(false)
    }

    /// Clear passive buffer and remove tracking state
    pub async fn clear_and_complete(&self, segment_id: u64) -> Option<Arc<Mutex<MemTable>>> {
        self.states
            .write()
            .await
            .remove(&segment_id)
            .map(|state| state.passive_buffer)
    }
}
