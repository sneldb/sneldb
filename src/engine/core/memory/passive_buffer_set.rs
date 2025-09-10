use super::memtable::MemTable;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Manages multiple passive MemTables that are awaiting flush.
/// Each rotation creates a new passive buffer; queries should read from all non-empty buffers.
#[derive(Debug)]
pub struct PassiveBufferSet {
    buffers: Mutex<Vec<Arc<Mutex<MemTable>>>>,
    max_inflight: usize,
}

impl PassiveBufferSet {
    pub fn new(max_inflight: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::new()),
            max_inflight,
        }
    }

    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    /// Add a new passive buffer cloned from the given active MemTable.
    /// Returns the Arc to the specific passive buffer, which should be passed to the flush worker.
    pub async fn add_from(&self, source: &MemTable) -> Arc<Mutex<MemTable>> {
        let passive = Arc::new(Mutex::new(source.clone()));

        let mut guard = self.buffers.lock().await;
        // Enforce a simple cap by dropping the oldest empty buffers first, then blocking growth.
        if guard.len() >= self.max_inflight {
            // Try to prune empties to make space
            guard.retain(|buf| {
                // Retain if non-empty; empty buffers will be dropped
                // Note: try_lock to avoid await inside retain; if contended, keep it.
                if let Ok(inner) = buf.try_lock() {
                    inner.len() > 0
                } else {
                    true
                }
            });
        }

        guard.push(Arc::clone(&passive));
        passive
    }

    /// Returns a snapshot vector of all non-empty passive buffers.
    pub async fn non_empty(&self) -> Vec<Arc<Mutex<MemTable>>> {
        let guard = self.buffers.lock().await;
        let mut out = Vec::with_capacity(guard.len());
        for buf in guard.iter() {
            if let Ok(inner) = buf.try_lock() {
                if inner.len() > 0 {
                    out.push(Arc::clone(buf));
                }
            } else {
                // If contended, include it to be safe; query will lock later
                out.push(Arc::clone(buf));
            }
        }
        out
    }

    /// Remove empty buffers from the set. Best-effort.
    pub async fn prune_empties(&self) {
        let mut guard = self.buffers.lock().await;
        guard.retain(|buf| {
            if let Ok(inner) = buf.try_lock() {
                inner.len() > 0
            } else {
                true
            }
        });
    }
}
