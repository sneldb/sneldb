use crate::engine::core::{FlushWorker, InflightSegments, MemTable, SegmentLifecycleTracker};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::flush_progress::FlushProgress;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc::Sender, oneshot};
use tracing::{debug, error, info};

/// Manages the flushing of MemTables to disk segments
#[derive(Debug, Clone)]
pub struct FlushManager {
    shard_id: usize,
    flush_sender: Sender<(
        u64,
        MemTable,
        Arc<TokioRwLock<SchemaRegistry>>,
        Arc<Mutex<MemTable>>,
        u64,
        Option<oneshot::Sender<Result<(), StoreError>>>,
    )>,
    segment_ids: Arc<RwLock<Vec<String>>>,
}

impl FlushManager {
    /// Creates a new FlushManager instance
    pub fn new(
        shard_id: usize,
        base_dir: PathBuf,
        segment_ids: Arc<RwLock<Vec<String>>>,
        flush_coordination_lock: Arc<Mutex<()>>,
        segment_lifecycle: Arc<SegmentLifecycleTracker>,
        flush_progress: Arc<FlushProgress>,
        inflight_segments: InflightSegments,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);

        // Spawn the flush worker task
        let worker = FlushWorker::new(
            shard_id,
            base_dir.clone(),
            flush_coordination_lock,
            Arc::clone(&segment_ids),
            segment_lifecycle,
            flush_progress,
            inflight_segments,
        );

        let worker_handle = tokio::spawn(async move {
            let result = worker.run(rx).await;
            match &result {
                Ok(()) => {
                    info!(
                        target: "sneldb::flush",
                        shard_id,
                        "Flush worker exited normally (receiver closed)"
                    );
                }
                Err(e) => {
                    error!(
                        target: "sneldb::flush",
                        shard_id,
                        error = %e,
                        "Flush worker exited with error"
                    );
                }
            }
            result
        });

        // Monitor the worker task to detect if it panics
        let shard_id_for_monitor = shard_id;
        tokio::spawn(async move {
            let result = worker_handle.await;
            match result {
                Ok(Ok(())) => {
                    // Normal exit
                }
                Ok(Err(e)) => {
                    error!(
                        target: "sneldb::flush",
                        shard_id = shard_id_for_monitor,
                        error = %e,
                        "Flush worker task completed with error"
                    );
                }
                Err(_panic) => {
                    // Task panicked
                    error!(
                        target: "sneldb::flush",
                        shard_id = shard_id_for_monitor,
                        "Flush worker task panicked and crashed - channel will be closed"
                    );
                }
            }
        });

        info!(target: "sneldb::flush", shard_id, "FlushManager started");
        Self {
            shard_id,
            flush_sender: tx,
            segment_ids,
        }
    }

    /// Queues a full MemTable for flushing
    pub async fn queue_for_flush(
        &self,
        full_memtable: MemTable,
        schema_registry: Arc<TokioRwLock<SchemaRegistry>>,
        segment_id: u64,
        passive_memtable: Arc<Mutex<MemTable>>,
        flush_id: u64,
        completion: Option<oneshot::Sender<Result<(), StoreError>>>,
    ) -> Result<(), StoreError> {
        debug!(
            target: "sneldb::flush",
            shard_id = self.shard_id,
            segment_id,
            "Queueing MemTable for flush"
        );

        self.flush_sender
            .send((
                segment_id,
                full_memtable,
                Arc::clone(&schema_registry),
                Arc::clone(&passive_memtable),
                flush_id,
                completion,
            ))
            .await
            .map_err(|e| {
                error!(
                    target: "sneldb::flush",
                    shard_id = self.shard_id,
                    segment_id,
                    error = %e,
                    "Failed to send MemTable to flush worker"
                );
                StoreError::FlushFailed(format!("flush send error: {}", e))
            })?;

        let segment_name = format!("{:05}", segment_id);

        info!(
            target: "sneldb::flush",
            shard_id = self.shard_id,
            segment_id,
            "MemTable queued for flush to segment '{}'",
            segment_name
        );

        Ok(())
    }

    /// Gets the current list of segment IDs
    pub fn get_segment_ids(&self) -> Vec<String> {
        self.segment_ids.read().unwrap().clone()
    }
}
