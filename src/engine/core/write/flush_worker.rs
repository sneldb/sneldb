use crate::engine::core::Flusher;
use crate::engine::core::MemTable;
use crate::engine::core::WalCleaner;
use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc::Receiver, oneshot};
use tracing::{debug, error, info};

/// Worker that processes MemTables and writes them to disk
pub struct FlushWorker {
    shard_id: usize,
    base_dir: PathBuf,
    flush_coordination_lock: Arc<Mutex<()>>,
}

impl FlushWorker {
    /// Creates a new FlushWorker instance
    pub fn new(
        shard_id: usize,
        base_dir: PathBuf,
        flush_coordination_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            shard_id,
            base_dir,
            flush_coordination_lock,
        }
    }

    /// Runs the worker loop that processes MemTables
    pub async fn run(
        &self,
        mut rx: Receiver<(
            u64,
            MemTable,
            Arc<TokioRwLock<SchemaRegistry>>,
            Arc<tokio::sync::Mutex<MemTable>>,
            Option<oneshot::Sender<Result<(), StoreError>>>,
        )>,
    ) -> Result<(), StoreError> {
        while let Some((segment_id, memtable, registry, passive_memtable, completion)) =
            rx.recv().await
        {
            // Spawn the flush operation in a separate task to catch panics
            let segment_dir = SegmentId::from(segment_id as u32).join_dir(&self.base_dir);
            let shard_id = self.shard_id;
            let flush_coord_lock = Arc::clone(&self.flush_coordination_lock);

            let flush_task = tokio::spawn(async move {
                // Check if memtable is empty before flushing
                // If empty, we should not clear passive memtable or clean WAL files
                let was_empty = memtable.is_empty();

                info!(
                    target: "sneldb::flush",
                    shard_id,
                    segment_id,
                    path = ?segment_dir,
                    empty = was_empty,
                    "Starting flush"
                );

                let flusher = Flusher::new(
                    memtable,
                    segment_id,
                    &segment_dir,
                    Arc::clone(&registry),
                    Arc::clone(&flush_coord_lock),
                );
                let flush_result = flusher.flush().await;

                match &flush_result {
                    Err(e) => {
                        error!(
                            target: "sneldb::flush",
                            shard_id,
                            segment_id,
                            error = %e,
                            "Flush failed"
                        );
                    }
                    Ok(()) => {
                        if was_empty {
                            debug!(
                                target: "sneldb::flush",
                                shard_id,
                                segment_id,
                                "Flush skipped (empty memtable), not cleaning up passive memtable or WAL"
                            );
                        } else {
                            info!(
                                target: "sneldb::flush",
                                shard_id,
                                segment_id,
                                "Flush succeeded"
                            );

                            debug!(
                                target: "sneldb::flush",
                                shard_id,
                                segment_id,
                                "Clearing passive MemTable"
                            );
                            let mut pmem = passive_memtable.lock().await;
                            pmem.flush();

                            debug!(
                                target: "sneldb::flush",
                                shard_id,
                                wal_cutoff = segment_id + 1,
                                "Cleaning up WAL files"
                            );
                            let cleaner = WalCleaner::new(shard_id);
                            cleaner.cleanup_up_to(segment_id + 1);
                        }
                    }
                }

                flush_result
            });

            // Await the task and handle panics
            let flush_result = match flush_task.await {
                Ok(Ok(flush_result)) => Ok(flush_result),
                Ok(Err(e)) => Err(e),
                Err(_panic) => {
                    // Task panicked - log it and return an error but continue processing
                    // The panic info is captured by tokio
                    error!(
                        target: "sneldb::flush",
                        shard_id = self.shard_id,
                        segment_id,
                        "Flush worker task panicked during flush operation"
                    );
                    Err(StoreError::FlushFailed(
                        "panic during flush operation - task panicked".to_string(),
                    ))
                }
            };

            // Always send completion signal, even on error/panic
            if let Some(completion) = completion {
                let _ = completion.send(flush_result);
            }
        }

        // Only log if the receiver was closed (shouldn't normally happen)
        info!(
            target: "sneldb::flush",
            shard_id = self.shard_id,
            "Flush worker receiver closed, worker exiting"
        );
        Ok(())
    }
}
