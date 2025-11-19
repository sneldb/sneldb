use crate::engine::core::segment::segment_id::SegmentId;
use crate::engine::core::{
    Flusher, MemTable, SegmentLifecycleTracker, SegmentVerifier, WalCleaner,
};
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc::Receiver, oneshot};
use tracing::{debug, error, info, warn};

const SEGMENT_VERIFY_MAX_ATTEMPTS: u32 = 5;
const SEGMENT_VERIFY_RETRY_DELAY_MS: u64 = 50;

/// Worker that processes MemTables and writes them to disk
pub struct FlushWorker {
    shard_id: usize,
    base_dir: PathBuf,
    flush_coordination_lock: Arc<Mutex<()>>,
    segment_lifecycle: Arc<SegmentLifecycleTracker>,
}

impl FlushWorker {
    /// Creates a new FlushWorker instance
    pub fn new(
        shard_id: usize,
        base_dir: PathBuf,
        flush_coordination_lock: Arc<Mutex<()>>,
        segment_lifecycle: Arc<SegmentLifecycleTracker>,
    ) -> Self {
        Self {
            shard_id,
            base_dir,
            flush_coordination_lock,
            segment_lifecycle,
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
            let segment_dir = SegmentId::from(segment_id as u32).join_dir(&self.base_dir);
            let shard_id = self.shard_id;
            let flush_coord_lock = Arc::clone(&self.flush_coordination_lock);
            let lifecycle = Arc::clone(&self.segment_lifecycle);
            let base_dir = self.base_dir.clone();

            let flush_task = tokio::spawn(async move {
                let was_empty = memtable.is_empty();

                info!(
                    target: "sneldb::flush",
                    shard_id,
                    segment_id,
                    path = ?segment_dir,
                    empty = was_empty,
                    "Starting flush"
                );

                let track_lifecycle = !was_empty;
                if track_lifecycle {
                    lifecycle
                        .register_flush(segment_id, Arc::clone(&passive_memtable))
                        .await;
                }

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
                        return flush_result;
                    }
                    Ok(()) => {
                        if was_empty {
                            debug!(
                                target: "sneldb::flush",
                                shard_id,
                                segment_id,
                                "Flush skipped (empty memtable)"
                            );
                            return flush_result;
                        }

                        info!(
                            target: "sneldb::flush",
                            shard_id,
                            segment_id,
                            "Flush completed, verifying segment queryability"
                        );

                        // Mark as written to disk
                        if track_lifecycle {
                            lifecycle.mark_written(segment_id).await;
                        }

                        // Verify segment is queryable before clearing passive buffer
                        let verifier = SegmentVerifier::new(base_dir, shard_id);
                        let is_queryable = verifier
                            .verify_with_retry(
                                segment_id,
                                SEGMENT_VERIFY_MAX_ATTEMPTS,
                                SEGMENT_VERIFY_RETRY_DELAY_MS,
                            )
                            .await;

                        if !is_queryable {
                            warn!(
                                target: "sneldb::flush",
                                shard_id,
                                segment_id,
                                "Segment verification failed after retries, retaining passive buffer"
                            );
                            return flush_result;
                        }

                        // Mark as verified and clear passive buffer
                        if track_lifecycle {
                            lifecycle.mark_verified(segment_id).await;

                            if let Some(passive) = lifecycle.clear_and_complete(segment_id).await {
                                passive.lock().await.flush();
                                debug!(
                                    target: "sneldb::flush",
                                    shard_id,
                                    segment_id,
                                    "Passive buffer cleared after verification"
                                );
                            } else {
                                warn!(
                                    target: "sneldb::flush",
                                    shard_id,
                                    segment_id,
                                    "Passive buffer missing when attempting to clear after verification"
                                );
                            }
                        }

                        // Note: Passive buffer is now empty and will be filtered out by
                        // PassiveBufferSet::non_empty() in subsequent queries

                        // Clean up WAL files
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
