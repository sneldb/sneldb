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
            let segment_dir = SegmentId::from(segment_id as u32).join_dir(&self.base_dir);

            info!(
                target: "sneldb::flush",
                shard_id = self.shard_id,
                segment_id,
                path = ?segment_dir,
                "Starting flush"
            );

            let flusher = Flusher::new(
                memtable,
                segment_id,
                &segment_dir,
                Arc::clone(&registry),
                Arc::clone(&self.flush_coordination_lock),
            );
            let flush_result = flusher.flush().await;

            match &flush_result {
                Err(e) => {
                    error!(
                        target: "sneldb::flush",
                        shard_id = self.shard_id,
                        segment_id,
                        error = %e,
                        "Flush failed"
                    );
                }
                Ok(()) => {
                    info!(
                        target: "sneldb::flush",
                        shard_id = self.shard_id,
                        segment_id,
                        "Flush succeeded"
                    );

                    debug!(
                        target: "sneldb::flush",
                        shard_id = self.shard_id,
                        segment_id,
                        "Clearing passive MemTable"
                    );
                    let mut pmem = passive_memtable.lock().await;
                    pmem.flush();

                    debug!(
                        target: "sneldb::flush",
                        shard_id = self.shard_id,
                        wal_cutoff = segment_id + 1,
                        "Cleaning up WAL files"
                    );
                    let cleaner = WalCleaner::new(self.shard_id);
                    cleaner.cleanup_up_to(segment_id + 1);
                }
            }

            if let Some(completion) = completion {
                let _ = completion.send(flush_result);
            }
        }

        Ok(())
    }
}
