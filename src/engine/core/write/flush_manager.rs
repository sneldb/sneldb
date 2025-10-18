use crate::engine::core::FlushWorker;
use crate::engine::core::MemTable;
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, RwLock as TokioRwLock, mpsc::Sender};
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
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);

        // Spawn the flush worker task
        let worker = FlushWorker::new(shard_id, base_dir.clone(), flush_coordination_lock);

        tokio::spawn(async move {
            if let Err(e) = worker.run(rx).await {
                error!(target: "sneldb::flush", shard_id, error = %e, "Flush worker failed");
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
        {
            let mut segs = self.segment_ids.write().unwrap();
            segs.push(segment_name.clone());
        }

        info!(
            target: "sneldb::flush",
            shard_id = self.shard_id,
            segment_id,
            "MemTable queued for flush to segment '{}'",
            segment_name
        );

        debug!(
            target: "sneldb::flush",
            shard_id = self.shard_id,
            segment_ids = ?self.segment_ids.read().unwrap(),
            "Updated segment list"
        );

        Ok(())
    }

    /// Gets the current list of segment IDs
    pub fn get_segment_ids(&self) -> Vec<String> {
        self.segment_ids.read().unwrap().clone()
    }
}
