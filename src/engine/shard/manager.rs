use crate::engine::compactor::background::start_background_compactor;
use crate::engine::core::FlushWorker;
use crate::engine::shard::Shard;
use crate::shared::path::absolutize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug)]
pub struct ShardManager {
    pub shards: Vec<Shard>,
}

impl ShardManager {
    /// Create and initialize all shards with WAL, flush workers, and background compactors.
    pub async fn new(num_shards: usize, base_dir: PathBuf, wal_dir: PathBuf) -> Self {
        info!(target: "shard::manager", "Initializing ShardManager with {num_shards} shards");
        let base_dir = absolutize(base_dir);
        let wal_dir = absolutize(wal_dir);
        let mut shards = Vec::with_capacity(num_shards);

        for id in 0..num_shards {
            info!(target: "shard::manager", shard_id = id, "Spawning shard");

            let shard_base_dir = base_dir.join(format!("shard-{id}"));
            let shard_wal_dir = wal_dir.join(format!("shard-{id}"));

            // Spawn shard and get its flush channel and coordination lock
            let (shard, flush_rx, flush_lock) =
                Shard::spawn(id, shard_base_dir.clone(), shard_wal_dir).await;

            // Spawn flush worker with the shard's coordination lock
            let flush_worker = FlushWorker::new(id, shard_base_dir.clone(), flush_lock);
            tokio::spawn(async move {
                if let Err(e) = flush_worker.run(flush_rx).await {
                    error!(target: "shard::manager", shard_id = id, "Flush worker crashed: {e}");
                }
            });

            // Start background compactor
            // start_background_compactor(id as u32, shard_base_dir.clone()).await;

            shards.push(shard);
        }

        info!(target: "shard::manager", "ShardManager initialized with {} shards", shards.len());
        Self { shards }
    }

    /// Return all shards.
    pub fn all_shards(&self) -> &[Shard] {
        &self.shards
    }

    /// Select a shard by hashing the context_id.
    pub fn get_shard(&self, context_id: &str) -> &Shard {
        let mut hasher = DefaultHasher::new();
        context_id.hash(&mut hasher);
        let shard_id = (hasher.finish() as usize) % self.shards.len();
        &self.shards[shard_id]
    }
}
