use crate::engine::compactor::background::start_background_compactor;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::Shard;
use crate::engine::shard::message::ShardMessage;
use crate::shared::path::absolutize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::info;

#[derive(Debug)]
pub struct ShardManager {
    pub shards: Vec<Shard>,
}

impl ShardManager {
    /// Create and initialize all shards with WAL and background compactors.
    pub async fn new(num_shards: usize, base_dir: PathBuf, wal_dir: PathBuf) -> Self {
        info!(target: "shard::manager", "Initializing ShardManager with {num_shards} shards");
        let base_dir = absolutize(base_dir);
        let wal_dir = absolutize(wal_dir);
        let mut shards = Vec::with_capacity(num_shards);

        for id in 0..num_shards {
            info!(target: "shard::manager", shard_id = id, "Spawning shard");

            let shard_base_dir = base_dir.join(format!("shard-{id}"));
            let shard_wal_dir = wal_dir.join(format!("shard-{id}"));

            // Spawn shard and get its shared coordination state
            // Note: Flush management is handled internally by FlushManager within ShardContext
            let (shard, shared_state) =
                Shard::spawn(id, shard_base_dir.clone(), shard_wal_dir).await;

            // Start background compactor
            start_background_compactor(
                id as u32,
                shard_base_dir.clone(),
                Arc::clone(&shared_state.segment_ids),
                Arc::clone(&shared_state.flush_lock),
            )
            .await;

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

    /// Flush all shards and wait for completion. Returns a list of (shard_id, error) pairs.
    pub async fn flush_all(
        &self,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
    ) -> Vec<(usize, String)> {
        let mut completions = Vec::new();
        let mut errors = Vec::new();

        for shard in &self.shards {
            let (tx, rx) = oneshot::channel();
            match shard
                .tx
                .send(ShardMessage::Flush {
                    registry: Arc::clone(&registry),
                    completion: tx,
                })
                .await
            {
                Ok(_) => completions.push((shard.id, rx)),
                Err(e) => errors.push((shard.id, format!("Failed to send flush command: {}", e))),
            }
        }

        for (shard_id, rx) in completions {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => errors.push((shard_id, err)),
                Err(_) => errors.push((shard_id, "Flush completion channel dropped".to_string())),
            }
        }

        errors
    }

    /// Signal all shards to shutdown and wait for acknowledgement. Returns (shard_id, error) pairs.
    pub async fn shutdown_all(&self) -> Vec<(usize, String)> {
        let mut completions = Vec::new();
        let mut errors = Vec::new();

        for shard in &self.shards {
            let (tx, rx) = oneshot::channel();
            match shard
                .tx
                .send(ShardMessage::Shutdown { completion: tx })
                .await
            {
                Ok(_) => completions.push((shard.id, rx)),
                Err(e) => {
                    errors.push((shard.id, format!("Failed to send shutdown command: {}", e)))
                }
            }
        }

        for (shard_id, rx) in completions {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => errors.push((shard_id, err)),
                Err(_) => {
                    errors.push((shard_id, "Shutdown completion channel dropped".to_string()))
                }
            }
        }

        errors
    }
}
