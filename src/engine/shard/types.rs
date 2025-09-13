use crate::engine::core::MemTable;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::context::ShardContext;
use crate::engine::shard::message::ShardMessage;
use crate::engine::shard::worker::run_worker_loop;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{
    RwLock,
    mpsc::{Receiver, Sender, channel},
};
use tracing::{error, info};

#[derive(Debug)]
pub struct Shard {
    pub id: usize,
    pub tx: Sender<ShardMessage>,
}

impl Shard {
    /// Spawns a shard worker with its context and channels.
    /// Returns the shard handle and a receiver for flush operations.
    pub async fn spawn(
        id: usize,
        base_dir: PathBuf,
        wal_dir: PathBuf,
    ) -> (
        Self,
        Receiver<(
            u64,
            MemTable,
            Arc<RwLock<SchemaRegistry>>,
            Arc<tokio::sync::Mutex<MemTable>>,
        )>,
    ) {
        // Channels: one for shard messages, one for flush notifications
        let (flush_tx, flush_rx) = channel(4096);
        let (tx, rx) = channel(4096);

        // Ensure shard directory exists
        if let Err(e) = std::fs::create_dir_all(&base_dir) {
            error!(target: "shard::types", shard_id = id, "Failed to create shard base directory: {}", e);
            panic!("Shard-{id}: Failed to create shard base directory: {e}");
        }

        // Create shard context (includes WAL + MemTable)
        let ctx = ShardContext::new(id, flush_tx.clone(), base_dir.clone(), wal_dir.clone());

        info!(
            target: "shard::types",
            shard_id = id,
            ?base_dir,
            ?wal_dir,
            "Shard context created"
        );

        // Spawn worker loop for shard
        tokio::spawn(async move {
            info!(target: "shard::types", shard_id = id, "Shard worker started");
            run_worker_loop(ctx, rx).await;
            info!(target: "shard::types", shard_id = id, "Shard worker exited");
        });

        // Give some time for the shard to initialize (ensures WAL thread starts)
        tokio::time::sleep(Duration::from_millis(100)).await;

        info!(target: "shard::types", shard_id = id, "Shard spawned successfully");

        (Shard { id, tx }, flush_rx)
    }
}
