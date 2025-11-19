use crate::engine::shard::context::ShardContext;
use crate::engine::shard::message::ShardMessage;
use crate::engine::shard::worker::run_worker_loop;
use std::path::PathBuf;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;
use tokio::sync::mpsc::{Sender, channel};
use tracing::{error, info};

#[derive(Debug)]
pub struct Shard {
    pub id: usize,
    pub tx: Sender<ShardMessage>,
    pub base_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ShardSharedState {
    pub flush_lock: Arc<tokio::sync::Mutex<()>>,
    pub segment_ids: Arc<StdRwLock<Vec<String>>>,
}

impl Shard {
    /// Spawns a shard worker with its context and command channel.
    pub async fn spawn(
        id: usize,
        base_dir: PathBuf,
        wal_dir: PathBuf,
    ) -> (Self, ShardSharedState) {
        // Channel for shard messages
        let (tx, rx) = channel(8096);

        // Ensure shard directory exists
        if let Err(e) = std::fs::create_dir_all(&base_dir) {
            error!(target: "shard::types", shard_id = id, "Failed to create shard base directory: {}", e);
            panic!("Shard-{id}: Failed to create shard base directory: {e}");
        }

        // Create shard context (includes WAL + MemTable)
        let ctx = ShardContext::new(id, base_dir.clone(), wal_dir.clone());

        // Clone shared state before moving ctx
        let flush_lock = ctx.flush_coordination_lock.clone();
        let segment_ids = ctx.segment_ids.clone();

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

        (
            Shard { id, tx, base_dir },
            ShardSharedState {
                flush_lock,
                segment_ids,
            },
        )
    }
}
