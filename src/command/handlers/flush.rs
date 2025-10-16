use crate::command::types::Command;
use crate::engine::core::MemTable;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::config::CONFIG;
use crate::shared::response::Response;
use crate::shared::response::render::Renderer;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, mpsc::channel};
use tracing::{debug, info};

pub async fn handle<W: AsyncWrite + Unpin>(
    _cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    debug!(target: "sneldb::flush", "Received Flush command");

    let (tx, _rx) = channel(4096);
    let capacity = CONFIG.engine.fill_factor * CONFIG.engine.event_per_zone;

    for shard in shard_manager.all_shards() {
        let tx_clone = tx.clone();
        info!(
            target: "sneldb::flush",
            shard_id = shard.id,
            "Sending Flush to shard"
        );
        let _ = shard
            .tx
            .send(ShardMessage::Flush(
                tx_clone,
                Arc::clone(registry),
                Arc::new(tokio::sync::Mutex::new(MemTable::new(capacity))),
            ))
            .await;
    }

    drop(tx);

    info!(target: "sneldb::flush", "Flush commands dispatched to all shards");
    let resp = Response::ok_lines(vec!["Flush command accepted".to_string()]);
    writer.write_all(&renderer.render(&resp)).await
}
