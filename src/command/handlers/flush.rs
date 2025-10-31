use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, error, info};

pub async fn handle<W: AsyncWrite + Unpin>(
    _cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    debug!(target: "sneldb::flush", "Received Flush command");

    let mut completions = Vec::new();
    let mut errors = Vec::new();

    for shard in shard_manager.all_shards() {
        info!(
            target: "sneldb::flush",
            shard_id = shard.id,
            "Sending Flush to shard"
        );

        let (tx, rx) = oneshot::channel();
        match shard
            .tx
            .send(ShardMessage::Flush {
                registry: Arc::clone(registry),
                completion: tx,
            })
            .await
        {
            Ok(_) => completions.push((shard.id, rx)),
            Err(e) => {
                error!(
                    target: "sneldb::flush",
                    shard_id = shard.id,
                    error = %e,
                    "Flush dispatch failed"
                );
                errors.push(format!(
                    "Failed to dispatch flush to shard {}: {}",
                    shard.id, e
                ));
            }
        }
    }

    for (shard_id, rx) in completions {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                error!(
                    target: "sneldb::flush",
                    shard_id,
                    error = %err,
                    "Flush failed"
                );
                errors.push(format!("Shard {} flush failed: {}", shard_id, err));
            }
            Err(_) => {
                error!(
                    target: "sneldb::flush",
                    shard_id,
                    "Flush completion channel dropped"
                );
                errors.push(format!(
                    "Shard {} did not acknowledge flush before shutdown",
                    shard_id
                ));
            }
        }
    }

    if errors.is_empty() {
        info!(target: "sneldb::flush", "Flush commands completed successfully");
        let resp = Response::ok_lines(vec!["Flush command completed".to_string()]);
        writer.write_all(&renderer.render(&resp)).await
    } else {
        let resp = Response::error(StatusCode::InternalError, errors.join("; "));
        writer.write_all(&renderer.render(&resp)).await
    }
}
