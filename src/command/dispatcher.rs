use crate::command::handlers::{define, flush, query, replay, store};
use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::RwLock;
use tracing::{debug, error};

pub async fn dispatch_command<W: AsyncWrite + Unpin>(
    cmd: &Command,
    writer: &mut W,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    use Command::*;

    debug!(target: "sneldb::dispatch", command = ?cmd, "Dispatching command");

    match cmd {
        Store { .. } => store::handle(cmd, shard_manager, registry, writer, renderer).await,
        Define { .. } => define::handle(cmd, shard_manager, registry, writer, renderer).await,
        Query { .. } => query::handle(cmd, shard_manager, registry, writer, renderer).await,
        Replay { .. } => replay::handle(cmd, shard_manager, registry, writer, renderer).await,
        Flush { .. } => flush::handle(cmd, shard_manager, registry, writer, renderer).await,
        _ => {
            error!(target: "sneldb::dispatch", ?cmd, "Unreachable command variant encountered");
            unreachable!("dispatch_command called with non-command")
        }
    }
}
