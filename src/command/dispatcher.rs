use crate::command::handlers::query::QueryCommandHandler;
use crate::command::handlers::{auth, compare, define, flush, remember, replay, show, store};
use crate::command::types::Command;
use crate::engine::auth::AuthManager;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, error};

pub async fn dispatch_command<W: AsyncWrite + Unpin>(
    cmd: &Command,
    writer: &mut W,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    auth_manager: Option<&Arc<AuthManager>>,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    use Command::*;

    debug!(target: "sneldb::dispatch", command = ?cmd, "Dispatching command");

    match cmd {
        Store { .. } => store::handle(cmd, shard_manager, registry, writer, renderer).await,
        Define { .. } => define::handle(cmd, shard_manager, registry, writer, renderer).await,
        RememberQuery { .. } => {
            remember::handle(cmd, shard_manager, registry, writer, renderer).await
        }
        Query { .. } => {
            QueryCommandHandler::new(cmd, shard_manager, Arc::clone(registry), writer, renderer)
                .handle()
                .await
        }
        Compare { .. } => {
            compare::ComparisonCommandHandler::new(
                cmd,
                shard_manager,
                Arc::clone(registry),
                writer,
                renderer,
            )
            .handle()
            .await
        }
        Replay { .. } => replay::handle(cmd, shard_manager, registry, writer, renderer).await,
        ShowMaterialized { .. } => {
            show::handle(cmd, shard_manager, registry, writer, renderer).await
        }
        Flush { .. } => flush::handle(cmd, shard_manager, registry, writer, renderer).await,
        CreateUser { .. } | RevokeKey { .. } | ListUsers => {
            if let Some(auth_mgr) = auth_manager {
                auth::handle(cmd, auth_mgr, writer, renderer).await
            } else {
                error!(target: "sneldb::dispatch", "Auth manager not available");
                let resp =
                    Response::error(StatusCode::InternalError, "Authentication not configured");
                writer.write_all(&renderer.render(&resp)).await?;
                writer.flush().await?;
                Ok(())
            }
        }
        _ => {
            error!(target: "sneldb::dispatch", ?cmd, "Unreachable command variant encountered");
            unreachable!("dispatch_command called with non-command")
        }
    }
}
