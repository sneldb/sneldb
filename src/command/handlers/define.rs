use crate::command::types::Command;
use crate::engine::define::run as engine_define;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    _shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::Define {
        event_type,
        version,
        schema,
    } = cmd
    else {
        let resp = Response::error(StatusCode::BadRequest, "Invalid Define command");
        error!(target: "sneldb::define", "Received invalid Define command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    debug!(
        target: "sneldb::define",
        event_type, version = ?version, "Defining schema for event_type"
    );

    let mut registry = registry.write().await;

    match engine_define::define_schema(
        &mut registry,
        event_type,
        version.unwrap_or(1),
        schema.clone().into(),
    ) {
        Ok(_) => {
            info!(
                target: "sneldb::define",
                event_type, "Schema defined successfully"
            );
            let resp = Response::ok_lines(vec![format!("Schema defined for '{}'", event_type)]);
            return writer.write_all(&renderer.render(&resp)).await;
        }
        Err(e) => {
            error!(
                target: "sneldb::define",
                event_type, error = %e, "Failed to define schema"
            );
            let resp = Response::error(StatusCode::InternalError, format!("Define failed: {}", e));
            return writer.write_all(&renderer.render(&resp)).await;
        }
    }
}
