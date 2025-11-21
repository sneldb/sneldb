use crate::command::handlers::query::QueryExecutionPipeline;
use crate::command::handlers::query::QueryResponseWriter;
use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Handles REPLAY commands by converting them to streaming QUERY commands.
///
/// REPLAY uses the same streaming infrastructure as QUERY, which provides:
/// - Memory efficiency (no buffering entire result set)
/// - Backpressure support
/// - Incremental response delivery
pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::Replay {
        event_type,
        context_id,
        since,
        ..
    } = cmd
    else {
        warn!(target: "sneldb::replay", "Invalid Replay command received");
        let resp = Response::error(StatusCode::BadRequest, "Invalid Replay command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    if context_id.trim().is_empty() {
        warn!(target: "sneldb::replay", "Empty context_id in Replay command");
        let resp = Response::error(StatusCode::BadRequest, "context_id cannot be empty");
        return writer.write_all(&renderer.render(&resp)).await;
    }

    debug!(
        target: "sneldb::replay",
        event_type = event_type.as_deref().unwrap_or("*"),
        context_id,
        since = ?since,
        "Processing Replay command via streaming query path"
    );

    // Ensure any in-flight flushes finish so replay sees the segments they produce.
    let flush_errors = shard_manager.wait_for_flush_completion().await;
    if !flush_errors.is_empty() {
        let mut joined = String::new();
        for (idx, (shard_id, err)) in flush_errors.iter().enumerate() {
            if idx > 0 {
                joined.push_str(", ");
            }
            let _ = write!(joined, "shard {}: {}", shard_id, err);
        }
        warn!(
            target: "sneldb::replay",
            context_id,
            errors = %joined,
            "Replay waited for flushes but encountered errors"
        );
        let resp = Response::error(
            StatusCode::InternalError,
            &format!("Replay failed to wait for pending flushes: {joined}"),
        );
        return writer.write_all(&renderer.render(&resp)).await;
    }

    // Convert REPLAY to QUERY command
    let query_cmd = match cmd.to_query_command() {
        Some(q) => q,
        None => {
            warn!(target: "sneldb::replay", "Failed to convert Replay to Query command");
            let resp = Response::error(
                StatusCode::InternalError,
                "Failed to process Replay command",
            );
            return writer.write_all(&renderer.render(&resp)).await;
        }
    };

    // Create query execution pipeline with the converted command
    let pipeline = QueryExecutionPipeline::new(&query_cmd, shard_manager, Arc::clone(registry));

    // Execute using streaming path
    match pipeline.execute_streaming().await {
        Ok(Some(stream)) => {
            info!(
                target: "sneldb::replay",
                context_id,
                "Streaming replay results"
            );

            // Use QueryResponseWriter to stream results incrementally
            // No limit/offset for replay - return all matching events
            let response_writer = QueryResponseWriter::new(
                writer,
                renderer,
                stream.schema(),
                None, // No limit
                None, // No offset
            );
            response_writer.write(stream).await
        }
        Ok(None) => {
            warn!(target: "sneldb::replay", "Streaming not available for Replay");
            let resp = Response::error(
                StatusCode::InternalError,
                "Replay requires streaming execution",
            );
            writer.write_all(&renderer.render(&resp)).await
        }
        Err(error) => {
            warn!(
                target: "sneldb::replay",
                error = %error,
                context_id,
                "Replay execution failed"
            );
            let resp = Response::error(
                StatusCode::InternalError,
                &format!("Replay failed: {error}"),
            );
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}
