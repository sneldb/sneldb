use super::query_orchestrator::QueryOrchestrator;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Handles Query commands by orchestrating execution across shards.
///
/// This is a thin wrapper around QueryOrchestrator, focusing on:
/// - Input validation
/// - Response formatting
/// - Error handling
pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    // Validate command type
    let Command::Query {
        event_type,
        context_id,
        since,
        where_clause,
        limit,
        offset,
        ..
    } = cmd
    else {
        warn!(target: "sneldb::query", "Invalid Query command received");
        let resp = Response::error(StatusCode::BadRequest, "Invalid Query command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    // Validate event_type
    if event_type.trim().is_empty() {
        warn!(target: "sneldb::query", "Empty event_type in Query command");
        let resp = Response::error(StatusCode::BadRequest, "event_type cannot be empty");
        return writer.write_all(&renderer.render(&resp)).await;
    }

    // Validate OFFSET requires LIMIT (prevent unbounded result sets)
    if offset.is_some() && limit.is_none() {
        warn!(target: "sneldb::query", "OFFSET specified without LIMIT");
        let resp = Response::error(
            StatusCode::BadRequest,
            "OFFSET requires LIMIT to prevent unbounded results",
        );
        return writer.write_all(&renderer.render(&resp)).await;
    }

    debug!(
        target: "sneldb::query",
        event_type,
        context_id = context_id.as_deref().unwrap_or("<none>"),
        since = ?since,
        limit = ?limit,
        has_filter = where_clause.is_some(),
        "Dispatching Query command to orchestrator"
    );

    // Execute query through orchestrator
    let orchestrator = QueryOrchestrator::new(cmd, shard_manager, Arc::clone(registry));

    match orchestrator.execute().await {
        Ok(result) => format_and_write_result(result, writer, renderer).await,
        Err(e) => {
            warn!(target: "sneldb::query", error = %e, "Query execution failed");
            let resp = Response::error(StatusCode::InternalError, &format!("Query failed: {}", e));
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

/// Formats the query result and writes it to the output.
async fn format_and_write_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    match result {
        QueryResult::Selection(sel) => {
            let table = sel.finalize();

            // Check if empty
            if table.rows.is_empty() {
                let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
                return writer.write_all(&renderer.render(&resp)).await;
            }

            // Format columns and rows
            let columns = table
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.logical_type.clone()))
                .collect::<Vec<(String, String)>>();

            let rows = table.rows;
            let count = rows.len();

            let resp = Response::ok_table(columns, rows, count);
            writer.write_all(&renderer.render(&resp)).await
        }
        QueryResult::Aggregation(agg) => {
            let table = agg.finalize();

            // Format columns and rows
            let columns = table
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.logical_type.clone()))
                .collect::<Vec<(String, String)>>();

            let rows = table.rows;
            let count = rows.len();

            let resp = Response::ok_table(columns, rows, count);
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}
