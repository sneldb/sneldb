use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::{RwLock, mpsc::channel};
use tracing::{debug, info, warn};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::Query {
        event_type,
        context_id,
        since,
        where_clause,
        limit,
        ..
    } = cmd
    else {
        warn!(target: "sneldb::query", "Invalid Query command received");
        let resp = Response::error(StatusCode::BadRequest, "Invalid Query command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    if event_type.trim().is_empty() {
        warn!(target: "sneldb::query", "Empty event_type in Query command");
        let resp = Response::error(StatusCode::BadRequest, "event_type cannot be empty");
        return writer.write_all(&renderer.render(&resp)).await;
    }

    debug!(
        target: "sneldb::query",
        event_type,
        context_id = context_id.as_deref().unwrap_or("<none>"),
        since = ?since,
        limit = ?limit,
        has_filter = where_clause.is_some(),
        "Dispatching Query command to all shards"
    );

    let (tx, mut rx) = channel(4096);

    for shard in shard_manager.all_shards() {
        let tx_clone = tx.clone();
        info!(target: "sneldb::query", shard_id = shard.id, "Sending Query to shard");
        let _ = shard
            .tx
            .send(ShardMessage::Query(
                cmd.clone(),
                tx_clone,
                Arc::clone(registry),
            ))
            .await;
    }

    drop(tx);

    let mut all_results = vec![];
    while let Some(received) = rx.recv().await {
        all_results.extend(received);
    }

    if all_results.is_empty() {
        info!(target: "sneldb::query", event_type, "Query returned no results");
        let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
        return writer.write_all(&renderer.render(&resp)).await;
    } else {
        let count = all_results.len();
        info!(
            target: "sneldb::query",
            event_type,
            result_count = count,
            "Query returned results"
        );

        let json_rows: Vec<Value> = all_results
            .into_iter()
            .map(|event| {
                let mut obj = serde_json::Map::new();
                obj.insert("context_id".to_string(), json!(event.context_id));
                obj.insert("event_type".to_string(), json!(event.event_type));
                obj.insert("timestamp".to_string(), json!(event.timestamp));
                obj.insert("payload".to_string(), event.payload);
                Value::Object(obj)
            })
            .collect();

        let resp = Response::ok_json(json_rows, count);
        writer.write_all(&renderer.render(&resp)).await
    }
}
