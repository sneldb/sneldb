use crate::command::types::Command;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{RwLock, mpsc::channel};
use tracing::{debug, info, warn};

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
        event_type,
        context_id,
        since = ?since,
        "Dispatching Replay command to responsible shard"
    );

    let shard = shard_manager.get_shard(context_id);
    info!(
        target: "sneldb::replay",
        shard_id = shard.id,
        "Sending Replay to shard"
    );

    let (tx, mut rx) = channel(4096);
    let tx_clone = tx.clone();

    tokio::spawn(async {
        tokio::task::yield_now().await;
    });

    let _ = shard
        .tx
        .send(ShardMessage::Replay(
            cmd.clone(),
            tx_clone,
            Arc::clone(registry),
        ))
        .await;

    drop(tx);

    let mut all_results = vec![];
    while let Some(received) = rx.recv().await {
        all_results.extend(received);
    }

    if all_results.is_empty() {
        info!(
            target: "sneldb::replay",
            context_id,
            "Replay returned no results"
        );
        let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
        return writer.write_all(&renderer.render(&resp)).await;
    } else {
        let count = all_results.len();
        info!(
            target: "sneldb::replay",
            context_id,
            result_count = count,
            "Replay returned results"
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
