use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};
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
        offset,
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
        offset = ?offset,
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

    // Collect and merge QueryResult, then finalize to ResultTable once
    let mut acc_opt: Option<QueryResult> = None;
    while let Some(msg) = rx.recv().await {
        if let Some(cur) = &mut acc_opt {
            let nxt = msg;
            cur.merge(nxt);
        } else {
            acc_opt = Some(msg);
        }
    }

    let Some(result) = acc_opt else {
        info!(target: "sneldb::query", event_type, "Query returned no results");
        let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
        return writer.write_all(&renderer.render(&resp)).await;
    };

    match result {
        QueryResult::Selection(sel) => {
            let mut table = sel.finalize();
            // Apply global OFFSET then LIMIT across merged shard results.
            // Only sort when pagination is requested (offset or limit) to avoid O(n log n) on large full scans.
            let need_sort = offset.is_some() || limit.is_some();
            if need_sort && !table.rows.is_empty() && !table.rows[0].is_empty() {
                table
                    .rows
                    .sort_unstable_by(|a, b| a[0].to_string().cmp(&b[0].to_string()));
            }
            if let Some(off) = offset {
                let off = *off as usize;
                if off > 0 {
                    if off >= table.rows.len() {
                        table.rows.clear();
                    } else {
                        table.rows.drain(0..off);
                    }
                }
            }
            if let Some(lim) = limit {
                let lim = *lim as usize;
                if table.rows.len() > lim {
                    table.rows.truncate(lim);
                }
            }
            if table.rows.is_empty() {
                info!(target: "sneldb::query", event_type, "Query returned no results");
                let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
                return writer.write_all(&renderer.render(&resp)).await;
            }
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
            let mut table = agg.finalize();
            // Apply LIMIT to aggregation rows as well; order by bucket/group columns for stability
            if let Some(lim) = limit {
                if !table.rows.is_empty() && !table.rows[0].is_empty() {
                    table.rows.sort_by(|a, b| {
                        let av = a.get(0);
                        let bv = b.get(0);
                        match (av, bv) {
                            (Some(va), Some(vb)) => va.to_string().cmp(&vb.to_string()),
                            (Some(_), None) => std::cmp::Ordering::Less,
                            (None, Some(_)) => std::cmp::Ordering::Greater,
                            (None, None) => std::cmp::Ordering::Equal,
                        }
                    });
                }
                let lim = *lim as usize;
                if table.rows.len() > lim {
                    table.rows.truncate(lim);
                }
            }
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
