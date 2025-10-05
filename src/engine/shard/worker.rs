use crate::engine::core::read::result::QueryResult;
use crate::engine::core::{FlushManager, MemTable, SegmentIdLoader};
use crate::engine::query::scan::scan;
use crate::engine::replay::scan::scan as replay_scan;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::context::ShardContext;
use crate::engine::shard::message::ShardMessage;
use crate::engine::store::insert::insert_and_maybe_flush;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, info};

const LOG_TARGET: &str = "engine::shard::worker";

/// Main worker loop for a shard.
/// Processes messages: Store, Query, Replay, Flush.
pub async fn run_worker_loop(mut ctx: ShardContext, mut rx: Receiver<ShardMessage>) {
    let id = ctx.id;
    info!(target: LOG_TARGET, shard_id = id, "Shard worker started");

    while let Some(msg) = rx.recv().await {
        match msg {
            ShardMessage::Store(event, registry) => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Store message");
                if let Err(e) = on_store(event, &mut ctx, &registry).await {
                    error!(target: LOG_TARGET, shard_id = id, error = %e, "Failed to store event");
                }
            }
            ShardMessage::Query(command, tx, registry) => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Query message");
                if let Err(e) = on_query(command, tx, &ctx, &registry).await {
                    error!(target: LOG_TARGET, shard_id = id, error = %e, "Query failed");
                }
            }
            ShardMessage::Replay(command, tx, registry) => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Replay message");
                if let Err(e) = on_replay(command, tx, &ctx, &registry).await {
                    error!(target: LOG_TARGET, shard_id = id, error = %e, "Replay failed");
                }
            }
            ShardMessage::Flush(_, registry, _) => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Flush message");
                if let Err(e) = on_flush(&mut ctx, &registry).await {
                    error!(target: LOG_TARGET, shard_id = id, error = %e, "Flush failed");
                }
            }
        }
    }

    info!(target: LOG_TARGET, shard_id = id, "Shard worker shutting down");
}

/// Handles Store messages.
async fn on_store(
    event: crate::engine::core::Event,
    ctx: &mut ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    insert_and_maybe_flush(event, ctx, registry)
        .await
        .map_err(|e| e.to_string())
}

/// Handles Query messages.
async fn on_query(
    command: crate::command::types::Command,
    tx: tokio::sync::mpsc::Sender<QueryResult>,
    ctx: &ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    let results = scan(
        &command,
        registry,
        &ctx.base_dir,
        &ctx.segment_ids,
        &ctx.memtable,
        &ctx.passive_buffers,
    )
    .await
    .map_err(|e| e.to_string())?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(target: LOG_TARGET, shard_id = ctx.id, "Query completed on shard");
    }
    tx.send(results).await.map_err(|e| e.to_string())
}

/// Handles Replay messages.
async fn on_replay(
    command: crate::command::types::Command,
    tx: tokio::sync::mpsc::Sender<Vec<crate::engine::core::Event>>,
    ctx: &ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    let results = replay_scan(
        &command,
        registry,
        &ctx.base_dir,
        &ctx.segment_ids,
        &ctx.memtable,
        &ctx.passive_buffers,
    )
    .await
    .map_err(|e| e.to_string())?;
    debug!(target: LOG_TARGET, shard_id = ctx.id, results_count = results.len(), "Replay completed");
    tx.send(results).await.map_err(|e| e.to_string())
}

/// Handles Flush messages.
async fn on_flush(
    ctx: &mut ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    let capacity = ctx.memtable.capacity();
    let segment_id = SegmentIdLoader::next_id(&ctx.segment_ids);

    // Move current memtable to a new passive buffer
    let passive = ctx.passive_buffers.add_from(&ctx.memtable).await;
    let flushed_mem = std::mem::replace(&mut ctx.memtable, MemTable::new(capacity));

    // Create flush manager and enqueue
    let flush_manager =
        FlushManager::new(ctx.id, ctx.base_dir.clone(), Arc::clone(&ctx.segment_ids));
    info!(target: LOG_TARGET, shard_id = ctx.id, "Queueing memtable for flush");
    flush_manager
        .queue_for_flush(
            flushed_mem,
            Arc::clone(registry),
            segment_id,
            Arc::clone(&passive),
        )
        .await
        .map_err(|e| e.to_string())
}
