use crate::command::types::Command;
use crate::engine::core::Event;
use crate::engine::core::MemTable;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::query::scan::scan;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::context::ShardContext;
use crate::engine::shard::message::ShardMessage;
use crate::engine::store::insert::insert_and_maybe_flush;
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, oneshot};
use tracing::{debug, error, info};

const LOG_TARGET: &str = "engine::shard::worker";

/// Main worker loop for a shard.
/// Processes messages: Store, QueryStream, Flush, Shutdown.
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
            ShardMessage::QueryStream {
                command,
                metadata,
                response,
                registry,
            } => {
                debug!(target: LOG_TARGET, shard_id = id, "Received QueryStream message");
                let result = on_query_streaming(command, metadata, &ctx, &registry).await;
                if response.send(result).is_err() {
                    error!(target: LOG_TARGET, shard_id = id, "Streaming response receiver dropped");
                }
            }
            ShardMessage::Flush {
                registry,
                completion,
            } => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Flush message");
                match on_flush(&mut ctx, &registry).await {
                    Ok(()) => {
                        let _ = completion.send(Ok(()));
                    }
                    Err(e) => {
                        error!(target: LOG_TARGET, shard_id = id, error = %e, "Flush failed");
                        let _ = completion.send(Err(e));
                    }
                }
            }
            ShardMessage::AwaitFlush { completion } => {
                debug!(target: LOG_TARGET, shard_id = id, "Received AwaitFlush message");
                match on_wait_for_flush(&ctx).await {
                    Ok(()) => {
                        let _ = completion.send(Ok(()));
                    }
                    Err(e) => {
                        error!(target: LOG_TARGET, shard_id = id, error = %e, "AwaitFlush failed");
                        let _ = completion.send(Err(e));
                    }
                }
            }
            ShardMessage::Shutdown { completion } => {
                debug!(target: LOG_TARGET, shard_id = id, "Received Shutdown message");
                let result = on_shutdown(&mut ctx).await;
                if let Err(ref e) = result {
                    error!(target: LOG_TARGET, shard_id = id, error = %e, "Shutdown sequence failed");
                } else {
                    info!(target: LOG_TARGET, shard_id = id, "Shutdown sequence completed");
                }
                let _ = completion.send(result);
                break;
            }
        }
    }

    info!(target: LOG_TARGET, shard_id = id, "Shard worker shutting down");
}

/// Handles Store messages.
async fn on_store(
    event: Event,
    ctx: &mut ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    let mut event = event;
    if event.event_id().is_zero() {
        let id = ctx.next_event_id();
        event.set_event_id(id);
    }
    insert_and_maybe_flush(event, ctx, registry)
        .await
        .map_err(|e| e.to_string())
}

/// Handles Query messages.
async fn on_query_streaming(
    command: Command,
    metadata: Option<std::collections::HashMap<String, String>>,
    ctx: &ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<ShardFlowHandle, String> {
    scan(
        &command,
        metadata,
        registry,
        &ctx.base_dir,
        &ctx.segment_ids,
        &ctx.memtable,
        &ctx.passive_buffers,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Handles Flush messages.
async fn on_flush(
    ctx: &mut ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    let capacity = ctx.memtable.capacity();
    let segment_id = ctx.allocator.next_for_level(0) as u64;

    // Move current memtable to a new passive buffer
    let passive = ctx.passive_buffers.add_from(&ctx.memtable).await;
    let flushed_mem = std::mem::replace(&mut ctx.memtable, MemTable::new(capacity));

    // Use the existing flush manager from context
    info!(target: LOG_TARGET, shard_id = ctx.id, "Queueing memtable for flush");
    let (completion_tx, completion_rx) = oneshot::channel();
    let flush_id = ctx.flush_progress.next_id();
    ctx.flush_manager
        .queue_for_flush(
            flushed_mem,
            Arc::clone(registry),
            segment_id,
            Arc::clone(&passive),
            flush_id,
            Some(completion_tx),
        )
        .await
        .map_err(|e| e.to_string())?;

    match completion_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("Flush worker dropped completion signal".to_string()),
    }
}

async fn on_wait_for_flush(ctx: &ShardContext) -> Result<(), String> {
    use tokio::time::{Duration, sleep};

    let target = ctx.flush_progress.snapshot();

    if ctx.flush_progress.completed() >= target {
        return Ok(());
    }

    loop {
        if ctx.flush_progress.completed() >= target {
            return Ok(());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

async fn on_shutdown(ctx: &mut ShardContext) -> Result<(), String> {
    info!(target: LOG_TARGET, shard_id = ctx.id, "Initiating shard shutdown");

    // Ensure any passive buffers are cleaned up before exiting
    ctx.passive_buffers.prune_empties().await;

    if let Some(wal) = ctx.wal.as_ref() {
        info!(target: LOG_TARGET, shard_id = ctx.id, "Shutting down WAL");
        wal.shutdown().await;
    }

    Ok(())
}
