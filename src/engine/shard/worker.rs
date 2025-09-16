use crate::engine::core::snapshot::snapshot_reader::SnapshotReader;
use crate::engine::core::snapshot::snapshot_registry::{
    ReplayStrategy, SnapshotKey, SnapshotRegistry,
};
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

    let mut counter = 0;
    const SLEEP_EVERY: usize = 100;

    while let Some(msg) = rx.recv().await {
        match msg {
            ShardMessage::Store(event, registry) => {
                counter += 1;
                if counter % SLEEP_EVERY == 0 {
                    tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
                }

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
    tx: tokio::sync::mpsc::Sender<Vec<crate::engine::core::Event>>,
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
    debug!(target: LOG_TARGET, shard_id = ctx.id, results_count = results.len(), "Query completed");
    tx.send(results).await.map_err(|e| e.to_string())
}

async fn on_replay(
    command: crate::command::types::Command,
    tx: tokio::sync::mpsc::Sender<Vec<crate::engine::core::Event>>,
    ctx: &ShardContext,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<(), String> {
    // Determine snapshot strategy
    use crate::command::types::Command;
    let (event_type_opt, context_id, since_opt) = match &command {
        Command::Replay {
            event_type,
            context_id,
            since,
            ..
        } => (event_type.clone(), context_id.clone(), since.clone()),
        _ => unreachable!(),
    };

    // Resolve UID; if not resolvable, we will ignore snapshots entirely.
    let maybe_key = {
        let et = event_type_opt.clone().unwrap_or_else(|| "*".to_string());
        let maybe_uid = registry.read().await.get_uid(&et);
        maybe_uid.map(|uid| SnapshotKey {
            uid,
            context_id: context_id.clone(),
        })
    };

    // Parse since -> u64 if provided
    let since_ts_opt = parse_since_to_ts(&since_opt);

    // If no SINCE provided, per policy ignore snapshot and rebuild full replay
    let strategy = if let (Some(key), Some(since_ts)) = (maybe_key.as_ref(), since_ts_opt) {
        let mut registry = SnapshotRegistry::new(ctx.base_dir.join("snapshots"));
        let _ = registry.load();
        registry.decide_replay_strategy(key, since_ts)
    } else {
        ReplayStrategy::IgnoreSnapshot
    };

    // Execute according to strategy
    let mut combined: Vec<crate::engine::core::Event> = Vec::new();

    match strategy {
        ReplayStrategy::IgnoreSnapshot => {
            let mut results = replay_scan(
                &command,
                registry,
                &ctx.base_dir,
                &ctx.segment_ids,
                &ctx.memtable,
                &ctx.passive_buffers,
            )
            .await
            .map_err(|e| e.to_string())?;

            if let Some(since_ts) = since_ts_opt {
                results.retain(|e| e.timestamp >= since_ts);
            }
            combined = results;
        }
        ReplayStrategy::UseSnapshot {
            start_ts: _,
            end_ts,
            path,
        } => {
            // 1) Load snapshot and filter by optional constraints
            let mut snap_events = SnapshotReader::new(&path)
                .read_all()
                .map_err(|e| e.to_string())?;
            if let Some(et) = &event_type_opt {
                snap_events.retain(|e| &e.event_type == et);
            }
            if let Some(since_ts) = since_ts_opt {
                snap_events.retain(|e| e.timestamp >= since_ts);
            }

            // 2) Fetch deltas from raw storage and filter ts >= max(end_ts+1, since_ts)
            let mut delta_events = replay_scan(
                &command,
                registry,
                &ctx.base_dir,
                &ctx.segment_ids,
                &ctx.memtable,
                &ctx.passive_buffers,
            )
            .await
            .map_err(|e| e.to_string())?;
            let delta_start = since_ts_opt
                .map(|s| s.max(end_ts.saturating_add(1)))
                .unwrap_or_else(|| end_ts.saturating_add(1));
            delta_events.retain(|e| e.timestamp >= delta_start);

            combined.reserve(snap_events.len() + delta_events.len());
            combined.extend(snap_events);
            combined.extend(delta_events);
        }
    }

    // Ensure append order by timestamp
    combined.sort_by_key(|e| e.timestamp);

    debug!(target: LOG_TARGET, shard_id = ctx.id, results_count = combined.len(), "Replay completed");
    tx.send(combined).await.map_err(|e| e.to_string())
}

fn parse_since_to_ts(since: &Option<String>) -> Option<u64> {
    let raw = since.as_ref()?;
    if let Ok(n) = raw.parse::<u64>() {
        return Some(n);
    }
    // Try RFC3339
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        let ts_ms = dt.timestamp_millis();
        if ts_ms >= 0 {
            return Some(ts_ms as u64);
        }
    }
    None
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
