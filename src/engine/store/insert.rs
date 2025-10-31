use crate::engine::core::Event;
use crate::engine::core::MemTable;
use crate::engine::core::WalEntry;
use crate::engine::errors::StoreError;
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::shard::context::ShardContext;
use crate::shared::config::CONFIG;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};

/// Inserts a validated event into the current `MemTable`.
/// If the table is full after insertion, it is swapped and queued for flushing.
///
/// This function owns the full ingest path for STORE commands.
pub async fn insert_and_maybe_flush(
    event: Event,
    ctx: &mut ShardContext,
    schema_registry: &Arc<RwLock<SchemaRegistry>>,
) -> Result<(), StoreError> {
    // 1. Append to WAL BEFORE MemTable
    if CONFIG.wal.enabled {
        if let Some(wal) = &ctx.wal {
            trace!(
                target: "sneldb::store",
                "Appending event to WAL (context_id = {})",
                event.context_id
            );
            wal.append(WalEntry::from_event(&event)).await;
        }
    }

    // 2. Insert into MemTable
    debug!(
        target: "sneldb::store",
        event_type = event.event_type,
        context_id = event.context_id,
        "Inserting event into MemTable"
    );
    ctx.memtable.insert(event)?;

    // 3. If MemTable is full, flush and rotate
    if ctx.memtable.is_full() {
        info!(
            target: "sneldb::store",
            "MemTable is full; flushing and rotating"
        );

        let current_segment_id = ctx.allocator.next_for_level(0) as u64;

        let capacity = ctx.memtable.capacity();

        let passive_arc = ctx.passive_buffers.add_from(&ctx.memtable).await;
        let flushed_mem = std::mem::replace(&mut ctx.memtable, MemTable::new(capacity));

        debug!(
            target: "sneldb::store",
            segment_id = current_segment_id,
            "Queuing passive MemTable for flush"
        );

        ctx.flush_manager
            .queue_for_flush(
                flushed_mem,
                Arc::clone(schema_registry),
                current_segment_id,
                Arc::clone(&passive_arc),
                None,
            )
            .await?;

        // Opportunistic pruning: every max_inflight/2 rotations
        let prune_every = std::cmp::max(1, ctx.passive_buffers.max_inflight() / 2);
        if (ctx.segment_id as usize) % prune_every == 0 {
            ctx.passive_buffers.prune_empties().await;
        }
    }

    Ok(())
}
