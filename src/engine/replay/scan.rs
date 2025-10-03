use crate::command::types::Command;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
use crate::engine::core::{Event, MemTable, QueryCaches, QueryExecution, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};

/// Executes a replay-like scan by converting Replay -> Query, planning, and executing.
pub async fn scan(
    command: &Command,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_buffers: &Arc<PassiveBufferSet>,
) -> Result<Vec<Event>, QueryExecutionError> {
    // Step 1: Convert to a Query command
    let query_command = match command.to_query_command() {
        Some(q) => {
            info!(target: "engine::replay::scan", ?q, "Converted Replay command to Query command");
            q
        }
        None => {
            error!(target: "engine::replay::scan", "Failed to convert Replay command to Query command");
            return Err(QueryExecutionError::Aborted);
        }
    };

    // Step 2: Build query plan
    let plan = match QueryPlan::new(query_command, registry, segment_base_dir, segment_ids).await {
        Some(p) => {
            info!(target: "engine::replay::scan", "Query plan successfully created");
            p
        }
        None => {
            error!(target: "engine::replay::scan", "Failed to create Query Plan");
            return Err(QueryExecutionError::Aborted);
        }
    };

    // Step 3: Execute the query
    let passives = passive_buffers.non_empty().await;
    let passive_refs: Vec<&Arc<Mutex<MemTable>>> = passives.iter().collect();

    let abs_dir = if segment_base_dir.is_absolute() {
        segment_base_dir.to_path_buf()
    } else {
        std::fs::canonicalize(&segment_base_dir).unwrap_or(segment_base_dir.to_path_buf())
    };
    let caches = QueryCaches::new_abs(abs_dir);

    let mut execution = QueryExecution::new(&plan)
        .with_memtable(memtable)
        .with_passive_memtables(passive_refs)
        .with_caches(&caches);

    let results = execution.run().await?;
    info!(target: "engine::replay::scan", count = results.len(), "Replay scan completed");

    if tracing::enabled!(tracing::Level::DEBUG) {
        let s = GlobalZoneIndexCache::instance().stats();
        debug!(target: "engine::replay::scan", hits=%s.hits, misses=%s.misses, reloads=%s.reloads, evictions=%s.evictions, "Cache totals");
    }

    Ok(results)
}
