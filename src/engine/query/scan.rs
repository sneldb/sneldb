use crate::command::types::Command;
use crate::engine::core::{Event, MemTable, QueryExecution, QueryPlan};
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::errors::QueryExecutionError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};

/// Executes a query by scanning memtables and on-disk segments.
pub async fn scan(
    command: &Command,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_buffers: &Arc<PassiveBufferSet>,
) -> Result<Vec<Event>, QueryExecutionError> {
    debug!(
        target: "engine::query::scan",
        ?segment_base_dir,
        "Starting query scan for event_type={:?} context_id={:?}",
        command.event_type(),
        command.context_id()
    );

    // Step 1: Build query plan
    let plan = match QueryPlan::new(command.clone(), registry, segment_base_dir, segment_ids).await
    {
        Some(p) => {
            info!(target: "engine::query::scan", "Query plan successfully created");
            p
        }
        None => {
            error!(target: "engine::query::scan", "Failed to create query plan");
            return Err(QueryExecutionError::Aborted);
        }
    };

    // Step 2: Execute query over memtable and disk
    let passives = passive_buffers.non_empty().await;
    let passive_refs: Vec<&Arc<Mutex<MemTable>>> = passives.iter().collect();

    let mut execution = QueryExecution::new(&plan)
        .with_memtable(memtable)
        .with_passive_memtables(passive_refs);

    let results = execution.run().await?;
    info!(
        target: "engine::query::scan",
        count = results.len(),
        "Query execution completed"
    );

    Ok(results)
}
