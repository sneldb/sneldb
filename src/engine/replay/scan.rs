use crate::command::types::Command;
use crate::engine::core::{Event, MemTable, QueryExecution, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};

/// Executes a replay-like scan by converting Replay -> Query, planning, and executing.
pub async fn scan(
    command: &Command,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_memtable: &Arc<Mutex<MemTable>>,
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
    let mut execution = QueryExecution::new(&plan)
        .with_memtable(memtable)
        .with_passive_memtable(passive_memtable);

    let results = execution.run().await?;
    info!(target: "engine::replay::scan", count = results.len(), "Replay scan completed");
    Ok(results)
}
