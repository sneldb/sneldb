use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::core::{MemTable, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::query::execution_engine::{ExecutionEngine, ScanContext};
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Executes a query by scanning memtables and on-disk segments.
pub async fn scan(
    command: &Command,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_buffers: &Arc<crate::engine::core::memory::passive_buffer_set::PassiveBufferSet>,
) -> Result<QueryResult, QueryExecutionError> {
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

    // Step 2: Build context and delegate to ExecutionEngine
    let abs_dir = if segment_base_dir.is_absolute() {
        segment_base_dir.to_path_buf()
    } else {
        std::fs::canonicalize(&segment_base_dir).unwrap_or(segment_base_dir.to_path_buf())
    };
    let ctx = ScanContext::new(registry, abs_dir, segment_ids, memtable, passive_buffers);
    ExecutionEngine::execute(&plan, &ctx).await
}
