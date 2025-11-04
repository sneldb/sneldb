use crate::command::types::Command;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::result::QueryResult;
use crate::engine::core::{MemTable, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::query::execution_engine::{ExecutionEngine, ScanContext};
use crate::engine::query::streaming::StreamingScan;
use crate::engine::schema::registry::SchemaRegistry;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Executes a query by scanning memtables and on-disk segments.
pub async fn scan(
    command: &Command,
    metadata: Option<std::collections::HashMap<String, String>>,
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
    let mut plan =
        match QueryPlan::new(command.clone(), registry, segment_base_dir, segment_ids).await {
            Some(p) => {
                info!(target: "engine::query::scan", "Query plan successfully created");
                p
            }
            None => {
                error!(target: "engine::query::scan", "Failed to create query plan");
                return Err(QueryExecutionError::Aborted);
            }
        };

    // Apply metadata if provided
    if let Some(meta) = metadata {
        for (k, v) in meta {
            plan.set_metadata(k, v);
        }
    }

    // Step 2: Build context and delegate to ExecutionEngine

    let ctx = ScanContext::new(
        registry,
        segment_base_dir.to_path_buf(),
        segment_ids,
        memtable,
        passive_buffers,
    );
    ExecutionEngine::execute(&plan, &ctx).await
}

/// Entry point used by shard workers to start a streaming scan and return the
/// resulting flow handle back to the coordinator.
pub async fn scan_streaming(
    command: &Command,
    metadata: Option<std::collections::HashMap<String, String>>,
    registry: &Arc<RwLock<SchemaRegistry>>,
    segment_base_dir: &Path,
    segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
    memtable: &MemTable,
    passive_buffers: &Arc<crate::engine::core::memory::passive_buffer_set::PassiveBufferSet>,
) -> Result<ShardFlowHandle, QueryExecutionError> {
    let scan = StreamingScan::new(
        command,
        metadata,
        registry,
        segment_base_dir,
        segment_ids,
        memtable,
        passive_buffers,
    )
    .await?;
    scan.execute().await
}
