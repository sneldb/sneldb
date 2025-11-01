use std::path::Path;
use std::sync::Arc;

use crate::command::types::Command;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::{MemTable, QueryPlan};
use crate::engine::errors::QueryExecutionError;
use crate::engine::schema::registry::SchemaRegistry;
use tokio::sync::RwLock;

use super::builders::FlowBuilders;
use super::context::StreamingContext;
use super::merger::ShardFlowMerger;

const STREAMING_BATCH_SIZE: usize = 32768;

/// Primary orchestrator for streaming scans. Encapsulates the query plan,
/// flow context, and helper routines used to build shard flows.
pub struct StreamingScan<'a> {
    memtable: &'a MemTable,
    context: StreamingContext,
}

impl<'a> StreamingScan<'a> {
    pub async fn new(
        command: &Command,
        registry: &Arc<RwLock<SchemaRegistry>>,
        segment_base_dir: &Path,
        segment_ids: &Arc<std::sync::RwLock<Vec<String>>>,
        memtable: &'a MemTable,
        passive_buffers: &Arc<PassiveBufferSet>,
    ) -> Result<Self, QueryExecutionError> {
        let plan = QueryPlan::new(command.clone(), registry, segment_base_dir, segment_ids)
            .await
            .ok_or(QueryExecutionError::Aborted)?;

        if plan.aggregate_plan.is_some() {
            return Err(QueryExecutionError::Aborted);
        }

        let context =
            StreamingContext::new(Arc::new(plan), passive_buffers, STREAMING_BATCH_SIZE).await?;

        Ok(Self { memtable, context })
    }

    pub async fn execute(&self) -> Result<ShardFlowHandle, QueryExecutionError> {
        let builders = FlowBuilders::new(self.memtable);
        let mut handles = Vec::new();

        if let Some(handle) = builders.memtable_flow(&self.context).await? {
            handles.push(handle);
        }

        if let Some(handle) = builders.segment_flow(&self.context).await? {
            handles.push(handle);
        }

        ShardFlowMerger::merge(&self.context, handles).await
    }
}
