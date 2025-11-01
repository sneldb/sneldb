use crate::engine::core::read::flow::shard_pipeline::{ShardFlowHandle, build_segment_stream};
use crate::engine::core::{MemTable, MemTableQueryRunner};
use crate::engine::errors::QueryExecutionError;

use super::context::StreamingContext;

/// Helper responsible for constructing shard-level flows that feed the
/// streaming pipeline.
pub struct FlowBuilders<'a> {
    memtable: &'a MemTable,
}

impl<'a> FlowBuilders<'a> {
    pub fn new(memtable: &'a MemTable) -> Self {
        Self { memtable }
    }

    /// Builds the flow that streams data from the active and passive memtables.
    pub async fn memtable_flow(
        &self,
        ctx: &StreamingContext,
    ) -> Result<Option<ShardFlowHandle>, QueryExecutionError> {
        let passive_refs = ctx.passive_refs();
        let runner = MemTableQueryRunner::new(Some(self.memtable), &passive_refs, ctx.plan());

        runner
            .stream(ctx.flow_context(), ctx.effective_limit())
            .await
            .map_err(|err| QueryExecutionError::ExprEval(err.to_string()))
    }

    /// Builds the flow that streams data from on-disk segments.
    pub async fn segment_flow(
        &self,
        ctx: &StreamingContext,
    ) -> Result<Option<ShardFlowHandle>, QueryExecutionError> {
        build_segment_stream(
            ctx.plan_arc(),
            ctx.flow_context(),
            ctx.caches(),
            ctx.effective_limit(),
        )
        .await
        .map_err(|err| QueryExecutionError::ExprEval(err.to_string()))
    }
}
