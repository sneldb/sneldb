use super::streaming::{OrderedStreamMerger, UnorderedStreamMerger};
use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;

/// Merger kind for streaming query results, handling both ordered and unordered cases.
pub enum StreamMergerKind {
    Ordered(OrderedStreamMerger),
    Unordered(UnorderedStreamMerger),
}

impl StreamMergerKind {
    /// Creates a StreamMergerKind appropriate for the given context.
    pub fn for_context(ctx: &QueryContext<'_>) -> Self {
        if let crate::command::types::Command::Query {
            order_by: Some(order_by),
            limit,
            offset,
            ..
        } = ctx.command
        {
            StreamMergerKind::Ordered(OrderedStreamMerger::new(
                order_by.field.clone(),
                !order_by.desc,
                *limit,
                *offset,
            ))
        } else {
            StreamMergerKind::Unordered(UnorderedStreamMerger::new())
        }
    }

    /// Merges the shard flow handles into a single query batch stream.
    pub fn merge(
        &self,
        ctx: &QueryContext<'_>,
        handles: Vec<ShardFlowHandle>,
    ) -> Result<QueryBatchStream, String> {
        match self {
            StreamMergerKind::Ordered(merger) => merger.merge(ctx, handles),
            StreamMergerKind::Unordered(merger) => merger.merge(ctx, handles),
        }
    }
}
