use super::aggregate_stream::AggregateStreamMerger;
use super::streaming::{OrderedStreamMerger, UnorderedStreamMerger};
use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;

/// Merger kind for streaming query results, handling ordered, unordered, and aggregate cases.
pub enum StreamMergerKind {
    Ordered(OrderedStreamMerger),
    Unordered(UnorderedStreamMerger),
    Aggregate(AggregateStreamMerger),
}

impl StreamMergerKind {
    /// Creates a StreamMergerKind appropriate for the given context.
    pub fn for_context(ctx: &QueryContext<'_>) -> Self {
        // Check for aggregate queries first (aggregates now support ORDER BY in streaming)
        if let crate::command::types::Command::Query { aggs: Some(_), .. } = ctx.command {
            return StreamMergerKind::Aggregate(AggregateStreamMerger::new(ctx.command));
        }

        // Check for ORDER BY
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
            StreamMergerKind::Aggregate(merger) => merger.merge(ctx, handles),
        }
    }
}
