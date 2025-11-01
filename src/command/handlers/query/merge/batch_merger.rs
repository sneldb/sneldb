use crate::command::handlers::query::context::QueryContext;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;

use super::ordered::OrderedMerger;
use super::unordered::UnorderedMerger;

/// Merger for batch query results, handling both ordered and unordered cases.
pub enum BatchMerger {
    Ordered(OrderedMerger),
    Unordered(UnorderedMerger),
}

impl BatchMerger {
    /// Creates a BatchMerger appropriate for the given command.
    pub fn for_command(command: &Command) -> Self {
        if let Command::Query {
            order_by: Some(order_by),
            limit,
            offset,
            ..
        } = command
        {
            BatchMerger::Ordered(OrderedMerger::new(
                order_by.field.clone(),
                !order_by.desc,
                *limit,
                *offset,
            ))
        } else {
            let (limit, offset) = match command {
                Command::Query { limit, offset, .. } => (*limit, *offset),
                _ => (None, None),
            };
            BatchMerger::Unordered(UnorderedMerger::new(limit, offset))
        }
    }

    /// Merges the shard results into a single query result.
    pub fn merge(
        &self,
        ctx: &QueryContext<'_>,
        results: Vec<QueryResult>,
    ) -> Result<QueryResult, String> {
        match self {
            BatchMerger::Ordered(merger) => merger.merge(ctx, results),
            BatchMerger::Unordered(merger) => merger.merge(ctx, results),
        }
    }
}
