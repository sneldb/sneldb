use std::sync::Arc;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{
    BatchSchema, FlowChannel, FlowMetrics, OrderedStreamMerger as FlowOrderedStreamMerger,
};
use tokio::task::JoinHandle;

/// Merges shard flows while respecting a global `ORDER BY` specification.
pub struct OrderedStreamMerger {
    field: String,
    ascending: bool,
    limit: Option<u32>,
    offset: Option<u32>,
}

impl OrderedStreamMerger {
    pub fn new(field: String, ascending: bool, limit: Option<u32>, offset: Option<u32>) -> Self {
        Self {
            field,
            ascending,
            limit,
            offset,
        }
    }

    /// Builds a shared ordered stream by wiring each shard receiver into the
    /// flow-level ordered merger. Returns an error if schemas diverge or the
    /// ordering column is absent.
    pub fn merge(
        &self,
        _ctx: &QueryContext<'_>,
        handles: Vec<ShardFlowHandle>,
    ) -> Result<QueryBatchStream, String> {
        if handles.is_empty() {
            return Err("no shard handles".to_string());
        }

        let capacity = handles.len().max(1) * 2;
        let metrics = FlowMetrics::new();
        let (tx, rx) = FlowChannel::bounded(capacity, Arc::clone(&metrics));

        let mut schema: Option<Arc<BatchSchema>> = None;
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        let mut receivers = Vec::new();

        for handle in handles {
            let (receiver, handle_schema, mut handle_tasks) = handle.into_parts();

            if let Some(existing) = &schema {
                if !existing.is_compatible_with(&handle_schema) {
                    return Err("stream schemas differ across shards".to_string());
                }
            } else {
                schema = Some(Arc::clone(&handle_schema));
            }

            tasks.append(&mut handle_tasks);
            receivers.push(receiver);
        }

        let schema = schema.ok_or_else(|| "no shards produced schema".to_string())?;
        let order_index = schema
            .columns()
            .iter()
            .position(|col| col.name == self.field)
            .ok_or_else(|| format!("order by field '{}' not found in stream schema", self.field))?;

        let limit = self.limit.map(|value| value as usize);
        let offset = self.offset.map(|value| value as usize).unwrap_or(0);

        let merger_handle = FlowOrderedStreamMerger::spawn(
            Arc::clone(&schema),
            receivers,
            order_index,
            self.ascending,
            offset,
            limit,
            tx,
            1024,
        )?;

        tasks.push(merger_handle);

        Ok(QueryBatchStream::new(schema, rx, tasks))
    }
}

/// Performs a simple fan-in merge when no ordering is required for the
/// streaming query.
pub struct UnorderedStreamMerger;

impl UnorderedStreamMerger {
    pub fn new() -> Self {
        Self
    }

    /// Fan-in merge that forwards batches as they arrive without touching
    /// ordering or limits. Drops the sink once all shard senders complete.
    pub fn merge(
        &self,
        _ctx: &QueryContext<'_>,
        handles: Vec<ShardFlowHandle>,
    ) -> Result<QueryBatchStream, String> {
        let capacity = handles.len().max(1) * 2;
        let metrics = FlowMetrics::new();
        let (tx, rx) = FlowChannel::bounded(capacity, metrics);

        let mut schema: Option<Arc<BatchSchema>> = None;
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        for handle in handles {
            let (mut receiver, handle_schema, mut handle_tasks) = handle.into_parts();

            if let Some(existing) = &schema {
                if !existing.is_compatible_with(&handle_schema) {
                    return Err("stream schemas differ across shards".to_string());
                }
            } else {
                schema = Some(Arc::clone(&handle_schema));
            }

            tasks.append(&mut handle_tasks);
            let tx_clone = tx.clone();
            tasks.push(tokio::spawn(async move {
                while let Some(batch) = receiver.recv().await {
                    if tx_clone.send(batch).await.is_err() {
                        break;
                    }
                }
            }));
        }

        drop(tx);

        let schema = schema.ok_or_else(|| "no shards produced schema".to_string())?;
        Ok(QueryBatchStream::new(schema, rx, tasks))
    }
}
