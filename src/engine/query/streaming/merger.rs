use std::sync::Arc;

use crate::engine::core::read::flow::operators::MemTableSource;
use crate::engine::core::read::flow::shard_pipeline::{DEFAULT_MEMTABLE_COLUMNS, ShardFlowHandle};
use crate::engine::core::read::flow::{
    BatchReceiver, BatchSchema, FlowChannel, OrderedStreamMerger,
};
use crate::engine::errors::QueryExecutionError;
use tokio::task::JoinHandle;

use super::context::StreamingContext;

/// Merges multiple shard flow handles into a single ordered or unordered stream.
pub struct ShardFlowMerger;

impl ShardFlowMerger {
    pub async fn merge(
        ctx: &StreamingContext,
        handles: Vec<ShardFlowHandle>,
    ) -> Result<ShardFlowHandle, QueryExecutionError> {
        let metrics = ctx.metrics();
        let (merged_tx, merged_rx) = FlowChannel::bounded(ctx.batch_size(), metrics);

        let mut schema_opt: Option<Arc<BatchSchema>> = None;
        let mut receivers: Vec<BatchReceiver> = Vec::new();
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        for handle in handles {
            let (receiver, schema, mut inner_tasks) = handle.into_parts();
            if let Some(existing) = &schema_opt {
                if !existing.is_compatible_with(&schema) {
                    return Err(QueryExecutionError::Aborted);
                }
            } else {
                schema_opt = Some(Arc::clone(&schema));
            }

            tasks.append(&mut inner_tasks);
            receivers.push(receiver);
        }

        let schema = match schema_opt {
            Some(schema) => schema,
            None => Self::infer_schema(ctx).await?,
        };

        let order_spec = ctx.plan().order_by().cloned();

        if let Some(order_spec) = order_spec {
            if receivers.is_empty() {
                drop(merged_tx);
            } else {
                let order_index = schema
                    .columns()
                    .iter()
                    .position(|column| column.name == order_spec.field)
                    .ok_or_else(|| {
                        QueryExecutionError::ExprEval(
                            "order by field missing from shard schema".to_string(),
                        )
                    })?;

                let ascending = !order_spec.desc;
                let merger_handle = OrderedStreamMerger::spawn(
                    Arc::clone(&schema),
                    receivers,
                    order_index,
                    ascending,
                    0,
                    ctx.effective_limit(),
                    merged_tx,
                    ctx.batch_size(),
                )
                .map_err(QueryExecutionError::ExprEval)?;
                tasks.push(merger_handle);
            }
        } else {
            for mut receiver in receivers {
                let tx_clone = merged_tx.clone();
                tasks.push(tokio::spawn(async move {
                    while let Some(batch) = receiver.recv().await {
                        if tx_clone.send(batch).await.is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(merged_tx);
        }

        Ok(ShardFlowHandle::new(merged_rx, schema, tasks))
    }

    async fn infer_schema(ctx: &StreamingContext) -> Result<Arc<BatchSchema>, QueryExecutionError> {
        let required: Vec<String> = DEFAULT_MEMTABLE_COLUMNS
            .iter()
            .map(|column| (*column).to_string())
            .collect();
        let columns = MemTableSource::compute_columns(ctx.plan(), &required)
            .await
            .map_err(|err| QueryExecutionError::ExprEval(err.to_string()))?;
        let schema = BatchSchema::new(columns)
            .map_err(|err| QueryExecutionError::ExprEval(err.to_string()))?;
        Ok(Arc::new(schema))
    }
}
