use std::sync::Arc;

use crate::engine::core::read::flow::{FlowContext, FlowOperator, FlowOperatorError};
use crate::engine::types::ScalarValue;

use super::super::{BatchReceiver, BatchSender};

pub type FilterPredicate = Arc<dyn Fn(&[&ScalarValue]) -> bool + Send + Sync>;

pub struct FilterOp {
    predicate: FilterPredicate,
}

impl FilterOp {
    pub fn new(predicate: FilterPredicate) -> Self {
        Self { predicate }
    }
}

#[async_trait::async_trait]
impl FlowOperator for FilterOp {
    async fn run(
        self,
        mut input: BatchReceiver,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        while let Some(batch_arc) = input.recv().await {
            if batch_arc.is_empty() {
                continue;
            }
            let schema = Arc::new(batch_arc.schema().clone());
            let mut builder = ctx.pool().acquire(Arc::clone(&schema));
            let column_count = schema.column_count();

            let mut row_values: Vec<ScalarValue> = Vec::with_capacity(column_count);

            let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::with_capacity(column_count);
            for col_idx in 0..column_count {
                column_vecs.push(batch_arc.column(col_idx).map_err(|e| {
                    FlowOperatorError::Batch(format!("failed to read column: {}", e))
                })?);
            }
            let column_views: Vec<&[ScalarValue]> =
                column_vecs.iter().map(|v| v.as_slice()).collect();

            for row_idx in 0..batch_arc.len() {
                row_values.clear();
                for col in &column_views {
                    row_values.push(col.get(row_idx).cloned().unwrap_or(ScalarValue::Null));
                }

                let row_refs: Vec<&ScalarValue> = row_values.iter().collect();

                if (self.predicate)(&row_refs) {
                    builder
                        .push_row(&row_values)
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

                    if builder.is_full() {
                        let batch = builder
                            .finish()
                            .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                        output
                            .send(Arc::new(batch))
                            .await
                            .map_err(|_| FlowOperatorError::ChannelClosed)?;
                        builder = ctx.pool().acquire(Arc::clone(&schema));
                    }
                }
            }

            if builder.len() > 0 {
                let batch = builder
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                output
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
            }
        }

        Ok(())
    }
}
