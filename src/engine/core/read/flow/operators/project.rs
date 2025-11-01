use std::sync::Arc;

use serde_json::Value;

use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperator, FlowOperatorError};

use super::super::{BatchReceiver, BatchSender};

#[derive(Clone)]
pub struct Projection {
    pub indices: Vec<usize>,
    pub schema: Arc<BatchSchema>,
}

impl Projection {
    /// Check if this is an identity projection (all columns in same order)
    /// Returns true if indices == [0, 1, 2, ..., n-1] where n = column_count
    pub fn is_identity(&self) -> bool {
        let expected_count = self.schema.column_count();
        if self.indices.len() != expected_count {
            return false;
        }
        // Check if indices are 0, 1, 2, ..., n-1
        self.indices.iter().enumerate().all(|(i, &idx)| idx == i)
    }
}

pub struct ProjectOp {
    projection: Projection,
}

impl ProjectOp {
    pub fn new(projection: Projection) -> Self {
        Self { projection }
    }
}

#[async_trait::async_trait]
impl FlowOperator for ProjectOp {
    async fn run(
        self,
        mut input: BatchReceiver,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        let target_schema = Arc::clone(&self.projection.schema);

        while let Some(batch) = input.recv().await {
            if batch.is_empty() {
                continue;
            }

            let mut builder = ctx.pool().acquire(Arc::clone(&target_schema));
            let mut row_values: Vec<Value> = Vec::with_capacity(self.projection.indices.len());

            let mut column_views: Vec<&[Value]> = Vec::with_capacity(self.projection.indices.len());
            for index in &self.projection.indices {
                column_views.push(batch.column(*index).map_err(|e| {
                    FlowOperatorError::Batch(format!("failed to read column {}: {}", index, e))
                })?);
            }

            // Optimize: avoid Option overhead - row_idx is always valid since we iterate 0..batch.len()
            // Pre-allocate row_values once and reuse, clearing between rows
            for row_idx in 0..batch.len() {
                row_values.clear();
                // Direct indexing is safe and faster than .get().unwrap_or()
                for col in &column_views {
                    row_values.push(col[row_idx].clone());
                }

                builder
                    .push_row(&row_values)
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

                if builder.is_full() {
                    let batch = builder
                        .finish()
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                    output
                        .send(batch)
                        .await
                        .map_err(|_| FlowOperatorError::ChannelClosed)?;
                    builder = ctx.pool().acquire(Arc::clone(&target_schema));
                }
            }

            drop(batch);

            if builder.len() > 0 {
                let batch = builder
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                output
                    .send(batch)
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
            }
        }

        Ok(())
    }
}
