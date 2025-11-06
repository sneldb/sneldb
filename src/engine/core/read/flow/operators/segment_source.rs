use std::sync::Arc;

use crate::engine::core::Event;
use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperatorError, FlowSource};
use crate::engine::types::ScalarValue;

use super::super::BatchSender;

pub struct SegmentSourceConfig {
    pub events: Vec<Event>,
    pub schema: Arc<BatchSchema>,
}

pub struct SegmentSource {
    config: SegmentSourceConfig,
}

impl SegmentSource {
    pub fn new(config: SegmentSourceConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl FlowSource for SegmentSource {
    async fn run(
        mut self,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        if self.config.events.is_empty() {
            return Ok(());
        }

        let mut builder = ctx.pool().acquire(Arc::clone(&self.config.schema));
        let mut row_values: Vec<ScalarValue> =
            Vec::with_capacity(self.config.schema.column_count());

        for event in self.config.events.into_iter() {
            row_values.clear();
            for column in self.config.schema.columns() {
                let value = event
                    .get_field_scalar(&column.name)
                    .unwrap_or(ScalarValue::Null);
                row_values.push(value);
            }

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
                builder = ctx.pool().acquire(Arc::clone(&self.config.schema));
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

        Ok(())
    }
}
