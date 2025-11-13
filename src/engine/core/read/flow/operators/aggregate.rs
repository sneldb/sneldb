use std::sync::Arc;

use super::super::{BatchReceiver, BatchSender};
use crate::engine::core::QueryPlan;
use crate::engine::core::read::aggregate::plan::AggregatePlan;
use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperator, FlowOperatorError};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::read::sink::AggregateSink;

use crate::engine::core::read::flow::operators::agg::{
    ColumnConverter, PartialConverter, SchemaBuilder,
};
#[derive(Clone)]
pub struct AggregateOpConfig {
    pub plan: Arc<QueryPlan>,
    pub aggregate: AggregatePlan,
}

pub struct AggregateOp {
    config: AggregateOpConfig,
    cached_output_schema: Option<Arc<BatchSchema>>,
}

impl AggregateOp {
    pub fn new(config: AggregateOpConfig) -> Self {
        Self {
            config,
            cached_output_schema: None,
        }
    }

    fn get_output_schema(&mut self) -> Result<Arc<BatchSchema>, FlowOperatorError> {
        if let Some(ref schema) = self.cached_output_schema {
            return Ok(Arc::clone(schema));
        }

        let schema_columns = SchemaBuilder::build(&self.config.aggregate);
        let schema = Arc::new(BatchSchema::new(schema_columns).map_err(|e| {
            FlowOperatorError::Batch(format!("failed to build aggregate schema: {}", e))
        })?);
        self.cached_output_schema = Some(Arc::clone(&schema));
        Ok(schema)
    }
}

pub fn aggregate_output_schema(plan: &AggregatePlan) -> Vec<ColumnSpec> {
    SchemaBuilder::build(plan)
}

#[async_trait::async_trait]
impl FlowOperator for AggregateOp {
    async fn run(
        mut self,
        mut input: BatchReceiver,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        let mut sink = AggregateSink::from_query_plan(&self.config.plan, &self.config.aggregate);

        // Cache needed columns once - they don't change per batch
        let needed_columns = ColumnConverter::determine_needed_columns(&sink);

        while let Some(batch_arc) = input.recv().await {
            if batch_arc.is_empty() {
                continue;
            }

            let schema = batch_arc.schema();
            let column_names: Vec<String> =
                schema.columns().iter().map(|c| c.name.clone()).collect();

            sink.initialize_column_indices(&column_names);

            let columns_map = ColumnConverter::convert(&batch_arc, &column_names, &needed_columns)?;
            let row_count = batch_arc.len();

            sink.on_column_slice(0, row_count, &columns_map);
        }

        let partial = sink.into_partial();
        let schema = self.get_output_schema()?;

        if partial.groups.is_empty() {
            return Ok(());
        }

        PartialConverter::to_batches(partial, schema, ctx, output).await
    }
}
