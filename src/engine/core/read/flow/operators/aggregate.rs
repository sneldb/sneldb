use std::sync::Arc;

use crate::engine::core::Event;
use crate::engine::core::QueryPlan;
use crate::engine::core::event::event_id::EventId;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperator, FlowOperatorError};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::read::sink::{AggregateSink, ResultSink};
use serde_json::{Map, Value};

use super::super::{BatchReceiver, BatchSender};

#[derive(Clone)]
pub struct AggregateOpConfig {
    pub plan: Arc<QueryPlan>,
    pub aggregate: AggregatePlan,
}

pub struct AggregateOp {
    config: AggregateOpConfig,
}

impl AggregateOp {
    pub fn new(config: AggregateOpConfig) -> Self {
        Self { config }
    }

    fn output_schema(&self) -> Vec<ColumnSpec> {
        build_aggregate_schema(&self.config.aggregate)
    }
}

pub fn aggregate_output_schema(plan: &AggregatePlan) -> Vec<ColumnSpec> {
    build_aggregate_schema(plan)
}

#[async_trait::async_trait]
impl FlowOperator for AggregateOp {
    async fn run(
        self,
        mut input: BatchReceiver,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        let mut sink = AggregateSink::from_query_plan(&self.config.plan, &self.config.aggregate);
        let mut synthetic_id = 1u64;

        while let Some(batch_arc) = input.recv().await {
            if batch_arc.is_empty() {
                continue;
            }

            let schema = batch_arc.schema();
            let column_names: Vec<String> =
                schema.columns().iter().map(|c| c.name.clone()).collect();
            let mut column_vecs: Vec<Vec<Value>> = Vec::with_capacity(schema.column_count());
            for col_idx in 0..schema.column_count() {
                column_vecs.push(batch_arc.column(col_idx).map_err(|e| {
                    FlowOperatorError::Batch(format!("failed to read column: {}", e))
                })?);
            }
            let column_views: Vec<&[Value]> = column_vecs.iter().map(|v| v.as_slice()).collect();

            for row_idx in 0..batch_arc.len() {
                let event = event_from_row(
                    &column_views,
                    &column_names,
                    row_idx,
                    self.config.plan.event_type(),
                    &mut synthetic_id,
                );
                sink.on_event(&event);
            }
        }

        let aggregated_events = sink.into_events(&self.config.plan);
        let schema_columns = self.output_schema();
        let schema = Arc::new(BatchSchema::new(schema_columns.clone()).map_err(|e| {
            FlowOperatorError::Batch(format!("failed to build aggregate schema: {}", e))
        })?);
        if aggregated_events.is_empty() {
            return Ok(());
        }

        let mut builder = ctx.pool().acquire(Arc::clone(&schema));
        let mut row_values: Vec<Value> = Vec::with_capacity(schema.column_count());

        for event in aggregated_events {
            row_values.clear();
            for column in schema.columns() {
                let mut value = event.get_field(&column.name).unwrap_or(Value::Null);

                if let Some(group_by) = self.config.aggregate.group_by.as_ref() {
                    if group_by.iter().any(|g| g == &column.name) {
                        if let Value::Object(map) = &event.payload {
                            if let Some(v) = map.get(&column.name) {
                                value = v.clone();
                            }
                        }
                    }
                }

                if column.name == "bucket" {
                    if let Value::Object(map) = &event.payload {
                        if let Some(v) = map.get("bucket") {
                            value = v.clone();
                        }
                    }
                }

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
                builder = ctx.pool().acquire(Arc::clone(&schema));
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

fn event_from_row(
    column_views: &[&[Value]],
    column_names: &[String],
    row_idx: usize,
    default_event_type: &str,
    synthetic_id: &mut u64,
) -> Event {
    let mut context_id = String::new();
    let mut event_type = Some(default_event_type.to_string());
    let mut timestamp = 0u64;
    let mut event_id = None;
    let mut payload = Map::new();

    for (idx, name) in column_names.iter().enumerate() {
        let value = column_views[idx]
            .get(row_idx)
            .cloned()
            .unwrap_or(Value::Null);
        match name.as_str() {
            "context_id" => {
                context_id = match &value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => value.to_string(),
                };
            }
            "event_type" => {
                if let Value::String(s) = &value {
                    event_type = Some(s.clone());
                }
            }
            "timestamp" => {
                timestamp = value_as_u64(&value).unwrap_or(0);
            }
            "event_id" => {
                event_id = value_as_u64(&value).map(EventId::from);
            }
            _ => {
                if !matches!(value, Value::Null) {
                    payload.insert(name.clone(), value);
                }
            }
        }
    }

    let event = Event {
        event_type: event_type.unwrap_or_else(|| default_event_type.to_string()),
        context_id,
        timestamp,
        id: event_id.unwrap_or_else(|| {
            let id = EventId::from(*synthetic_id);
            *synthetic_id = synthetic_id.saturating_add(1);
            id
        }),
        payload: Value::Object(payload),
    };

    event
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(num) => {
            if num.is_u64() {
                num.as_u64()
            } else if num.is_i64() {
                num.as_i64().map(|v| v as u64)
            } else {
                num.as_f64().map(|v| v as u64)
            }
        }
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn build_aggregate_schema(plan: &AggregatePlan) -> Vec<ColumnSpec> {
    let mut columns: Vec<ColumnSpec> = Vec::new();
    if plan.time_bucket.is_some() {
        columns.push(ColumnSpec {
            name: "bucket".to_string(),
            logical_type: "Timestamp".to_string(),
        });
    }
    if let Some(group_by) = &plan.group_by {
        for field in group_by {
            columns.push(ColumnSpec {
                name: field.clone(),
                logical_type: "String".to_string(),
            });
        }
    }
    for spec in &plan.ops {
        match spec {
            AggregateOpSpec::CountAll => columns.push(ColumnSpec {
                name: "count".to_string(),
                logical_type: "Integer".to_string(),
            }),
            AggregateOpSpec::CountField { field } => columns.push(ColumnSpec {
                name: format!("count_{}", field),
                logical_type: "Integer".to_string(),
            }),
            AggregateOpSpec::CountUnique { field } => columns.push(ColumnSpec {
                name: format!("count_unique_{}", field),
                logical_type: "Integer".to_string(),
            }),
            AggregateOpSpec::Total { field } => columns.push(ColumnSpec {
                name: format!("total_{}", field),
                logical_type: "Integer".to_string(),
            }),
            AggregateOpSpec::Avg { field } => columns.push(ColumnSpec {
                name: format!("avg_{}", field),
                logical_type: "Float".to_string(),
            }),
            AggregateOpSpec::Min { field } => columns.push(ColumnSpec {
                name: format!("min_{}", field),
                logical_type: "String".to_string(),
            }),
            AggregateOpSpec::Max { field } => columns.push(ColumnSpec {
                name: format!("max_{}", field),
                logical_type: "String".to_string(),
            }),
        }
    }
    columns
}
