use std::sync::Arc;

use crate::engine::core::Event;
use crate::engine::core::QueryPlan;
use crate::engine::core::event::event_id::EventId;
use crate::engine::core::read::aggregate::partial::{AggPartial, AggState};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::{BatchSchema, FlowContext, FlowOperator, FlowOperatorError};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::read::sink::{AggregateSink, ResultSink};
use crate::engine::types::ScalarValue;
use serde_json;
use std::collections::BTreeMap;

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
            let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::with_capacity(schema.column_count());
            for col_idx in 0..schema.column_count() {
                column_vecs.push(batch_arc.column(col_idx).map_err(|e| {
                    FlowOperatorError::Batch(format!("failed to read column: {}", e))
                })?);
            }
            let column_views: Vec<&[ScalarValue]> =
                column_vecs.iter().map(|v| v.as_slice()).collect();

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

        // Use into_partial() instead of into_events() to preserve sum/count for AVG merging
        let partial = sink.into_partial();
        let schema_columns = self.output_schema();
        let schema = Arc::new(BatchSchema::new(schema_columns.clone()).map_err(|e| {
            FlowOperatorError::Batch(format!("failed to build aggregate schema: {}", e))
        })?);

        if partial.groups.is_empty() {
            return Ok(());
        }

        partial_to_batches(partial, schema, ctx, output).await
    }
}

/// Converts AggPartial to batches, preserving sum/count for AVG aggregations
async fn partial_to_batches(
    partial: AggPartial,
    schema: Arc<BatchSchema>,
    ctx: Arc<FlowContext>,
    output: BatchSender,
) -> Result<(), FlowOperatorError> {
    let mut builder = ctx.pool().acquire(Arc::clone(&schema));

    for (group_key, states) in partial.groups {
        let mut row = Vec::with_capacity(schema.column_count());

        // Add bucket if present
        if partial.time_bucket.is_some() {
            row.push(ScalarValue::Int64(
                group_key.bucket.map(|b| b as i64).unwrap_or(0),
            ));
        }

        // Add group_by values
        if let Some(_group_by_fields) = &partial.group_by {
            for val in &group_key.groups {
                row.push(ScalarValue::Utf8(val.clone()));
            }
        }

        // Add metric values
        for (spec, state) in partial.specs.iter().zip(states.iter()) {
            match (spec, state) {
                (AggregateOpSpec::CountAll, AggState::CountAll { count }) => {
                    row.push(ScalarValue::Int64(*count));
                }
                (AggregateOpSpec::CountField { .. }, AggState::CountAll { count }) => {
                    row.push(ScalarValue::Int64(*count));
                }
                (AggregateOpSpec::CountUnique { .. }, AggState::CountUnique { values }) => {
                    // Serialize HashSet as JSON array string for accurate merging across shards
                    // This preserves the actual values, not just the count
                    let json_values: Vec<&String> = values.iter().collect();
                    let json_str = serde_json::to_string(&json_values).map_err(|e| {
                        FlowOperatorError::Batch(format!(
                            "failed to serialize CountUnique values: {}",
                            e
                        ))
                    })?;
                    row.push(ScalarValue::Utf8(json_str));
                }
                (AggregateOpSpec::Total { .. }, AggState::Sum { sum }) => {
                    row.push(ScalarValue::Int64(*sum));
                }
                (AggregateOpSpec::Avg { .. }, AggState::Avg { sum, count }) => {
                    // Output sum and count separately for accurate merging
                    row.push(ScalarValue::Int64(*sum));
                    row.push(ScalarValue::Int64(*count));
                }
                (AggregateOpSpec::Min { .. }, AggState::Min { min_num, min_str }) => {
                    if let Some(n) = min_num {
                        row.push(ScalarValue::Int64(*n));
                    } else if let Some(s) = min_str {
                        row.push(ScalarValue::Utf8(s.clone()));
                    } else {
                        row.push(ScalarValue::Utf8(String::new()));
                    }
                }
                (AggregateOpSpec::Max { .. }, AggState::Max { max_num, max_str }) => {
                    if let Some(n) = max_num {
                        row.push(ScalarValue::Int64(*n));
                    } else if let Some(s) = max_str {
                        row.push(ScalarValue::Utf8(s.clone()));
                    } else {
                        row.push(ScalarValue::Utf8(String::new()));
                    }
                }
                _ => {
                    return Err(FlowOperatorError::Batch(format!(
                        "unsupported aggregate spec/state combination: {:?}/{:?}",
                        spec, state
                    )));
                }
            }
        }

        builder
            .push_row(&row)
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

fn event_from_row(
    column_views: &[&[ScalarValue]],
    column_names: &[String],
    row_idx: usize,
    default_event_type: &str,
    synthetic_id: &mut u64,
) -> Event {
    let mut context_id = String::new();
    let mut event_type = Some(default_event_type.to_string());
    let mut timestamp = 0u64;
    let mut event_id = None;
    let mut payload = BTreeMap::new();

    for (idx, name) in column_names.iter().enumerate() {
        let value = column_views[idx]
            .get(row_idx)
            .cloned()
            .unwrap_or(ScalarValue::Null);
        match name.as_str() {
            "context_id" => {
                context_id = match &value {
                    ScalarValue::Utf8(s) => s.clone(),
                    ScalarValue::Int64(i) => i.to_string(),
                    ScalarValue::Boolean(b) => b.to_string(),
                    _ => value.to_string_repr(),
                };
            }
            "event_type" => {
                if let ScalarValue::Utf8(s) = &value {
                    event_type = Some(s.clone());
                }
            }
            "timestamp" => {
                timestamp = scalar_as_u64(&value).unwrap_or(0);
            }
            "event_id" => {
                event_id = scalar_as_u64(&value).map(EventId::from);
            }
            _ => {
                if !matches!(value, ScalarValue::Null) {
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
        payload,
    };

    event
}

fn scalar_as_u64(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::Int64(i) => {
            if *i >= 0 {
                Some(*i as u64)
            } else {
                None
            }
        }
        ScalarValue::Timestamp(t) => {
            if *t >= 0 {
                Some(*t as u64)
            } else {
                None
            }
        }
        ScalarValue::Utf8(s) => {
            // Try parsing as u64, then i64, then f64
            if let Ok(u) = s.parse::<u64>() {
                Some(u)
            } else if let Ok(i) = s.parse::<i64>() {
                if i >= 0 { Some(i as u64) } else { None }
            } else if let Ok(f) = s.parse::<f64>() {
                if f >= 0.0 { Some(f as u64) } else { None }
            } else {
                None
            }
        }
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
            AggregateOpSpec::CountUnique { field } => {
                // For streaming mode, output JSON array string for accurate merging
                // This preserves the actual unique values, not just the count
                columns.push(ColumnSpec {
                    name: format!("count_unique_{}_values", field),
                    logical_type: "String".to_string(),
                });
            }
            AggregateOpSpec::Total { field } => columns.push(ColumnSpec {
                name: format!("total_{}", field),
                logical_type: "Integer".to_string(),
            }),
            AggregateOpSpec::Avg { field } => {
                // For streaming mode, output sum and count separately for accurate merging
                columns.push(ColumnSpec {
                    name: format!("avg_{}_sum", field),
                    logical_type: "Integer".to_string(),
                });
                columns.push(ColumnSpec {
                    name: format!("avg_{}_count", field),
                    logical_type: "Integer".to_string(),
                });
            }
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
