use std::cmp::Ordering;
use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, info};

use crate::engine::core::MemTable;
use crate::engine::core::read::flow::{
    BatchSchema, ColumnBatchBuilder, FlowContext, FlowOperatorError, FlowSource,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::{ConditionEvaluatorBuilder, QueryContext, QueryPlan};

use super::super::BatchSender;

#[derive(Clone)]
pub struct MemTableSourceConfig {
    pub plan: Arc<QueryPlan>,
    pub memtable: Option<Arc<MemTable>>,
    pub passive_memtables: Vec<Arc<tokio::sync::Mutex<MemTable>>>,
    pub limit_override: Option<usize>,
    pub mandatory_columns: Vec<String>,
}

pub struct MemTableSource {
    config: MemTableSourceConfig,
}

impl MemTableSource {
    pub fn new(config: MemTableSourceConfig) -> Self {
        Self { config }
    }

    pub async fn compute_columns(
        plan: &QueryPlan,
        mandatory_columns: &[String],
    ) -> Result<Vec<ColumnSpec>, FlowOperatorError> {
        let mut names = plan.columns_to_load().await;
        for required in mandatory_columns {
            if !names.iter().any(|n| n == required) {
                names.push(required.clone());
            }
        }

        let registry = plan.registry.read().await;
        let maybe_schema = registry.get(plan.event_type());

        Ok(names
            .into_iter()
            .map(|name| ColumnSpec {
                logical_type: maybe_schema
                    .and_then(|schema| schema.field_type(&name))
                    .map(field_type_to_logical)
                    .unwrap_or_else(|| logical_type_for_builtin(&name)),
                name,
            })
            .collect())
    }

    fn determine_limit(&self, ctx: &QueryContext) -> Option<usize> {
        if ctx.should_defer_limit() {
            None
        } else {
            self.config
                .limit_override
                .or_else(|| self.config.plan.limit())
        }
    }

    async fn resolve_columns(&self) -> Result<Vec<ColumnSpec>, FlowOperatorError> {
        Self::compute_columns(&self.config.plan, &self.config.mandatory_columns).await
    }

    fn new_builder(&self, ctx: &FlowContext, schema: Arc<BatchSchema>) -> ColumnBatchBuilder {
        ctx.pool().acquire(schema)
    }

    async fn push_rows_from_memtable(
        &self,
        memtable: &MemTable,
        limit: Option<usize>,
        columns: &[ColumnSpec],
        evaluator: &crate::engine::core::ConditionEvaluator,
        builder: &mut Option<ColumnBatchBuilder>,
        schema: &Arc<BatchSchema>,
        output: &BatchSender,
        ctx: &FlowContext,
        row_buf: &mut Vec<Value>,
        mut emitted: usize,
    ) -> Result<usize, FlowOperatorError> {
        debug!(
            target: "sneldb::memtable_source",
            rows = memtable.len(),
            "Scanning memtable"
        );

        for event in memtable.iter() {
            if let Some(lim) = limit {
                if emitted >= lim {
                    break;
                }
            }

            if !evaluator.evaluate_event(event) {
                continue;
            }

            row_buf.clear();
            for column in columns {
                let value = event.get_field(&column.name).unwrap_or(Value::Null);
                row_buf.push(value);
            }
            if builder.is_none() {
                *builder = Some(self.new_builder(ctx, Arc::clone(schema)));
            }

            let current_builder = builder.as_mut().expect("builder present");
            current_builder
                .push_row(row_buf)
                .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
            emitted += 1;

            if current_builder.is_full() {
                let finished = builder.take().unwrap();
                let batch = finished
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                output
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
                *builder = Some(self.new_builder(ctx, Arc::clone(schema)));
            }
        }

        Ok(emitted)
    }

    async fn collect_rows_from_memtable(
        &self,
        memtable: &MemTable,
        limit: Option<usize>,
        columns: &[ColumnSpec],
        evaluator: &crate::engine::core::ConditionEvaluator,
        rows: &mut Vec<Vec<Value>>,
        emitted: &mut usize,
    ) -> Result<(), FlowOperatorError> {
        for event in memtable.iter() {
            if let Some(lim) = limit {
                if *emitted >= lim {
                    break;
                }
            }

            if !evaluator.evaluate_event(event) {
                continue;
            }

            let mut row = Vec::with_capacity(columns.len());
            for column in columns {
                let value = event.get_field(&column.name).unwrap_or(Value::Null);
                row.push(value);
            }
            rows.push(row);
            *emitted += 1;

            if let Some(lim) = limit {
                if *emitted >= lim {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowSource for MemTableSource {
    async fn run(
        self,
        output: BatchSender,
        ctx: Arc<FlowContext>,
    ) -> Result<(), FlowOperatorError> {
        let columns = self.resolve_columns().await?;
        let schema = Arc::new(
            BatchSchema::new(columns.clone())
                .map_err(|e| FlowOperatorError::Batch(format!("failed to build schema: {}", e)))?,
        );

        let evaluator = ConditionEvaluatorBuilder::build_from_plan(&self.config.plan);
        let query_ctx = QueryContext::from_command(&self.config.plan.command);
        let limit = self.determine_limit(&query_ctx);

        if let Some(order_spec) = self.config.plan.order_by() {
            let order_index = schema
                .columns()
                .iter()
                .position(|col| col.name == order_spec.field)
                .ok_or_else(|| {
                    FlowOperatorError::Operator(format!(
                        "order column '{}' not found in stream schema",
                        order_spec.field
                    ))
                })?;
            let ascending = !order_spec.desc;

            let mut rows: Vec<Vec<Value>> = Vec::new();
            let mut emitted = 0usize;

            if let Some(active) = self.config.memtable.clone() {
                self.collect_rows_from_memtable(
                    &active,
                    limit,
                    &columns,
                    &evaluator,
                    &mut rows,
                    &mut emitted,
                )
                .await?;
            }

            for passive in self.config.passive_memtables.iter() {
                if let Some(lim) = limit {
                    if emitted >= lim {
                        break;
                    }
                }

                let guard = passive.lock().await;
                self.collect_rows_from_memtable(
                    &*guard,
                    limit,
                    &columns,
                    &evaluator,
                    &mut rows,
                    &mut emitted,
                )
                .await?;
            }

            // Use sort_unstable_by for better performance - maintains relative order of equal elements
            rows.sort_unstable_by(|a, b| {
                let ord = compare_json_values(&a[order_index], &b[order_index]);
                if ascending { ord } else { ord.reverse() }
            });

            if let Some(lim) = limit {
                if rows.len() > lim {
                    rows.truncate(lim);
                }
            }

            let mut builder = Some(self.new_builder(&ctx, Arc::clone(&schema)));
            for row in rows.iter() {
                let current_builder = builder.as_mut().expect("builder present");
                current_builder
                    .push_row(row)
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;

                if current_builder.is_full() {
                    let finished = builder.take().unwrap();
                    let batch = finished
                        .finish()
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                    output
                        .send(Arc::new(batch))
                        .await
                        .map_err(|_| FlowOperatorError::ChannelClosed)?;
                    builder = Some(self.new_builder(&ctx, Arc::clone(&schema)));
                }
            }

            if let Some(remaining) = builder.take() {
                if remaining.len() > 0 {
                    let batch = remaining
                        .finish()
                        .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                    output
                        .send(Arc::new(batch))
                        .await
                        .map_err(|_| FlowOperatorError::ChannelClosed)?;
                }
            }

            info!(
                target: "sneldb::memtable_source",
                rows = rows.len(),
                limit = limit,
                order_index = order_index,
                ascending = ascending,
                "MemTable source completed (ordered)"
            );
            return Ok(());
        }

        let mut builder: Option<ColumnBatchBuilder> = None;
        let mut emitted = 0usize;
        let mut row_buf: Vec<Value> = Vec::with_capacity(columns.len());

        if let Some(active) = self.config.memtable.clone() {
            emitted = self
                .push_rows_from_memtable(
                    &active,
                    limit,
                    &columns,
                    &evaluator,
                    &mut builder,
                    &schema,
                    &output,
                    &ctx,
                    &mut row_buf,
                    emitted,
                )
                .await?;

            if limit.is_some() && emitted >= limit.unwrap() {
                if let Some(remaining) = builder.take() {
                    if remaining.len() > 0 {
                        let batch = remaining
                            .finish()
                            .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                        output
                            .send(Arc::new(batch))
                            .await
                            .map_err(|_| FlowOperatorError::ChannelClosed)?;
                    }
                }
                info!(
                    target: "sneldb::memtable_source",
                    emitted,
                    "MemTable source completed"
                );
                return Ok(());
            }
        }

        for passive in self.config.passive_memtables.iter() {
            let guard = passive.lock().await;
            emitted = self
                .push_rows_from_memtable(
                    &*guard,
                    limit,
                    &columns,
                    &evaluator,
                    &mut builder,
                    &schema,
                    &output,
                    &ctx,
                    &mut row_buf,
                    emitted,
                )
                .await?;

            if limit.is_some() && emitted >= limit.unwrap() {
                break;
            }
        }

        if let Some(remaining) = builder.take() {
            if remaining.len() > 0 {
                let batch = remaining
                    .finish()
                    .map_err(|e| FlowOperatorError::Batch(e.to_string()))?;
                output
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| FlowOperatorError::ChannelClosed)?;
            }
        }

        info!(
            target: "sneldb::memtable_source",
            emitted,
            "MemTable source completed"
        );
        Ok(())
    }
}

fn compare_json_values(a: &Value, b: &Value) -> Ordering {
    if let (Some(va), Some(vb)) = (a.as_u64(), b.as_u64()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_i64(), b.as_i64()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_f64(), b.as_f64()) {
        return va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
    }
    if let (Some(va), Some(vb)) = (a.as_bool(), b.as_bool()) {
        return va.cmp(&vb);
    }
    if let (Some(va), Some(vb)) = (a.as_str(), b.as_str()) {
        return va.cmp(vb);
    }
    a.to_string().cmp(&b.to_string())
}

fn field_type_to_logical(field_type: &crate::engine::schema::types::FieldType) -> String {
    use crate::engine::schema::types::FieldType;
    match field_type {
        FieldType::String => "String".into(),
        FieldType::U64 | FieldType::I64 => "Integer".into(),
        FieldType::F64 => "Float".into(),
        FieldType::Bool => "Boolean".into(),
        FieldType::Timestamp => "Timestamp".into(),
        FieldType::Date => "Date".into(),
        FieldType::Optional(inner) => field_type_to_logical(inner),
        FieldType::Enum(_) => "Enum".into(),
    }
}

fn logical_type_for_builtin(name: &str) -> String {
    match name {
        "context_id" | "event_type" => "String".into(),
        "timestamp" => "Timestamp".into(),
        "event_id" => "Integer".into(),
        _ => "Unknown".into(),
    }
}
