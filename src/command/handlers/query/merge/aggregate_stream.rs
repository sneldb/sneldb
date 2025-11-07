use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::aggregate::partial::{AggState, GroupKey};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{
    BatchPool, BatchReceiver, BatchSchema, FlowChannel, FlowMetrics,
};
use crate::engine::types::ScalarValue;
use serde_json;
use tokio::task::JoinHandle;

/// Compares two ScalarValues for sorting.
fn compare_scalar_values(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    // Try u64 first (existing behavior)
    if let (Some(va), Some(vb)) = (a.as_u64(), b.as_u64()) {
        return va.cmp(&vb);
    }
    // Use the efficient direct comparison method
    a.compare(b)
}

/// Merges streaming aggregate results from multiple shards by combining
/// partial aggregates with the same group key.
pub struct AggregateStreamMerger {
    aggregate_plan: AggregatePlan,
    limit: Option<u32>,
    offset: Option<u32>,
    order_by: Option<crate::command::types::OrderSpec>,
}

impl AggregateStreamMerger {
    /// Creates a new aggregate stream merger for the given command.
    pub fn new(command: &crate::command::types::Command) -> Self {
        let aggregate_plan = AggregatePlan::from_command(command)
            .expect("AggregateStreamMerger should only be used for aggregate queries");
        let (limit, offset, order_by) = match command {
            crate::command::types::Command::Query {
                limit,
                offset,
                order_by,
                ..
            } => (*limit, *offset, order_by.clone()),
            _ => (None, None, None),
        };
        Self {
            aggregate_plan,
            limit,
            offset,
            order_by,
        }
    }

    /// Merges aggregate batches from multiple shards into a single stream.
    /// Groups with the same key are merged using AggState::merge.
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

        // Build final output schema (with average, not sum/count for AVG)
        let output_schema = Self::build_final_output_schema(&self.aggregate_plan)?;

        // Spawn merger task that collects and merges aggregate batches
        let aggregate_plan = self.aggregate_plan.clone();
        let limit = self.limit;
        let offset = self.offset;
        let order_by = self.order_by.clone();
        let merger_metrics = Arc::clone(&metrics);
        let schema_for_task = Arc::clone(&schema);
        tasks.push(tokio::spawn(async move {
            if let Err(err) = Self::merge_aggregate_batches(
                receivers,
                tx,
                schema_for_task,
                aggregate_plan,
                limit,
                offset,
                order_by,
                merger_metrics,
            )
            .await
            {
                tracing::error!(
                    target: "sneldb::aggregate_stream_merger",
                    error = %err,
                    "Failed to merge aggregate batches"
                );
            }
        }));

        Ok(QueryBatchStream::new(output_schema, rx, tasks))
    }

    /// Merges aggregate batches from multiple receivers.
    async fn merge_aggregate_batches(
        receivers: Vec<BatchReceiver>,
        output: crate::engine::core::read::flow::BatchSender,
        schema: Arc<BatchSchema>,
        aggregate_plan: AggregatePlan,
        limit: Option<u32>,
        offset: Option<u32>,
        order_by: Option<crate::command::types::OrderSpec>,
        metrics: Arc<FlowMetrics>,
    ) -> Result<(), String> {
        // Map to store merged groups: GroupKey -> Vec<AggState>
        let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();

        // Collect all batches from all receivers
        let mut all_batches = Vec::new();
        let receivers = receivers;
        for mut receiver in receivers {
            while let Some(batch) = receiver.recv().await {
                all_batches.push(batch);
            }
        }

        // Process each batch and merge groups
        for batch_arc in all_batches {
            Self::merge_batch_into_groups(
                &batch_arc,
                &schema,
                &aggregate_plan,
                &mut merged_groups,
            )?;
        }

        // Convert merged groups back to batches and emit
        Self::emit_merged_groups(
            merged_groups,
            schema,
            aggregate_plan,
            limit,
            offset,
            order_by,
            output,
            metrics,
        )
        .await
    }

    /// Merges a single batch into the merged_groups HashMap.
    pub(crate) fn merge_batch_into_groups(
        batch: &crate::engine::core::read::flow::ColumnBatch,
        schema: &Arc<BatchSchema>,
        aggregate_plan: &AggregatePlan,
        merged_groups: &mut HashMap<GroupKey, Vec<AggState>>,
    ) -> Result<(), String> {
        let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
        let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::with_capacity(schema.column_count());

        for col_idx in 0..schema.column_count() {
            column_vecs.push(
                batch
                    .column(col_idx)
                    .map_err(|e| format!("failed to read column {}: {}", col_idx, e))?,
            );
        }

        let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

        for row_idx in 0..batch.len() {
            let (group_key, states) =
                Self::parse_aggregate_row(&column_views, &column_names, row_idx, aggregate_plan)?;

            match merged_groups.entry(group_key) {
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(states);
                }
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let existing = e.get_mut();
                    if existing.len() == states.len() {
                        for (existing_state, new_state) in existing.iter_mut().zip(states.iter()) {
                            existing_state.merge(new_state);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Parses a single aggregate row to extract GroupKey and AggState values.
    pub(crate) fn parse_aggregate_row(
        column_views: &[&[ScalarValue]],
        column_names: &[String],
        row_idx: usize,
        aggregate_plan: &AggregatePlan,
    ) -> Result<(GroupKey, Vec<AggState>), String> {
        let mut bucket: Option<u64> = None;
        let mut group_by_values: Vec<String> = Vec::new();
        let mut states: Vec<AggState> = Vec::new();

        let mut metric_start_idx = 0;

        // Parse bucket if present
        if aggregate_plan.time_bucket.is_some() {
            if let Some(bucket_col) = column_names.first() {
                if bucket_col == "bucket" {
                    let value = column_views[0]
                        .get(row_idx)
                        .ok_or_else(|| "missing bucket value".to_string())?;
                    bucket = Self::scalar_to_u64(value);
                    metric_start_idx = 1;
                }
            }
        }

        // Parse group_by fields
        if let Some(group_by_fields) = &aggregate_plan.group_by {
            let group_start = metric_start_idx;
            for (i, field) in group_by_fields.iter().enumerate() {
                let col_idx = group_start + i;
                if col_idx >= column_names.len() {
                    return Err(format!("missing group_by column: {}", field));
                }
                let value = column_views[col_idx]
                    .get(row_idx)
                    .ok_or_else(|| format!("missing value for group_by field: {}", field))?;
                group_by_values.push(Self::scalar_to_string(value));
            }
            metric_start_idx += group_by_fields.len();
        }

        // Parse metric columns
        // Note: AVG uses two columns (sum and count), CountUnique uses JSON string
        // Both need special handling
        let mut col_idx = metric_start_idx;
        for spec in aggregate_plan.ops.iter() {
            match spec {
                AggregateOpSpec::Avg { field } => {
                    // AVG has two columns: sum and count
                    let sum_col_name = format!("avg_{}_sum", field);
                    let count_col_name = format!("avg_{}_count", field);

                    // Find sum column
                    let sum_col_idx = column_names
                        .iter()
                        .position(|n| n == &sum_col_name)
                        .ok_or_else(|| format!("missing avg_{}_sum column", field))?;
                    let sum_value = column_views[sum_col_idx]
                        .get(row_idx)
                        .ok_or_else(|| format!("missing sum value for avg_{}", field))?;
                    let sum = Self::scalar_to_i64(sum_value)?;

                    // Find count column
                    let count_col_idx = column_names
                        .iter()
                        .position(|n| n == &count_col_name)
                        .ok_or_else(|| format!("missing avg_{}_count column", field))?;
                    let count_value = column_views[count_col_idx]
                        .get(row_idx)
                        .ok_or_else(|| format!("missing count value for avg_{}", field))?;
                    let count = Self::scalar_to_i64(count_value)?;

                    states.push(AggState::Avg { sum, count });
                    // AVG uses 2 columns, so we advance by 2
                    col_idx += 2;
                }
                AggregateOpSpec::CountUnique { field } => {
                    // CountUnique has one column: JSON array string with unique values
                    let values_col_name = format!("count_unique_{}_values", field);

                    // Find values column
                    let values_col_idx = column_names
                        .iter()
                        .position(|n| n == &values_col_name)
                        .ok_or_else(|| format!("missing count_unique_{}_values column", field))?;
                    let json_value = column_views[values_col_idx]
                        .get(row_idx)
                        .ok_or_else(|| format!("missing values for count_unique_{}", field))?;

                    // Parse JSON array string back to HashSet
                    let json_str = Self::scalar_to_string(json_value);
                    let values: std::collections::HashSet<String> =
                        if json_str.is_empty() || json_str == "[]" {
                            // Empty string or empty JSON array both represent empty HashSet
                            // (empty string might occur if no values were found, but code should produce "[]")
                            std::collections::HashSet::new()
                        } else {
                            serde_json::from_str(&json_str).map_err(|e| {
                                format!("failed to parse CountUnique JSON '{}': {}", json_str, e)
                            })?
                        };

                    states.push(AggState::CountUnique { values });
                    // CountUnique uses 1 column, so we advance by 1
                    col_idx += 1;
                }
                _ => {
                    // Other aggregations use 1 column
                    if col_idx >= column_names.len() {
                        return Err(format!("missing metric column for spec {:?}", spec));
                    }
                    let value = column_views[col_idx]
                        .get(row_idx)
                        .ok_or_else(|| format!("missing metric value for spec {:?}", spec))?;
                    let state = Self::scalar_to_agg_state(value, spec)?;
                    states.push(state);
                    col_idx += 1;
                }
            }
        }

        let group_key = GroupKey {
            bucket,
            groups: group_by_values,
        };

        Ok((group_key, states))
    }

    /// Converts a ScalarValue to AggState based on the aggregate operation spec.
    pub(crate) fn scalar_to_agg_state(
        value: &ScalarValue,
        spec: &AggregateOpSpec,
    ) -> Result<AggState, String> {
        match spec {
            AggregateOpSpec::CountAll | AggregateOpSpec::CountField { .. } => {
                let count = Self::scalar_to_i64(value)?;
                Ok(AggState::CountAll { count })
            }
            AggregateOpSpec::CountUnique { .. } => {
                // CountUnique is handled directly in parse_aggregate_row() since it uses JSON string
                // This case should never be reached for CountUnique
                Err("CountUnique should be handled directly in parse_aggregate_row, not via scalar_to_agg_state".to_string())
            }
            AggregateOpSpec::Total { .. } => {
                let sum = Self::scalar_to_i64(value)?;
                Ok(AggState::Sum { sum })
            }
            // AVG is handled directly in parse_aggregate_row() since it uses two columns
            // This case should never be reached for AVG
            AggregateOpSpec::Avg { .. } => {
                Err("AVG should be handled directly in parse_aggregate_row, not via scalar_to_agg_state".to_string())
            }
            AggregateOpSpec::Min { .. } => {
                let (min_num, min_str) = Self::scalar_to_min_max(value)?;
                Ok(AggState::Min { min_num, min_str })
            }
            AggregateOpSpec::Max { .. } => {
                let (max_num, max_str) = Self::scalar_to_min_max(value)?;
                Ok(AggState::Max { max_num, max_str })
            }
        }
    }

    /// Converts ScalarValue to i64.
    pub(crate) fn scalar_to_i64(value: &ScalarValue) -> Result<i64, String> {
        match value {
            ScalarValue::Int64(i) => Ok(*i),
            ScalarValue::Float64(f) => Ok(*f as i64),
            ScalarValue::Utf8(s) => s
                .parse::<i64>()
                .map_err(|e| format!("failed to parse '{}' as i64: {}", s, e)),
            _ => Err(format!("cannot convert {:?} to i64", value)),
        }
    }

    /// Converts ScalarValue to u64.
    pub(crate) fn scalar_to_u64(value: &ScalarValue) -> Option<u64> {
        match value {
            ScalarValue::Int64(i) if *i >= 0 => Some(*i as u64),
            ScalarValue::Timestamp(t) if *t >= 0 => Some(*t as u64),
            ScalarValue::Utf8(s) => s.parse::<u64>().ok(),
            _ => None,
        }
    }

    /// Converts ScalarValue to String.
    pub(crate) fn scalar_to_string(value: &ScalarValue) -> String {
        match value {
            ScalarValue::Utf8(s) => s.clone(),
            ScalarValue::Int64(i) => i.to_string(),
            ScalarValue::Float64(f) => f.to_string(),
            ScalarValue::Boolean(b) => b.to_string(),
            ScalarValue::Timestamp(t) => t.to_string(),
            ScalarValue::Null => String::new(),
            ScalarValue::Binary(_) => String::new(), // Binary not supported in aggregates
        }
    }

    /// Converts ScalarValue to (Option<i64>, Option<String>) for Min/Max.
    pub(crate) fn scalar_to_min_max(
        value: &ScalarValue,
    ) -> Result<(Option<i64>, Option<String>), String> {
        match value {
            ScalarValue::Int64(i) => Ok((Some(*i), None)),
            ScalarValue::Utf8(s) => Ok((None, Some(s.clone()))),
            ScalarValue::Float64(f) => Ok((Some(*f as i64), None)),
            ScalarValue::Null => Ok((None, None)),
            _ => Err(format!("cannot convert {:?} to min/max", value)),
        }
    }

    /// Emits merged groups as batches.
    pub(crate) async fn emit_merged_groups(
        mut merged_groups: HashMap<GroupKey, Vec<AggState>>,
        _input_schema: Arc<BatchSchema>, // Input schema has sum/count for AVG
        aggregate_plan: AggregatePlan,
        limit: Option<u32>,
        offset: Option<u32>,
        order_by: Option<crate::command::types::OrderSpec>,
        output: crate::engine::core::read::flow::BatchSender,
        _metrics: Arc<FlowMetrics>,
    ) -> Result<(), String> {
        // Build final output schema (with average, not sum/count)
        let output_schema = Self::build_final_output_schema(&aggregate_plan)?;

        // Filter out empty groups first (before sorting/limiting)
        if aggregate_plan.group_by.is_some() {
            merged_groups.retain(|group_key, _| {
                !group_key.groups.is_empty() && !group_key.groups.iter().any(|g| g.is_empty())
            });
        }

        // Build all rows first
        let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
        for (group_key, states) in merged_groups {
            let mut row = Vec::with_capacity(output_schema.column_count());

            // Add bucket if present
            if aggregate_plan.time_bucket.is_some() {
                if let Some(bucket) = group_key.bucket {
                    row.push(ScalarValue::Int64(bucket as i64));
                } else {
                    row.push(ScalarValue::Null);
                }
            }

            // Add group_by values
            if let Some(group_by_fields) = &aggregate_plan.group_by {
                for (i, _field) in group_by_fields.iter().enumerate() {
                    let value = group_key
                        .groups
                        .get(i)
                        .map(|s| ScalarValue::Utf8(s.clone()))
                        .unwrap_or(ScalarValue::Utf8(String::new()));
                    row.push(value);
                }
            }

            // Add metric values (converted to final output format)
            for (spec, state) in aggregate_plan.ops.iter().zip(states.iter()) {
                let value = Self::agg_state_to_scalar(state, spec)?;
                row.push(value);
            }

            rows.push(row);
        }

        // Sort rows based on ORDER BY or by group keys for determinism
        if let Some(order_spec) = &order_by {
            // Find the column index for the ORDER BY field
            let order_index = Self::find_column_index(&output_schema, &order_spec.field)?;
            let ascending = !order_spec.desc;

            // Use sort_unstable_by for better performance
            rows.sort_unstable_by(|a, b| {
                let ord = compare_scalar_values(&a[order_index], &b[order_index]);
                if ascending { ord } else { ord.reverse() }
            });
        } else {
            // No ORDER BY: sort by group keys for deterministic LIMIT
            // Sort by all group_by fields (or bucket if present) for stable ordering
            rows.sort_unstable_by(|a, b| {
                // Compare bucket first if present
                if aggregate_plan.time_bucket.is_some() {
                    let bucket_cmp = compare_scalar_values(&a[0], &b[0]);
                    if bucket_cmp != std::cmp::Ordering::Equal {
                        return bucket_cmp;
                    }
                }

                // Then compare group_by fields
                let group_start = if aggregate_plan.time_bucket.is_some() {
                    1
                } else {
                    0
                };
                if let Some(group_by_fields) = &aggregate_plan.group_by {
                    for i in 0..group_by_fields.len() {
                        let idx = group_start + i;
                        let cmp = compare_scalar_values(&a[idx], &b[idx]);
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                }

                std::cmp::Ordering::Equal
            });
        }

        // Apply OFFSET
        if let Some(offset_val) = offset {
            let offset_usize = offset_val as usize;
            if offset_usize >= rows.len() {
                rows.clear();
            } else {
                rows.drain(0..offset_usize);
            }
        }

        // Apply LIMIT
        if let Some(limit_val) = limit {
            if rows.len() > limit_val as usize {
                rows.truncate(limit_val as usize);
            }
        }

        // Emit rows as batches
        if rows.is_empty() {
            return Ok(());
        }

        let batch_size = 32768; // Use same batch size as streaming
        let pool = BatchPool::new(batch_size)
            .map_err(|e| format!("failed to create batch pool: {}", e))?;
        let mut builder = pool.acquire(Arc::clone(&output_schema));

        for row in rows {
            builder
                .push_row(&row)
                .map_err(|e| format!("failed to push row: {}", e))?;

            if builder.is_full() {
                let batch = builder
                    .finish()
                    .map_err(|e| format!("failed to finish batch: {}", e))?;
                output
                    .send(Arc::new(batch))
                    .await
                    .map_err(|_| "output channel closed".to_string())?;
                builder = pool.acquire(Arc::clone(&output_schema));
            }
        }

        // Emit final batch if any remaining rows
        if builder.len() > 0 {
            let batch = builder
                .finish()
                .map_err(|e| format!("failed to finish final batch: {}", e))?;
            output
                .send(Arc::new(batch))
                .await
                .map_err(|_| "output channel closed".to_string())?;
        }

        Ok(())
    }

    /// Finds the column index for a given field name in the output schema.
    fn find_column_index(schema: &BatchSchema, field: &str) -> Result<usize, String> {
        schema
            .columns()
            .iter()
            .position(|c| c.name == field)
            .ok_or_else(|| format!("ORDER BY field '{}' not found in output schema", field))
    }

    /// Builds the final output schema (with average, not sum/count for AVG)
    pub(crate) fn build_final_output_schema(
        plan: &AggregatePlan,
    ) -> Result<Arc<BatchSchema>, String> {
        use crate::engine::core::read::flow::BatchSchema;
        use crate::engine::core::read::result::ColumnSpec;

        let mut columns: Vec<ColumnSpec> = Vec::new();

        // Add bucket column if present
        if plan.time_bucket.is_some() {
            columns.push(ColumnSpec {
                name: "bucket".to_string(),
                logical_type: "Timestamp".to_string(),
            });
        }

        // Add group_by columns
        if let Some(group_by) = &plan.group_by {
            for field in group_by {
                columns.push(ColumnSpec {
                    name: field.clone(),
                    logical_type: "String".to_string(),
                });
            }
        }

        // Add metric columns (AVG outputs average, not sum/count)
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

        BatchSchema::new(columns)
            .map(Arc::new)
            .map_err(|e| format!("failed to build output schema: {}", e))
    }

    /// Converts AggState back to ScalarValue for output.
    pub(crate) fn agg_state_to_scalar(
        state: &AggState,
        spec: &AggregateOpSpec,
    ) -> Result<ScalarValue, String> {
        match (spec, state) {
            (AggregateOpSpec::CountAll, AggState::CountAll { count }) => {
                Ok(ScalarValue::Int64(*count))
            }
            (AggregateOpSpec::CountField { .. }, AggState::CountAll { count }) => {
                Ok(ScalarValue::Int64(*count))
            }
            (AggregateOpSpec::CountUnique { .. }, AggState::CountUnique { values }) => {
                Ok(ScalarValue::Int64(values.len() as i64))
            }
            (AggregateOpSpec::Total { .. }, AggState::Sum { sum }) => Ok(ScalarValue::Int64(*sum)),
            (AggregateOpSpec::Avg { .. }, AggState::Avg { sum, count }) => {
                let avg = if *count == 0 {
                    0.0
                } else {
                    (*sum as f64) / (*count as f64)
                };
                Ok(ScalarValue::Float64(avg))
            }
            (AggregateOpSpec::Min { .. }, AggState::Min { min_num, min_str }) => {
                if let Some(n) = min_num {
                    Ok(ScalarValue::Int64(*n))
                } else if let Some(s) = min_str {
                    Ok(ScalarValue::Utf8(s.clone()))
                } else {
                    Ok(ScalarValue::Utf8(String::new()))
                }
            }
            (AggregateOpSpec::Max { .. }, AggState::Max { max_num, max_str }) => {
                if let Some(n) = max_num {
                    Ok(ScalarValue::Int64(*n))
                } else if let Some(s) = max_str {
                    Ok(ScalarValue::Utf8(s.clone()))
                } else {
                    Ok(ScalarValue::Utf8(String::new()))
                }
            }
            _ => Err(format!(
                "mismatch between spec {:?} and state {:?}",
                spec, state
            )),
        }
    }
}
