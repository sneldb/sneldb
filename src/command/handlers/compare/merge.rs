use std::collections::HashMap;
use std::sync::Arc;

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::QueryCommand;
use crate::engine::core::read::aggregate::partial::{AggState, GroupKey};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::{
    BatchPool, BatchReceiver, BatchSchema, BatchSender, ColumnBatch, FlowChannel, FlowMetrics,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;
use tokio::task::JoinHandle;

/// Merges multiple query streams into a single comparison stream with side-by-side columns.
pub struct ComparisonStreamMerger {
    queries: Vec<QueryCommand>,
    prefixes: Vec<String>,
}

impl ComparisonStreamMerger {
    pub fn new(queries: &[QueryCommand]) -> Self {
        let prefixes = Self::derive_column_prefixes(queries);
        Self {
            queries: queries.to_vec(),
            prefixes,
        }
    }

    /// Derives column prefixes for each query based on event types.
    /// Falls back to left/right/third/etc if event types are duplicated.
    fn derive_column_prefixes(queries: &[QueryCommand]) -> Vec<String> {
        let mut prefixes = Vec::new();
        let mut seen = HashMap::new();
        let fallback_names = vec![
            "left", "right", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth",
            "tenth",
        ];

        for (idx, query) in queries.iter().enumerate() {
            let event_type = &query.event_type;
            if seen.contains_key(event_type) {
                // Use fallback name
                prefixes.push(
                    fallback_names
                        .get(idx)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("side_{}", idx + 1)),
                );
            } else {
                prefixes.push(event_type.clone());
                seen.insert(event_type.clone(), idx);
            }
        }

        prefixes
    }

    /// Merges multiple query streams into a single comparison stream.
    pub async fn merge_streams(
        &self,
        mut streams: Vec<QueryBatchStream>,
    ) -> Result<QueryBatchStream, String> {
        if streams.len() != self.queries.len() {
            return Err(format!(
                "Expected {} streams, got {}",
                self.queries.len(),
                streams.len()
            ));
        }

        // Collect all batches from all streams
        let mut all_results: Vec<Vec<(GroupKey, Vec<ScalarValue>)>> = Vec::new();

        for (idx, mut stream) in streams.into_iter().enumerate() {
            let query = &self.queries[idx];
            let aggregate_plan = AggregatePlan::from_query_command(query)
                .ok_or_else(|| format!("Query {} is not an aggregate query", idx))?;

            let mut query_results = Vec::new();
            let schema = stream.schema();

            // Collect all batches from this stream
            while let Some(batch) = stream.recv().await {
                let rows = Self::parse_batch_to_rows(&batch, &schema, &aggregate_plan)?;
                query_results.extend(rows);
            }

            all_results.push(query_results);
        }

        // Merge results by GroupKey (full outer join)
        let merged = self.merge_results_by_group_key(all_results)?;

        // Build output schema
        let output_schema = self.build_output_schema()?;

        // Create output stream
        let metrics = FlowMetrics::new();
        let (tx, rx) = FlowChannel::bounded(32768, Arc::clone(&metrics));

        // Spawn task to emit merged results
        let output_schema_clone = Arc::clone(&output_schema);
        let queries_clone = self.queries.clone();
        let metrics_for_task = Arc::clone(&metrics);
        let task = tokio::spawn(async move {
            if let Err(e) = Self::emit_merged_results(
                merged,
                output_schema_clone,
                tx,
                metrics_for_task,
                queries_clone,
            )
            .await
            {
                eprintln!("Error emitting merged comparison results: {}", e);
            }
        });

        Ok(QueryBatchStream::new(output_schema, rx, vec![task]))
    }

    /// Parses a batch into rows with GroupKey and metric values.
    fn parse_batch_to_rows(
        batch: &ColumnBatch,
        schema: &BatchSchema,
        aggregate_plan: &AggregatePlan,
    ) -> Result<Vec<(GroupKey, Vec<ScalarValue>)>, String> {
        use crate::command::handlers::query::merge::aggregate_stream::AggregateStreamMerger;

        let mut rows = Vec::new();
        let columns = batch.columns();
        let column_views: Vec<&[ScalarValue]> = columns.iter().map(|c| c.as_slice()).collect();
        let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();

        for row_idx in 0..batch.len() {
            match AggregateStreamMerger::parse_aggregate_row(
                &column_views,
                &column_names,
                row_idx,
                aggregate_plan,
            ) {
                Ok((group_key, states)) => {
                    // Convert AggStates to ScalarValues (final metric values)
                    let mut metric_values = Vec::new();
                    for (spec, state) in aggregate_plan.ops.iter().zip(states.iter()) {
                        let value = AggregateStreamMerger::agg_state_to_scalar(state, spec)?;
                        metric_values.push(value);
                    }
                    rows.push((group_key, metric_values));
                }
                Err(e) => return Err(format!("Failed to parse row {}: {}", row_idx, e)),
            }
        }

        Ok(rows)
    }

    /// Merges results from multiple queries by GroupKey (full outer join).
    fn merge_results_by_group_key(
        &self,
        results_by_query: Vec<Vec<(GroupKey, Vec<ScalarValue>)>>,
    ) -> Result<HashMap<GroupKey, Vec<Option<Vec<ScalarValue>>>>, String> {
        let mut merged: HashMap<GroupKey, Vec<Option<Vec<ScalarValue>>>> = HashMap::new();

        // Collect all unique group keys
        let mut all_keys = std::collections::HashSet::new();
        for query_results in &results_by_query {
            for (key, _) in query_results {
                all_keys.insert(key.clone());
            }
        }

        // Initialize merged map with all keys
        for key in &all_keys {
            merged.insert(key.clone(), vec![None; self.queries.len()]);
        }

        // Fill in values for each query
        for (query_idx, query_results) in results_by_query.into_iter().enumerate() {
            for (key, values) in query_results {
                if let Some(entry) = merged.get_mut(&key) {
                    entry[query_idx] = Some(values);
                }
            }
        }

        Ok(merged)
    }

    /// Builds the output schema with prefixed column names.
    fn build_output_schema(&self) -> Result<Arc<BatchSchema>, String> {
        // Determine shared schema structure - check all queries to find group_by/time_bucket
        let mut time_bucket = None;
        let mut group_by = None;

        // Check all queries to find shared group_by and time_bucket
        for query in &self.queries {
            if let Some(plan) = AggregatePlan::from_query_command(query) {
                if time_bucket.is_none() {
                    time_bucket = plan.time_bucket.clone();
                }
                if group_by.is_none() {
                    group_by = plan.group_by.clone();
                }
                // Once we have both, we can break
                if time_bucket.is_some() && group_by.is_some() {
                    break;
                }
            }
        }

        let mut columns = Vec::new();

        // Add bucket column if present
        if time_bucket.is_some() {
            columns.push(ColumnSpec {
                name: "bucket".to_string(),
                logical_type: "Timestamp".to_string(),
            });
        }

        // Add group_by columns
        if let Some(group_by_fields) = &group_by {
            for field in group_by_fields {
                columns.push(ColumnSpec {
                    name: field.clone(),
                    logical_type: "String".to_string(),
                });
            }
        }

        // Add metric columns for each query with prefix
        for (idx, prefix) in self.prefixes.iter().enumerate() {
            let query = &self.queries[idx];
            let plan = AggregatePlan::from_query_command(query)
                .ok_or_else(|| format!("Query {} is not an aggregate query", idx))?;

            for spec in &plan.ops {
                let metric_name = match spec {
                    AggregateOpSpec::CountAll => "count".to_string(),
                    AggregateOpSpec::CountField { field } => format!("count_{}", field),
                    AggregateOpSpec::CountUnique { field } => format!("count_unique_{}", field),
                    AggregateOpSpec::Total { field } => format!("total_{}", field),
                    AggregateOpSpec::Avg { field } => format!("avg_{}", field),
                    AggregateOpSpec::Min { field } => format!("min_{}", field),
                    AggregateOpSpec::Max { field } => format!("max_{}", field),
                };
                let column_name = format!("{}.{}", prefix, metric_name);
                let logical_type = match spec {
                    AggregateOpSpec::CountAll
                    | AggregateOpSpec::CountField { .. }
                    | AggregateOpSpec::CountUnique { .. } => "Integer",
                    AggregateOpSpec::Total { .. } => "Integer",
                    AggregateOpSpec::Avg { .. } => "Float",
                    AggregateOpSpec::Min { .. } | AggregateOpSpec::Max { .. } => "Integer",
                };
                columns.push(ColumnSpec {
                    name: column_name,
                    logical_type: logical_type.to_string(),
                });
            }
        }

        Ok(Arc::new(BatchSchema::new(columns).map_err(|e| {
            format!("Failed to build output schema: {}", e)
        })?))
    }

    /// Emits merged results as batches.
    async fn emit_merged_results(
        merged: HashMap<GroupKey, Vec<Option<Vec<ScalarValue>>>>,
        output_schema: Arc<BatchSchema>,
        output: BatchSender,
        _metrics: Arc<FlowMetrics>,
        queries: Vec<QueryCommand>,
    ) -> Result<(), String> {
        // Pre-compute number of metrics per query for efficient NULL handling
        let metrics_per_query: Vec<usize> = queries
            .iter()
            .map(|query| {
                AggregatePlan::from_query_command(query)
                    .map(|plan| plan.ops.len())
                    .unwrap_or(0)
            })
            .collect();

        let mut rows: Vec<Vec<ScalarValue>> = Vec::new();

        // Convert merged groups to rows
        for (group_key, query_values) in merged {
            let mut row = Vec::new();

            // Add bucket if present
            if let Some(bucket) = group_key.bucket {
                row.push(ScalarValue::Int64(bucket as i64));
            }

            // Add group_by values
            for group_value in &group_key.groups {
                row.push(ScalarValue::Utf8(group_value.clone()));
            }

            // Add metric values for each query (or NULL if missing)
            for (query_idx, query_vals) in query_values.iter().enumerate() {
                match query_vals {
                    Some(vals) => row.extend(vals.clone()),
                    None => {
                        // Add NULL values for missing metrics
                        let num_metrics = metrics_per_query.get(query_idx).copied().unwrap_or(0);
                        for _ in 0..num_metrics {
                            row.push(ScalarValue::Null);
                        }
                    }
                }
            }

            rows.push(row);
        }

        // Sort rows by group key for determinism (bucket, then group_by fields)
        rows.sort_by(|a, b| {
            let mut idx = 0;

            // Compare bucket if present (first column)
            if !a.is_empty() && !b.is_empty() {
                let bucket_cmp = a[idx].compare(&b[idx]);
                if bucket_cmp != std::cmp::Ordering::Equal {
                    return bucket_cmp;
                }
                idx += 1;
            }

            // Compare group_by fields (all String columns after bucket)
            while idx < a.len().min(b.len()) {
                let cmp = a[idx].compare(&b[idx]);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
                idx += 1;
            }

            a.len().cmp(&b.len())
        });

        // Emit rows as batches
        if rows.is_empty() {
            return Ok(());
        }

        let batch_size = 32768;
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
}
