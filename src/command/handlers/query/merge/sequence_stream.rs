use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, error, info};

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::{Command, EventSequence};
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, ColumnBatch, FlowChannel, FlowMetrics,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::read::sequence::utils::scalar_to_string;
use crate::engine::core::read::sequence::{
    ColumnarGrouper, SequenceMatcher, SequenceMaterializer, SequenceWhereEvaluator,
};
use crate::engine::core::{CandidateZone, ColumnValues, Event};
use crate::engine::types::ScalarValue;

/// Merges streaming results from sequence sub-queries, groups by link field,
/// matches sequences, and materializes results.
pub struct SequenceStreamMerger {
    event_sequence: EventSequence,
    link_field: String,
    sequence_time_field: String,
    event_types: Vec<String>,
    limit: Option<usize>,
}

impl SequenceStreamMerger {
    pub fn new(
        event_sequence: EventSequence,
        link_field: String,
        sequence_time_field: String,
        limit: Option<usize>,
    ) -> Self {
        let mut event_types = vec![event_sequence.head.event.clone()];
        for (_link, target) in &event_sequence.links {
            event_types.push(target.event.clone());
        }

        Self {
            event_sequence,
            link_field,
            sequence_time_field,
            event_types,
            limit,
        }
    }

    /// Merges streaming handles grouped by event type, performs sequence matching,
    /// and returns a stream of matched sequence events.
    pub async fn merge(
        &self,
        ctx: &QueryContext<'_>,
        handles_by_type: HashMap<String, Vec<ShardFlowHandle>>,
    ) -> Result<QueryBatchStream, String> {
        info!(
            target: "sneldb::sequence::streaming::merger",
            event_types = ?self.event_types,
            "Starting sequence stream merge"
        );

        // Step 1: Collect batches from all streams and convert to zones grouped by event type
        let zones_by_type = self.collect_zones_by_type(handles_by_type).await?;

        // Step 2: Group zones by link_field using columnar grouper
        let grouper =
            ColumnarGrouper::new(self.link_field.clone(), self.sequence_time_field.clone());
        let grouped_indices = grouper.group_zones_by_link_field(&zones_by_type);

        // Step 3: Match sequences
        let where_clause = if let Command::Query { where_clause, .. } = ctx.command {
            where_clause.as_ref()
        } else {
            None
        };

        let where_evaluator =
            SequenceWhereEvaluator::new(where_clause, &self.event_types, &ctx.registry)
                .await
                .map_err(|e| format!("WHERE clause validation failed: {}", e))?;
        let matcher = SequenceMatcher::new(
            self.event_sequence.clone(),
            self.sequence_time_field.clone(),
        )
        .with_where_evaluator(where_evaluator);

        let matched_indices = matcher.match_sequences(grouped_indices, &zones_by_type, self.limit);

        debug!(
            target: "sneldb::sequence::streaming::merger",
            matched_indices_count = matched_indices.len(),
            "Matched indices from matcher"
        );

        // Step 4: Materialize matched sequences
        let materializer = SequenceMaterializer::new();
        let matched_sequences = materializer.materialize_matches(matched_indices, &zones_by_type);

        debug!(
            target: "sneldb::sequence::streaming::merger",
            matched_sequences_count = matched_sequences.len(),
            "Materialized sequences"
        );

        // Step 5: Flatten matched sequences into events and convert to stream
        let mut matched_events = Vec::new();
        for (seq_idx, seq) in matched_sequences.iter().enumerate() {
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::streaming::merger",
                    sequence_idx = seq_idx,
                    event_count = seq.events.len(),
                    "Processing sequence"
                );
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                for (event_idx, event) in seq.events.iter().enumerate() {
                    debug!(
                        target: "sneldb::sequence::streaming::merger",
                        sequence_idx = seq_idx,
                        event_idx = event_idx,
                        event_type = %event.event_type,
                        context_id = %event.context_id,
                        timestamp = event.timestamp,
                        "Event in sequence"
                    );
                }
            }
            matched_events.extend(seq.events.clone());
        }

        debug!(
            target: "sneldb::sequence::streaming::merger",
            matched_events_count = matched_events.len(),
            "Flattened events from sequences"
        );

        self.create_result_stream(matched_events).await
    }

    /// Collects batches from streaming handles and converts to CandidateZones, grouped by event type.
    ///
    /// This processes handles in parallel for better performance.
    ///
    /// IMPORTANT: We must keep the handle tasks alive while collecting batches, otherwise
    /// the background tasks that send batches will be dropped and the channel will close.
    async fn collect_zones_by_type(
        &self,
        handles_by_type: HashMap<String, Vec<ShardFlowHandle>>,
    ) -> Result<HashMap<String, Vec<CandidateZone>>, String> {
        // Process all event types in parallel
        let mut tasks = Vec::new();

        for (event_type, handles) in handles_by_type {
            let event_type_clone = event_type.clone();
            let task = tokio::spawn(async move {
                let mut all_batches = Vec::new();

                // Collect batches from all handles for this event type in parallel
                // Keep handle_tasks alive to prevent channel from closing
                let mut collect_tasks = Vec::new();
                let mut handle_tasks_vec = Vec::new();

                for handle in handles {
                    let (receiver, schema, handle_tasks) = handle.into_parts();

                    // Keep handle tasks alive - they drive the streaming pipeline
                    handle_tasks_vec.push(handle_tasks);

                    // Spawn task to collect batches from this handle
                    let collect_task = tokio::spawn(async move {
                        let mut batches = Vec::new();
                        let mut receiver = receiver;
                        while let Some(batch) = receiver.recv().await {
                            batches.push((batch, Arc::clone(&schema)));
                        }
                        batches
                    });
                    collect_tasks.push(collect_task);
                }

                // Wait for all collection tasks to complete
                for task in collect_tasks {
                    match task.await {
                        Ok(mut batches) => {
                            all_batches.append(&mut batches);
                        }
                        Err(e) => {
                            return Err(format!(
                                "Failed to collect batches for {}: {}",
                                event_type_clone, e
                            ));
                        }
                    }
                }

                // Wait for handle tasks to complete to ensure all batches were sent
                // This ensures the streaming pipeline has finished sending all data
                for handle_tasks in handle_tasks_vec {
                    for handle_task in handle_tasks {
                        let _ = handle_task.await;
                    }
                }

                Ok((event_type_clone, all_batches))
            });
            tasks.push(task);
        }

        // Wait for all event types to be processed
        let mut zones_by_type: HashMap<String, Vec<CandidateZone>> = HashMap::new();
        for task in tasks {
            match task.await {
                Ok(Ok((event_type, batches))) => {
                    if !batches.is_empty() {
                        let zones = self.batches_to_zones(batches, &event_type)?;
                        zones_by_type.insert(event_type, zones);
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(format!("Task join error: {}", e)),
            }
        }

        Ok(zones_by_type)
    }

    /// Converts batches to CandidateZones with columnar data format.
    ///
    /// This is optimized to work directly with columnar data without materializing rows.
    /// It merges multiple batches into a single zone per event type.
    fn batches_to_zones(
        &self,
        batches: Vec<(Arc<ColumnBatch>, Arc<BatchSchema>)>,
        event_type: &str,
    ) -> Result<Vec<CandidateZone>, String> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // Collect all field names from all batches
        let mut field_names = std::collections::HashSet::new();
        field_names.insert("context_id".to_string());
        field_names.insert("timestamp".to_string());
        field_names.insert("event_type".to_string());

        for (batch, _schema) in &batches {
            for column in batch.schema().columns() {
                field_names.insert(column.name.clone());
            }
        }

        // Calculate total row count across all batches
        let total_rows: usize = batches.iter().map(|(batch, _)| batch.len()).sum();
        if total_rows == 0 {
            return Ok(Vec::new());
        }

        // Build columnar data for each field by merging columns from all batches
        let mut column_values_map: HashMap<String, ColumnValues> = HashMap::new();

        // OPTIMIZATION: Pre-build column index maps for each batch to avoid repeated lookups
        let mut batch_column_indices: Vec<HashMap<String, usize>> =
            Vec::with_capacity(batches.len());
        for (_batch, schema) in &batches {
            let mut column_map = HashMap::new();
            for (idx, col) in schema.columns().iter().enumerate() {
                column_map.insert(col.name.clone(), idx);
            }
            batch_column_indices.push(column_map);
        }

        // Determine if field is a timestamp/datetime type by checking schema
        // Check first batch that has this field to determine type
        let mut field_is_timestamp: HashMap<String, bool> = HashMap::new();
        for field_name in &field_names {
            for (idx, (_, schema)) in batches.iter().enumerate() {
                if let Some(col_idx) = batch_column_indices[idx].get(field_name) {
                    if let Some(col_spec) = schema.columns().get(*col_idx) {
                        let is_timestamp = col_spec.logical_type == "Timestamp"
                            || col_spec.logical_type == "datetime"
                            || field_name == "timestamp"
                            || field_name == &self.sequence_time_field;
                        field_is_timestamp.insert(field_name.clone(), is_timestamp);
                        break;
                    }
                }
            }
        }

        for field_name in &field_names {
            let is_timestamp = field_is_timestamp.get(field_name).copied().unwrap_or(false);

            if is_timestamp {
                // For timestamp fields, store as typed i64 for efficient access
                let mut i64_values: Vec<i64> = Vec::with_capacity(total_rows);
                let mut null_bitmap: Vec<bool> = Vec::with_capacity(total_rows);

                // Process all batches for this field
                for (batch_idx, (batch, _schema)) in batches.iter().enumerate() {
                    // Use pre-built column index map
                    if let Some(&col_idx) = batch_column_indices[batch_idx].get(field_name) {
                        // Get column values directly from batch (columnar access)
                        let column_values = batch
                            .columns_ref()
                            .get(col_idx)
                            .ok_or_else(|| format!("Column index {} out of bounds", col_idx))?;

                        // Extract i64 values directly from ScalarValue
                        for value in column_values.iter() {
                            match value {
                                ScalarValue::Timestamp(ts) => {
                                    i64_values.push(*ts);
                                    null_bitmap.push(false);
                                }
                                ScalarValue::Int64(i) => {
                                    i64_values.push(*i);
                                    null_bitmap.push(false);
                                }
                                ScalarValue::Null => {
                                    i64_values.push(0); // Placeholder for null
                                    null_bitmap.push(true);
                                }
                                _ => {
                                    // Try to parse as i64 from string representation
                                    if let Some(ts) = value.as_i64() {
                                        i64_values.push(ts);
                                        null_bitmap.push(false);
                                    } else {
                                        i64_values.push(0);
                                        null_bitmap.push(true);
                                    }
                                }
                            }
                        }
                    } else {
                        // Field not in this batch - fill with null (0) values
                        for _ in 0..batch.len() {
                            i64_values.push(0);
                            null_bitmap.push(true);
                        }
                    }
                }

                // Create typed i64 ColumnValues
                let mut bytes: Vec<u8> = Vec::with_capacity(total_rows * 8 + (total_rows + 7) / 8);
                let payload_start = 0;
                for &val in &i64_values {
                    bytes.extend_from_slice(&val.to_le_bytes());
                }
                let row_count = i64_values.len();

                // Create nulls bitmap
                let nulls = if null_bitmap.iter().any(|&is_null| is_null) {
                    let null_bytes = (row_count + 7) / 8;
                    let nulls_start = bytes.len();
                    let mut nulls_bytes = vec![0u8; null_bytes];
                    for (idx, &is_null) in null_bitmap.iter().enumerate() {
                        if is_null {
                            let byte_idx = idx / 8;
                            let bit_idx = idx % 8;
                            nulls_bytes[byte_idx] |= 1 << bit_idx;
                        }
                    }
                    bytes.extend_from_slice(&nulls_bytes);
                    Some((nulls_start, null_bytes))
                } else {
                    None
                };

                let block = Arc::new(DecompressedBlock::from_bytes(bytes));
                let column_values =
                    ColumnValues::new_typed_i64(block, payload_start, row_count, nulls);
                column_values_map.insert(field_name.clone(), column_values);
            } else {
                // For non-timestamp fields, use string-based storage
                let mut bytes: Vec<u8> = Vec::new();
                let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(total_rows);

                // Process all batches for this field
                for (batch_idx, (batch, _schema)) in batches.iter().enumerate() {
                    // Use pre-built column index map
                    if let Some(&col_idx) = batch_column_indices[batch_idx].get(field_name) {
                        // Get column values directly from batch (columnar access)
                        let column_values = batch
                            .columns_ref()
                            .get(col_idx)
                            .ok_or_else(|| format!("Column index {} out of bounds", col_idx))?;

                        // Convert ScalarValues directly to bytes without row materialization
                        for value in column_values.iter() {
                            let value_str = scalar_to_string(value);
                            let start = bytes.len();
                            bytes.extend_from_slice(value_str.as_bytes());
                            let len = value_str.len();
                            ranges.push((start, len));
                        }
                    } else {
                        // Field not in this batch - fill with default/null values
                        let default_value = match field_name.as_str() {
                            "event_type" => ScalarValue::Utf8(event_type.to_string()),
                            _ => ScalarValue::Null,
                        };
                        let value_str = scalar_to_string(&default_value);
                        for _ in 0..batch.len() {
                            let start = bytes.len();
                            bytes.extend_from_slice(value_str.as_bytes());
                            let len = value_str.len();
                            ranges.push((start, len));
                        }
                    }
                }

                let block = Arc::new(DecompressedBlock::from_bytes(bytes));
                let column_values = ColumnValues::new(block, ranges);
                column_values_map.insert(field_name.clone(), column_values);
            }
        }

        // Create a single zone with all rows (zone_id 0, special segment_id for streaming)
        let segment_id = format!("streaming_{}", event_type);
        let mut zone = CandidateZone::new(0, segment_id);
        zone.set_values(column_values_map);

        Ok(vec![zone])
    }

    /// Creates a result stream from matched events.
    async fn create_result_stream(&self, events: Vec<Event>) -> Result<QueryBatchStream, String> {
        info!(
            target: "sneldb::sequence::streaming::merger",
            event_count = events.len(),
            "Creating result stream from matched events"
        );

        if events.is_empty() {
            debug!(
                target: "sneldb::sequence::streaming::merger",
                "No events to stream, returning empty stream"
            );
            // Return empty stream with a basic schema
            let schema = Arc::new(
                BatchSchema::new(vec![
                    ColumnSpec {
                        name: "event_type".to_string(),
                        logical_type: "String".to_string(),
                    },
                    ColumnSpec {
                        name: "context_id".to_string(),
                        logical_type: "String".to_string(),
                    },
                    ColumnSpec {
                        name: "timestamp".to_string(),
                        logical_type: "Timestamp".to_string(),
                    },
                ])
                .map_err(|e| format!("Failed to create schema: {}", e))?,
            );
            let capacity = 1024;
            let metrics = FlowMetrics::new();
            let (_tx, rx) = FlowChannel::bounded(capacity, metrics);
            return Ok(QueryBatchStream::new(schema, rx, Vec::new()));
        }

        // Log event details for debugging
        if tracing::enabled!(tracing::Level::DEBUG) {
            for (idx, event) in events.iter().enumerate() {
                debug!(
                    target: "sneldb::sequence::streaming::merger",
                    event_idx = idx,
                    event_type = %event.event_type,
                    context_id = %event.context_id,
                    timestamp = event.timestamp,
                    payload_fields = ?event.payload.keys().map(|k| k.as_ref()).collect::<Vec<_>>(),
                    "Event details"
                );
            }
        }

        // Infer schema from first event
        let first_event = &events[0];
        let mut columns = vec![
            ColumnSpec {
                name: "event_type".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Timestamp".to_string(),
            },
        ];

        // Add payload fields
        for (field, _value) in &first_event.payload {
            columns.push(ColumnSpec {
                name: field.as_ref().to_string(),
                logical_type: "String".to_string(), // Simplified - would infer actual type
            });
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::sequence::streaming::merger",
                column_count = columns.len(),
                columns = ?columns.iter().map(|c| &c.name).collect::<Vec<_>>(),
                "Created schema for result stream"
            );
        }

        let schema = Arc::new(
            BatchSchema::new(columns).map_err(|e| format!("Failed to create schema: {}", e))?,
        );

        // Create channel
        let capacity = 1024;
        let metrics = FlowMetrics::new();
        let (tx, rx) = FlowChannel::bounded(capacity, metrics);

        // Create batch pool
        let pool =
            BatchPool::new(1024).map_err(|e| format!("Failed to create batch pool: {}", e))?;

        // Spawn task to send events as batches
        let schema_clone = Arc::clone(&schema);
        let task = tokio::spawn(async move {
            let mut builder_opt = Some(pool.acquire(Arc::clone(&schema_clone)));
            let mut events_processed = 0;
            let mut batches_sent = 0;

            debug!(
                target: "sneldb::sequence::streaming::merger",
                total_events = events.len(),
                "Starting to process events into batches"
            );

            for (event_idx, event) in events.into_iter().enumerate() {
                let builder = match builder_opt.as_mut() {
                    Some(b) => b,
                    None => {
                        error!(
                            target: "sneldb::sequence::streaming::merger",
                            event_idx = event_idx,
                            "Builder is None, breaking"
                        );
                        break;
                    }
                };

                // Convert event to row values
                let mut row_values = Vec::new();
                for column in schema_clone.columns() {
                    let value = match column.name.as_str() {
                        "event_type" => ScalarValue::Utf8(event.event_type.clone()),
                        "context_id" => ScalarValue::Utf8(event.context_id.clone()),
                        "timestamp" => ScalarValue::Timestamp(event.timestamp as i64),
                        _ => event
                            .get_field_scalar(&column.name)
                            .unwrap_or(ScalarValue::Null),
                    };
                    row_values.push(value);
                }

                if builder.push_row(&row_values).is_err() {
                    error!(
                        target: "sneldb::sequence::streaming::merger",
                        event_idx = event_idx,
                        "Failed to push row to builder"
                    );
                    break;
                }

                events_processed += 1;

                if builder.is_full() {
                    let batch_size = builder.len();
                    match builder_opt.take().unwrap().finish() {
                        Ok(batch) => {
                            batches_sent += 1;
                            if tracing::enabled!(tracing::Level::DEBUG) {
                                debug!(
                                    target: "sneldb::sequence::streaming::merger",
                                    batch_idx = batches_sent,
                                    batch_size = batch_size,
                                    events_processed = events_processed,
                                    "Sending full batch"
                                );
                            }
                            if tx.send(Arc::new(batch)).await.is_err() {
                                error!(
                                    target: "sneldb::sequence::streaming::merger",
                                    "Failed to send batch, channel closed"
                                );
                                break;
                            }
                            builder_opt = Some(pool.acquire(Arc::clone(&schema_clone)));
                        }
                        Err(e) => {
                            error!(
                                target: "sneldb::sequence::streaming::merger",
                                error = %e,
                                "Failed to finish batch"
                            );
                            break;
                        }
                    }
                }
            }

            // Send final batch if there's remaining data
            if let Some(builder) = builder_opt {
                let final_batch_size = builder.len();
                if final_batch_size > 0 {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        debug!(
                            target: "sneldb::sequence::streaming::merger",
                            final_batch_size = final_batch_size,
                            events_processed = events_processed,
                            "Sending final batch"
                        );
                    }
                    if let Ok(batch) = builder.finish() {
                        batches_sent += 1;
                        if tx.send(Arc::new(batch)).await.is_err() {
                            error!(
                                target: "sneldb::sequence::streaming::merger",
                                "Failed to send final batch, channel closed"
                            );
                        } else if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(
                                target: "sneldb::sequence::streaming::merger",
                                "Successfully sent final batch"
                            );
                        }
                    } else {
                        error!(
                            target: "sneldb::sequence::streaming::merger",
                            "Failed to finish final batch"
                        );
                    }
                } else if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        target: "sneldb::sequence::streaming::merger",
                        "Final batch is empty, not sending"
                    );
                }
            } else if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    target: "sneldb::sequence::streaming::merger",
                    "No builder available for final batch"
                );
            }

            info!(
                target: "sneldb::sequence::streaming::merger",
                events_processed = events_processed,
                batches_sent = batches_sent,
                "Completed processing events into batches"
            );
        });

        Ok(QueryBatchStream::new(schema, rx, vec![task]))
    }
}
