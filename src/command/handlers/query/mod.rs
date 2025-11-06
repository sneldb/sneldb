mod context;
mod dispatch;
mod merge;
mod orchestrator;
mod planner;

#[cfg(test)]
mod context_test;
#[cfg(test)]
mod orchestrator_test;

pub use orchestrator::QueryExecutionPipeline;

use super::query_batch_stream::QueryBatchStream;
use crate::command::types::Command;
use crate::engine::core::read::result::QueryResult;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use crate::shared::response::ArrowStreamEncoder;
use crate::shared::response::render::{Renderer, StreamingFormat};
use crate::shared::response::{Response, StatusCode};
use arrow_array::Array;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::RwLock;
use tracing::{debug, warn};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::Query {
        event_type,
        context_id,
        since,
        where_clause,
        limit,
        offset,
        ..
    } = cmd
    else {
        warn!(target: "sneldb::query", "Invalid Query command received");
        let resp = Response::error(StatusCode::BadRequest, "Invalid Query command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    if event_type.trim().is_empty() {
        warn!(target: "sneldb::query", "Empty event_type in Query command");
        let resp = Response::error(StatusCode::BadRequest, "event_type cannot be empty");
        return writer.write_all(&renderer.render(&resp)).await;
    }

    if offset.is_some() && limit.is_none() {
        warn!(target: "sneldb::query", "OFFSET specified without LIMIT");
        let resp = Response::error(
            StatusCode::BadRequest,
            "OFFSET requires LIMIT to prevent unbounded results",
        );
        return writer.write_all(&renderer.render(&resp)).await;
    }

    debug!(
        target: "sneldb::query",
        event_type,
        context_id = context_id.as_deref().unwrap_or("<none>"),
        since = ?since,
        limit = ?limit,
        has_filter = where_clause.is_some(),
        "Dispatching Query command to pipeline"
    );

    let pipeline = QueryExecutionPipeline::new(cmd, shard_manager, Arc::clone(registry));

    let limit_value = *limit;
    let offset_value = *offset;

    if streaming_enabled() && pipeline.streaming_supported() {
        match pipeline.execute_streaming().await {
            Ok(Some(stream)) => {
                return write_streaming_response(
                    stream,
                    writer,
                    renderer,
                    limit_value,
                    offset_value,
                )
                .await;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    target: "sneldb::query",
                    error = %error,
                    "Streaming execution failed, falling back to buffered"
                );
            }
        }
    }

    match pipeline.execute().await {
        Ok(result) => format_and_write_result(result, writer, renderer).await,
        Err(error) => {
            warn!(target: "sneldb::query", error = %error, "Query execution failed");
            let resp = Response::error(
                StatusCode::InternalError,
                &format!("Query failed: {}", error),
            );
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

async fn format_and_write_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    match result {
        QueryResult::Selection(selection) => {
            let table = selection.finalize();

            if table.rows.is_empty() {
                let resp = Response::ok_lines(vec!["No matching events found".to_string()]);
                return writer.write_all(&renderer.render(&resp)).await;
            }

            let columns = table
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.logical_type.clone()))
                .collect::<Vec<(String, String)>>();

            let rows: Vec<Vec<serde_json::Value>> = table
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(|v| v.to_json()).collect())
                .collect();
            let count = rows.len();

            let resp = Response::ok_table(columns, rows, count);
            writer.write_all(&renderer.render(&resp)).await
        }
        QueryResult::Aggregation(aggregation) => {
            let table = aggregation.finalize();

            let columns = table
                .columns
                .iter()
                .map(|column| (column.name.clone(), column.logical_type.clone()))
                .collect::<Vec<(String, String)>>();

            let rows: Vec<Vec<serde_json::Value>> = table
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(|v| v.to_json()).collect())
                .collect();
            let count = rows.len();

            let resp = Response::ok_table(columns, rows, count);
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

async fn write_streaming_response<W: AsyncWrite + Unpin>(
    mut stream: QueryBatchStream,
    writer: &mut W,
    renderer: &dyn Renderer,
    limit: Option<u32>,
    offset: Option<u32>,
) -> std::io::Result<()> {
    let mut writer = BufWriter::with_capacity(65536, writer); // Increased from 8KB to 64KB
    let schema = stream.schema();
    let column_metadata: Vec<(String, String)> = schema
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.logical_type.clone()))
        .collect();
    // Cache column names as &str slices to avoid String cloning
    let column_names: Vec<&str> = column_metadata
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();

    // Get batch size from config (default 1000, 0 = per-row mode)
    let batch_size = CONFIG
        .query
        .as_ref()
        .and_then(|cfg| cfg.streaming_batch_size)
        .unwrap_or(1000);

    let mut encode_buf = Vec::with_capacity(if batch_size > 0 { 65536 } else { 4096 });
    let mut arrow_encoder = if renderer.streaming_format() == StreamingFormat::Arrow {
        Some(ArrowStreamEncoder::new(&schema).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to initialize Arrow encoder: {err}"),
            )
        })?)
    } else {
        None
    };

    match renderer.streaming_format() {
        StreamingFormat::Json => {
            renderer.stream_schema_json(&column_metadata, &mut encode_buf);
            writer.write_all(&encode_buf).await?;
            encode_buf.clear();
        }
        StreamingFormat::Arrow => {
            let encoder = arrow_encoder
                .as_mut()
                .expect("arrow encoder should exist for arrow format");
            encoder.write_schema(&mut encode_buf).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to encode Arrow schema: {err}"),
                )
            })?;
            write_with_backpressure(writer.get_mut(), &encode_buf).await?;
            encode_buf.clear();
        }
    }

    let event_id_idx = column_names.iter().position(|&name| name == "event_id");

    let mut seen_ids: HashSet<u64> = HashSet::new();
    let mut emitted: usize = 0;
    let mut skipped: usize = 0;
    let limit = limit.map(|value| value as usize);
    let offset = offset.map(|value| value as usize);
    let mut done = false;

    // Zero-copy approach: serialize directly from Arc<ColumnBatch> references while batch is alive
    // This avoids cloning all Values (6.98% overhead in flamegraph)
    // Serialize immediately per-batch using references - eliminates all cloning!
    // Note: We can't accumulate references across batches (lifetime issue), so we serialize per-batch

    while !done {
        match stream.recv().await {
            Some(batch_arc) => {
                if batch_arc.is_empty() {
                    continue;
                }

                // Collect row indices that pass filters
                // For Arrow format, filter directly on Arrow arrays (zero JSON conversion!)
                let mut valid_row_indices: Vec<usize> = Vec::new();

                match renderer.streaming_format() {
                    StreamingFormat::Arrow => {
                        // Filter directly on Arrow arrays - no JSON conversion!
                        let record_batch = batch_arc.record_batch();

                        for row_idx in 0..batch_arc.len() {
                            // Check event_id deduplication if needed (only convert that one column if necessary)
                            if let Some(idx) = event_id_idx {
                                let event_id_array = record_batch.column(idx);
                                // Extract event_id value directly from Arrow array (zero JSON conversion!)
                                let id = if let Some(int_array) = event_id_array
                                    .as_any()
                                    .downcast_ref::<arrow_array::Int64Array>(
                                ) {
                                    if !int_array.is_null(row_idx) {
                                        Some(int_array.value(row_idx) as u64)
                                    } else {
                                        None
                                    }
                                } else if let Some(uint_array) = event_id_array
                                    .as_any()
                                    .downcast_ref::<arrow_array::UInt64Array>(
                                ) {
                                    if !uint_array.is_null(row_idx) {
                                        Some(uint_array.value(row_idx))
                                    } else {
                                        None
                                    }
                                } else {
                                    // Fallback: convert only this column to JSON (rare case)
                                    match batch_arc.column(idx) {
                                        Ok(event_id_col) => {
                                            event_id_col.get(row_idx).and_then(|v| v.as_u64())
                                        }
                                        Err(_) => None,
                                    }
                                };

                                if let Some(id) = id {
                                    if !seen_ids.insert(id) {
                                        continue;
                                    }
                                }
                            }

                            if let Some(off) = offset {
                                if skipped < off {
                                    skipped += 1;
                                    continue;
                                }
                            }

                            if let Some(lim) = limit {
                                if emitted >= lim {
                                    done = true;
                                    break;
                                }
                            }

                            valid_row_indices.push(row_idx);
                            emitted += 1;
                        }
                    }
                    StreamingFormat::Json => {
                        // For JSON format, convert ScalarValue to JsonValue for serialization
                        let columns = batch_arc.columns();
                        let column_views: Vec<Vec<JsonValue>> = columns
                            .iter()
                            .map(|column| column.iter().map(|v| v.to_json()).collect())
                            .collect();

                        for row_idx in 0..batch_arc.len() {
                            if let Some(idx) = event_id_idx {
                                if let Some(id) =
                                    column_views[idx].get(row_idx).and_then(|v| v.as_u64())
                                {
                                    if !seen_ids.insert(id) {
                                        continue;
                                    }
                                }
                            }

                            if let Some(off) = offset {
                                if skipped < off {
                                    skipped += 1;
                                    continue;
                                }
                            }

                            if let Some(lim) = limit {
                                if emitted >= lim {
                                    done = true;
                                    break;
                                }
                            }

                            valid_row_indices.push(row_idx);
                            emitted += 1;
                        }

                        // Serialize JSON immediately (column_views is in scope here)
                        if batch_size > 0 && valid_row_indices.len() > 0 {
                            let batch_rows: Vec<Vec<&JsonValue>> = valid_row_indices
                                .iter()
                                .map(|&row_idx| {
                                    (0..column_names.len())
                                        .map(|col_idx| &column_views[col_idx][row_idx])
                                        .collect()
                                })
                                .collect();
                            renderer.stream_batch_json(&column_names, &batch_rows, &mut encode_buf);
                            writer.write_all(&encode_buf).await?;
                            encode_buf.clear();
                        } else {
                            for &row_idx in &valid_row_indices {
                                let row_refs: Vec<&JsonValue> = (0..column_names.len())
                                    .map(|col_idx| &column_views[col_idx][row_idx])
                                    .collect();
                                renderer.stream_row_json(&column_names, &row_refs, &mut encode_buf);
                                writer.write_all(&encode_buf).await?;
                                encode_buf.clear();
                            }
                        }
                    }
                }

                // For Arrow format, serialize now (valid_row_indices computed above)
                if renderer.streaming_format() == StreamingFormat::Arrow {
                    let encoder = arrow_encoder
                        .as_mut()
                        .expect("arrow encoder should exist for arrow format");

                    // Optimize: If all rows are included (no filtering), pass None to avoid expensive slicing
                    let row_indices_opt = if valid_row_indices.len() == batch_arc.len() {
                        // Check if valid_row_indices is contiguous [0, 1, 2, ..., len-1]
                        let is_contiguous = valid_row_indices
                            .iter()
                            .enumerate()
                            .all(|(i, &idx)| idx == i);
                        if is_contiguous {
                            None // Fast path: use RecordBatch reference directly (no slicing!)
                        } else {
                            Some(&valid_row_indices[..])
                        }
                    } else {
                        Some(&valid_row_indices[..])
                    };

                    encoder
                        .write_batch(
                            &schema,
                            batch_arc.as_ref(),
                            row_indices_opt,
                            &mut encode_buf,
                        )
                        .map_err(|err| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Failed to encode Arrow batch: {err}"),
                            )
                        })?;
                    // Write with flushing to handle backpressure
                    write_with_backpressure(writer.get_mut(), &encode_buf).await?;
                    encode_buf.clear();
                }
            }
            None => break,
        }
    }

    match renderer.streaming_format() {
        StreamingFormat::Json => {
            renderer.stream_end_json(emitted, &mut encode_buf);
            writer.write_all(&encode_buf).await?;
            encode_buf.clear();
        }
        StreamingFormat::Arrow => {
            if let Some(encoder) = arrow_encoder.as_mut() {
                encoder.write_end(&mut encode_buf).map_err(|err| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to finalize Arrow stream: {err}"),
                    )
                })?;
                write_with_backpressure(writer.get_mut(), &encode_buf).await?;
                encode_buf.clear();
            }
        }
    }
    writer.flush().await
}

/// Write data with backpressure handling - flushes after write to prevent buffer overflow
async fn write_with_backpressure<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> std::io::Result<()> {
    // Write in chunks to avoid overwhelming the socket buffer
    const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks

    if data.len() <= CHUNK_SIZE {
        writer.write_all(data).await?;
        writer.flush().await?;
    } else {
        for chunk in data.chunks(CHUNK_SIZE) {
            writer.write_all(chunk).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

fn streaming_enabled() -> bool {
    CONFIG
        .query
        .as_ref()
        .and_then(|cfg| cfg.streaming_enabled)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
    use crate::shared::response::JsonRenderer;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, duplex};

    fn build_schema() -> Arc<BatchSchema> {
        Arc::new(
            BatchSchema::new(vec![
                crate::engine::core::read::result::ColumnSpec {
                    name: "context_id".to_string(),
                    logical_type: "String".to_string(),
                },
                crate::engine::core::read::result::ColumnSpec {
                    name: "event_id".to_string(),
                    logical_type: "Number".to_string(),
                },
            ])
            .expect("schema should build"),
        )
    }

    #[tokio::test]
    async fn streaming_response_emits_schema_and_rows() {
        let schema = build_schema();
        let metrics = FlowMetrics::new();
        let (sender, receiver) = FlowChannel::bounded(4, Arc::clone(&metrics));

        use crate::engine::types::ScalarValue;
        let mut builder = BatchPool::new(4)
            .expect("pool")
            .acquire(Arc::clone(&schema));
        let row1 = vec![ScalarValue::from(json!("ctx-stream")), ScalarValue::from(json!(42u64))];
        builder.push_row(&row1).expect("push row should succeed");
        let batch = builder.finish().expect("batch finish");
        sender.send(Arc::new(batch)).await.expect("send batch");
        drop(sender);

        let stream = QueryBatchStream::new(Arc::clone(&schema), receiver, Vec::new());
        let (mut writer, mut reader) = duplex(4096);

        let renderer = JsonRenderer;
        write_streaming_response(stream, &mut writer, &renderer, Some(1), None)
            .await
            .expect("streaming write succeeds");
        drop(writer);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.expect("read output");

        let output = String::from_utf8(buf).expect("utf8");
        let mut lines = output.lines();
        let schema_line = lines.next().expect("schema line");
        assert!(schema_line.contains("\"type\":\"schema\""));

        let row_line = lines.next().expect("row line");
        assert!(row_line.contains("ctx-stream"));

        let end_line = lines.next().expect("end line");
        assert!(end_line.contains("\"type\":\"end\""));
        assert!(lines.next().is_none());
    }
}
