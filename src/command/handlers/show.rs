use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::command::handlers::query::QueryExecutionPipeline;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::Command;
use crate::engine::materialize::{
    HighWaterMark, MaterializationCatalog, MaterializedQuerySpecExt, MaterializedSink,
    MaterializedStore, schema_to_batch_schema,
};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use crate::shared::path::absolutize;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

use crate::engine::core::read::flow::{ColumnBatch, FlowChannel, FlowMetrics};

use serde_json::Value as JsonValue;

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::ShowMaterialized { name } = cmd else {
        let resp = Response::error(StatusCode::BadRequest, "Invalid SHOW command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    match show_materialized(name, shard_manager, registry, writer, renderer).await {
        Ok(()) => Ok(()),
        Err(message) => {
            let resp = Response::error(StatusCode::InternalError, &message);
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

async fn show_materialized<W: AsyncWrite + Unpin>(
    name: &str,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> Result<(), String> {
    let mut catalog = load_catalog()?;
    let mut entry = catalog
        .get(name)
        .cloned()
        .ok_or_else(|| format!("Materialization '{}' not found", name))?;

    if entry.schema.is_empty() {
        return Err(format!(
            "Materialization '{}' does not have a stored schema",
            name
        ));
    }

    let store_path = entry.storage_path.clone();
    let read_store = MaterializedStore::open(&store_path)
        .map_err(|e| format!("Failed to open materialized store: {e}"))?;
    let stored_frames = read_store.frames().to_vec();
    drop(read_store);

    let schema = schema_to_batch_schema(&entry.schema)
        .map_err(|e| format!("Failed to build batch schema: {e}"))?;
    let schema_arc = Arc::new(schema);

    let timestamp_column = match entry.spec.query() {
        Command::Query { time_field, .. } => {
            time_field.as_deref().unwrap_or("timestamp").to_string()
        }
        _ => "timestamp".to_string(),
    };

    let timestamp_idx = schema_arc
        .columns()
        .iter()
        .position(|column| column.name == timestamp_column);
    let event_idx = schema_arc
        .columns()
        .iter()
        .position(|column| column.name == "event_id");

    let sink_store = MaterializedStore::open(&store_path)
        .map_err(|e| format!("Failed to open sink store: {e}"))?;
    let mut base_sink = MaterializedSink::new(sink_store, entry.schema.clone())
        .map_err(|e| format!("Failed to create materialized sink: {e}"))?;
    if let Some(policy) = entry.retention.clone() {
        base_sink.set_retention_policy(policy);
    }
    let initial_high_water = base_sink.high_water_mark();
    let sink = Arc::new(Mutex::new(base_sink));

    let batch_size = 32768;
    let metrics = FlowMetrics::new();
    let (sender, receiver) = FlowChannel::bounded(batch_size, metrics);

    let mut tasks: Vec<JoinHandle<()>> = Vec::new();

    if !stored_frames.is_empty() {
        let sender_clone = sender.clone();
        let store_path_clone = store_path.clone();
        tasks.push(tokio::spawn(async move {
            match MaterializedStore::open(&store_path_clone) {
                Ok(store) => {
                    for meta in stored_frames {
                        match store.read_frame(&meta) {
                            Ok(batch) => {
                                if sender_clone.send(batch).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                tracing::error!(target: "sneldb::show", error = %err, "Failed to read stored frame");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(target: "sneldb::show", error = %err, "Failed to open store for streaming");
                }
            }
        }));
    }

    let delta_command = entry
        .spec
        .delta_command(entry.high_water_mark)
        .map_err(|e| format!("Failed to build delta command: {e}"))?;

    let delta_pipeline =
        QueryExecutionPipeline::new(&delta_command, shard_manager, Arc::clone(registry));
    let delta_stream = delta_pipeline
        .execute_streaming()
        .await
        .map_err(|e| format!("Failed to execute delta pipeline: {e}"))?;

    if let Some(mut stream) = delta_stream {
        let sender_clone = sender.clone();
        let sink_clone = Arc::clone(&sink);
        let initial_high_water = initial_high_water;
        let timestamp_idx = timestamp_idx;
        let event_idx = event_idx;
        tasks.push(tokio::spawn(async move {
            let mut watermark = initial_high_water;
            while let Some(batch) = stream.recv().await {
                let batch = if let (Some(ts_idx), Some(ev_idx)) = (timestamp_idx, event_idx) {
                    match filter_delta_batch(batch, ts_idx, ev_idx, &mut watermark) {
                        Some(filtered) => filtered,
                        None => continue,
                    }
                } else {
                    batch
                };

                if let Err(err) = sink_clone.lock().await.append(&batch) {
                    tracing::error!(target: "sneldb::show", error = %err, "Failed to append delta batch");
                    continue;
                }
                if sender_clone.send(batch).await.is_err() {
                    break;
                }
            }
        }));
    }

    drop(sender);

    let stream = QueryBatchStream::new(schema_arc.clone(), receiver, tasks);
    stream_to_writer(stream, writer, renderer, None, None)
        .await
        .map_err(|e| e.to_string())?;

    let sink = Arc::try_unwrap(sink)
        .map_err(|_| "Materialized sink still in use".to_string())?
        .into_inner();

    let high_water = sink.high_water_mark();
    entry.schema = sink.schema().to_vec();
    entry.row_count = sink.total_rows();
    entry.delta_rows_appended = sink.last_rows_appended();
    entry.byte_size = sink.total_bytes();
    entry.delta_bytes_appended = sink.last_bytes_appended();
    entry.high_water_mark = if high_water.is_zero() {
        entry.high_water_mark
    } else {
        Some(high_water)
    };
    entry.touch();

    let high_water_age = entry
        .high_water_mark
        .map(|hw| current_timestamp().saturating_sub(hw.timestamp));

    tracing::info!(
        target: "sneldb::show",
        alias = name,
        rows = entry.row_count,
        delta_rows = entry.delta_rows_appended,
        bytes = entry.byte_size,
        delta_bytes = entry.delta_bytes_appended,
        high_water_age = high_water_age.unwrap_or(0),
        "Materialized view refreshed"
    );

    catalog
        .upsert(entry)
        .map_err(|e| format!("Failed to update catalog: {e}"))?;

    Ok(())
}

fn load_catalog() -> Result<MaterializationCatalog, String> {
    let data_dir = absolutize(PathBuf::from(CONFIG.engine.data_dir.as_str()));
    MaterializationCatalog::load(&data_dir)
        .map_err(|e| format!("Failed to load materialization catalog: {e}"))
}

async fn stream_to_writer<W: AsyncWrite + Unpin>(
    mut stream: QueryBatchStream,
    writer: &mut W,
    renderer: &dyn Renderer,
    limit: Option<u32>,
    offset: Option<u32>,
) -> std::io::Result<()> {
    let mut writer = BufWriter::with_capacity(65536, writer);
    let schema = stream.schema();
    let column_metadata: Vec<(String, String)> = schema
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.logical_type.clone()))
        .collect();

    let column_names: Vec<&str> = column_metadata
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();

    let mut encode_buf = Vec::with_capacity(4096);
    renderer.stream_schema(&column_metadata, &mut encode_buf);
    writer.write_all(&encode_buf).await?;
    encode_buf.clear();

    let event_id_idx = column_names.iter().position(|&name| name == "event_id");

    let mut seen_ids: HashSet<u64> = HashSet::new();
    let mut emitted: usize = 0;
    let mut skipped: usize = 0;
    let limit = limit.map(|value| value as usize);
    let offset = offset.map(|value| value as usize);
    let mut done = false;

    while !done {
        match stream.recv().await {
            Some(batch) => {
                if batch.is_empty() {
                    continue;
                }

                let column_views: Vec<&[JsonValue]> = batch
                    .columns()
                    .map(|column| column as &[JsonValue])
                    .collect();

                let mut row_refs: Vec<&JsonValue> = Vec::with_capacity(column_names.len());

                for row_idx in 0..batch.len() {
                    if let Some(idx) = event_id_idx {
                        if let Some(id) = column_views[idx][row_idx].as_u64() {
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

                    if let Some(limit) = limit {
                        if emitted >= limit {
                            done = true;
                            break;
                        }
                    }

                    for col_idx in 0..column_names.len() {
                        row_refs.push(&column_views[col_idx][row_idx]);
                    }

                    renderer.stream_row(&column_names, &row_refs, &mut encode_buf);
                    writer.write_all(&encode_buf).await?;
                    encode_buf.clear();
                    row_refs.clear();
                    emitted += 1;
                }
            }
            None => break,
        }
    }

    renderer.stream_end(emitted, &mut encode_buf);
    writer.write_all(&encode_buf).await?;
    encode_buf.clear();
    writer.flush().await
}

fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn filter_delta_batch(
    batch: ColumnBatch,
    timestamp_idx: usize,
    event_idx: usize,
    watermark: &mut HighWaterMark,
) -> Option<ColumnBatch> {
    let len = batch.len();
    if len == 0 {
        return None;
    }

    let timestamps = match batch.column(timestamp_idx) {
        Ok(values) => values,
        Err(err) => {
            tracing::error!(
                target: "sneldb::show",
                error = %err,
                "Delta batch missing timestamp column"
            );
            return Some(batch);
        }
    };

    let events = match batch.column(event_idx) {
        Ok(values) => values,
        Err(err) => {
            tracing::error!(
                target: "sneldb::show",
                error = %err,
                "Delta batch missing event_id column"
            );
            return Some(batch);
        }
    };

    let mut keep = Vec::with_capacity(len);
    for row_idx in 0..len {
        let ts = parse_u64(&timestamps[row_idx]);
        let event = parse_u64(&events[row_idx]);

        if let (Some(ts), Some(event)) = (ts, event) {
            if (ts, event) > (watermark.timestamp, watermark.event_id) {
                keep.push(row_idx);
            }
        } else {
            keep.push(row_idx);
        }
    }

    if keep.is_empty() {
        return None;
    }

    for &idx in &keep {
        if let (Some(ts), Some(event)) = (parse_u64(&timestamps[idx]), parse_u64(&events[idx])) {
            watermark.advance(ts, event);
        }
    }

    if keep.len() == len {
        return Some(batch);
    }

    let (schema, columns) = batch.detach();
    let mut filtered_columns = Vec::with_capacity(columns.len());
    for column in columns {
        let mut filtered = Vec::with_capacity(keep.len());
        for &idx in &keep {
            filtered.push(column[idx].clone());
        }
        filtered_columns.push(filtered);
    }

    match ColumnBatch::new(schema, filtered_columns, keep.len(), None) {
        Ok(filtered) => Some(filtered),
        Err(err) => {
            tracing::error!(
                target: "sneldb::show",
                error = %err,
                "Failed to build filtered delta batch"
            );
            None
        }
    }
}

fn parse_u64(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(number) => number.as_u64(),
        JsonValue::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}
