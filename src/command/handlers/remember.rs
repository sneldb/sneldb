use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::command::handlers::query::QueryExecutionPipeline;
use crate::command::types::{Command, MaterializedQuerySpec};
use crate::engine::materialize::{
    HighWaterMark, MaterializationCatalog, MaterializationEntry, MaterializedQuerySpecExt,
    MaterializedSink, MaterializedStore, batch_schema_to_snapshots,
};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config::CONFIG;
use crate::shared::path::absolutize;
use crate::shared::response::render::Renderer;
use crate::shared::response::{Response, StatusCode};

pub async fn handle<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    let Command::RememberQuery { spec } = cmd else {
        let resp = Response::error(StatusCode::BadRequest, "Invalid REMEMBER command");
        return writer.write_all(&renderer.render(&resp)).await;
    };

    let spec = spec.clone();

    match remember_query(spec, shard_manager, registry).await {
        Ok(summary) => {
            let resp = Response::ok_lines(summary);
            writer.write_all(&renderer.render(&resp)).await
        }
        Err(message) => {
            let resp = Response::error(StatusCode::InternalError, &message);
            writer.write_all(&renderer.render(&resp)).await
        }
    }
}

async fn remember_query(
    spec: MaterializedQuerySpec,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
) -> Result<Vec<String>, String> {
    let query_command = spec.cloned_query();

    if !matches!(query_command, Command::Query { .. }) {
        return Err("REMEMBER only supports QUERY commands".into());
    }

    let pipeline = QueryExecutionPipeline::new(&query_command, shard_manager, Arc::clone(registry));

    if !pipeline.streaming_supported() {
        return Err("REMEMBER QUERY requires a streaming-compatible SELECT".into());
    }

    let mut stream = pipeline
        .execute_streaming()
        .await
        .map_err(|e| format!("Failed to execute query: {e}"))?
        .ok_or_else(|| "Query cannot be executed in streaming mode".to_string())?;

    let schema_arc = stream.schema();
    let snapshots = batch_schema_to_snapshots(&schema_arc);

    let mut catalog = load_catalog()?;

    if catalog.entries().contains_key(spec.alias()) {
        return Err(format!("Materialization '{}' already exists", spec.alias()));
    }

    let mut entry = MaterializationEntry::new(spec.clone(), catalog.root_dir())
        .map_err(|e| format!("Failed to create catalog entry: {e}"))?;

    let store = MaterializedStore::open(&entry.storage_path)
        .map_err(|e| format!("Failed to open materialized store: {e}"))?;

    let mut sink = MaterializedSink::new(store, snapshots)
        .map_err(|e| format!("Failed to initialize materialized sink: {e}"))?;

    if let Some(policy) = entry.retention.clone() {
        sink.set_retention_policy(policy);
    }

    while let Some(batch) = stream.recv().await {
        sink.append(&batch)
            .map_err(|e| format!("Failed to persist batch: {e}"))?;
    }

    entry.schema = sink.schema().to_vec();
    let high_water = sink.high_water_mark();
    entry.high_water_mark = if high_water.is_zero() {
        None
    } else {
        Some(high_water)
    };
    entry.row_count = sink.total_rows();
    entry.delta_rows_appended = sink.last_rows_appended();
    entry.byte_size = sink.total_bytes();
    entry.delta_bytes_appended = sink.last_bytes_appended();
    entry.touch();

    catalog
        .insert(entry)
        .map_err(|e| format!("Failed to persist catalog: {e}"))?;

    let mut summary = Vec::new();
    summary.push(format!("remembered query '{}'", spec.alias()));
    summary.push(format!("rows stored: {}", sink.total_rows()));
    summary.push(format!("rows appended: {}", sink.last_rows_appended()));
    summary.push(format!("compressed bytes: {}", sink.total_bytes()));
    summary.push(format!(
        "bytes appended: {}",
        sink.last_bytes_appended()
    ));
    if !high_water.is_zero() {
        summary.push(format!(
            "high-water mark: timestamp={} event_id={}",
            high_water.timestamp, high_water.event_id
        ));
        if let Some(age) = high_water_age_seconds(high_water) {
            summary.push(format!("high-water age (s): {}", age));
        }
    }

    tracing::info!(
        target: "sneldb::remember",
        alias = spec.alias(),
        rows = sink.total_rows(),
        appended = sink.last_rows_appended(),
        bytes = sink.total_bytes(),
        appended_bytes = sink.last_bytes_appended(),
        "Materialized query remembered"
    );

    Ok(summary)
}

fn load_catalog() -> Result<MaterializationCatalog, String> {
    let data_dir = absolutize(PathBuf::from(CONFIG.engine.data_dir.as_str()));
    MaterializationCatalog::load(&data_dir)
        .map_err(|e| format!("Failed to load materialization catalog: {e}"))
}

fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn high_water_age_seconds(high_water: HighWaterMark) -> Option<u64> {
    if high_water.is_zero() {
        None
    } else {
        Some(current_timestamp().saturating_sub(high_water.timestamp))
    }
}
