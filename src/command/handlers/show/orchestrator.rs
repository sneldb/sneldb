use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::AsyncWrite;

use crate::command::handlers::query::QueryExecutionPipeline;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::Command;
use crate::engine::core::read::flow::{FlowChannel, FlowMetrics};
use crate::engine::materialize::{
    HighWaterMark, MaterializationEntry, MaterializedQuerySpecExt, MaterializedSink,
};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::shared::config;
use crate::shared::path;
use crate::shared::response::render::Renderer;

use super::catalog::{CatalogGateway, CatalogHandle, FileCatalogGateway};
use super::context::ShowContext;
use super::delta::{DeltaRefresher, SchemaBuilder};
use super::errors::{ShowError, ShowResult};
use super::result::ShowRefreshOutcome;
use super::store::StoredFrameStreamer;
use super::streaming::ShowResponseWriter;
use tracing::debug;

pub struct ShowExecutionPipeline<'a, G: CatalogGateway> {
    context: ShowContext<'a>,
    catalog_gateway: G,
}

impl<'a> ShowExecutionPipeline<'a, FileCatalogGateway> {
    pub fn new(
        alias: &'a str,
        shard_manager: &'a ShardManager,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
    ) -> ShowResult<Self> {
        let data_dir = path::absolutize(std::path::PathBuf::from(
            config::CONFIG.engine.data_dir.as_str(),
        ));
        Self::new_with_data_dir(alias, shard_manager, registry, data_dir)
    }

    pub fn new_with_data_dir(
        alias: &'a str,
        shard_manager: &'a ShardManager,
        registry: Arc<tokio::sync::RwLock<SchemaRegistry>>,
        data_dir: impl AsRef<std::path::Path>,
    ) -> ShowResult<Self> {
        let context = ShowContext::new(alias, shard_manager, registry);
        let catalog_gateway = FileCatalogGateway::new(data_dir);
        Ok(Self {
            context,
            catalog_gateway,
        })
    }
}

#[cfg(test)]
impl<'a, G: CatalogGateway> ShowExecutionPipeline<'a, G> {
    pub(crate) fn new_with_gateway(context: ShowContext<'a>, catalog_gateway: G) -> Self {
        Self {
            context,
            catalog_gateway,
        }
    }

    pub(crate) fn test_ensure_schema_present(
        &self,
        entry: &MaterializationEntry,
    ) -> ShowResult<()> {
        self.ensure_schema_present(entry)
    }

    pub(crate) fn test_timestamp_column(&self, entry: &MaterializationEntry) -> String {
        self.timestamp_column(entry)
    }

    pub(crate) fn test_build_delta_command(
        &self,
        entry: &MaterializationEntry,
    ) -> ShowResult<Command> {
        self.build_delta_command(entry)
    }

    pub(crate) fn test_build_outcome(
        &self,
        entry: MaterializationEntry,
        sink: MaterializedSink,
        initial_high_water: HighWaterMark,
    ) -> ShowRefreshOutcome {
        self.build_outcome(entry, sink, initial_high_water)
    }
}

impl<'a, G: CatalogGateway> ShowExecutionPipeline<'a, G> {
    pub async fn run<W: AsyncWrite + Unpin>(
        self,
        writer: &mut W,
        renderer: &dyn Renderer,
    ) -> ShowResult<()> {
        let show_start = Instant::now();

        let mut catalog_handle = self.catalog_gateway.load()?;
        let entry = catalog_handle.fetch(self.context.alias())?;

        self.ensure_schema_present(&entry)?;
        self.flush_pending_writes().await?;

        let schema = SchemaBuilder::build(&entry)?;
        let timestamp_column = self.timestamp_column(&entry);
        let timestamp_idx = schema
            .columns()
            .iter()
            .position(|column| column.name == timestamp_column);
        let event_idx = schema
            .columns()
            .iter()
            .position(|column| column.name == "event_id");

        let frame_streamer = StoredFrameStreamer::new(entry.storage_path.clone())?;

        let metrics = FlowMetrics::new();
        let (sender, receiver) = FlowChannel::bounded(32_768, Arc::clone(&metrics));
        let mut tasks = Vec::new();

        if let Some(handle) = frame_streamer.spawn_stream_task(sender.clone()) {
            tasks.push(handle);
        }

        let delta_refresher = DeltaRefresher::new(&entry, timestamp_idx, event_idx)?;
        let initial_high_water = delta_refresher.initial_high_water();

        let mut metadata = HashMap::new();
        metadata.insert(
            "materialization_created_at".to_string(),
            entry.created_at.to_string(),
        );

        let delta_command = self.build_delta_command(&entry)?;

        let delta_pipeline = QueryExecutionPipeline::new(
            &delta_command,
            self.context.shard_manager(),
            self.context.registry(),
        )
        .with_metadata(metadata);

        let delta_start = Instant::now();
        tracing::warn!(
            target: "sneldb::show",
            alias = self.context.alias(),
            materialization_created_at = entry.created_at,
            high_water_mark = ?entry.high_water_mark,
            "Starting delta query execution"
        );

        let delta_stream = delta_pipeline
            .execute_streaming()
            .await
            .map_err(|err| ShowError::new(format!("Failed to execute delta pipeline: {err}")))?;

        let delta_setup_time = delta_start.elapsed();
        tracing::warn!(
            target: "sneldb::show",
            alias = self.context.alias(),
            setup_time_ms = delta_setup_time.as_millis(),
            has_stream = delta_stream.is_some(),
            "Delta query setup completed"
        );

        if let Some(stream) = delta_stream {
            let delta_handle =
                delta_refresher.spawn_stream_task(stream, sender.clone(), self.context.alias());
            tasks.push(delta_handle);
        } else {
            tracing::warn!(
                target: "sneldb::show",
                alias = self.context.alias(),
                "Delta stream is None (no streaming support or empty result)"
            );
        }

        drop(sender);

        let stream = QueryBatchStream::new(Arc::clone(&schema), receiver, tasks);
        let response_writer = ShowResponseWriter::new(
            writer,
            renderer,
            Arc::clone(&schema),
            frame_streamer.frame_count(),
            delta_refresher.has_watermark_filtering(),
            None,
            None,
        );
        response_writer.write(stream).await?;

        let sink = delta_refresher.take_sink()?;
        let outcome = self.build_outcome(entry, sink, initial_high_water);
        self.persist_outcome(&mut catalog_handle, outcome)?;

        let total_time = show_start.elapsed();
        tracing::warn!(
            target: "sneldb::show",
            alias = self.context.alias(),
            stored_frames = frame_streamer.frame_count(),
            total_time_ms = total_time.as_millis(),
            "SHOW command completed"
        );

        Ok(())
    }

    async fn flush_pending_writes(&self) -> ShowResult<()> {
        debug!(
            target: "sneldb::show",
            alias = self.context.alias(),
            "Waiting for in-flight shard flushes before SHOW"
        );

        let errors = self
            .context
            .shard_manager()
            .flush_all(self.context.registry())
            .await;

        if errors.is_empty() {
            debug!(
                target: "sneldb::show",
                alias = self.context.alias(),
                "Shard flush wait completed"
            );
            Ok(())
        } else {
            let joined = errors
                .into_iter()
                .map(|(id, err)| format!("shard {id}: {err}"))
                .collect::<Vec<_>>()
                .join(", ");
            Err(ShowError::new(format!(
                "Failed to flush shards before SHOW: {joined}"
            )))
        }
    }

    #[cfg(test)]
    pub(crate) async fn test_flush_pending_writes(&self) -> ShowResult<()> {
        self.flush_pending_writes().await
    }

    fn ensure_schema_present(&self, entry: &MaterializationEntry) -> ShowResult<()> {
        if entry.schema.is_empty() {
            return Err(ShowError::new(format!(
                "Materialization '{}' does not have a stored schema",
                entry.name
            )));
        }
        Ok(())
    }

    fn timestamp_column(&self, entry: &MaterializationEntry) -> String {
        match entry.spec.query() {
            Command::Query { time_field, .. } => {
                time_field.as_deref().unwrap_or("timestamp").to_string()
            }
            _ => "timestamp".to_string(),
        }
    }

    fn build_delta_command(&self, entry: &MaterializationEntry) -> ShowResult<Command> {
        entry
            .spec
            .delta_command(entry.high_water_mark)
            .map_err(|err| ShowError::new(format!("Failed to build delta command: {err}")))
    }

    fn build_outcome(
        &self,
        mut entry: MaterializationEntry,
        sink: MaterializedSink,
        initial_high_water: HighWaterMark,
    ) -> ShowRefreshOutcome {
        let high_water = sink.high_water_mark();
        entry.schema = sink.schema().to_vec();
        entry.row_count = sink.total_rows();
        entry.delta_rows_appended = sink.last_rows_appended();
        entry.byte_size = sink.total_bytes();
        entry.delta_bytes_appended = sink.last_bytes_appended();
        entry.high_water_mark = if high_water.is_zero() {
            entry.high_water_mark
        } else if high_water == initial_high_water {
            entry.high_water_mark
        } else {
            Some(high_water)
        };
        entry.touch();

        if let Some(mark) = entry.high_water_mark {
            let age = self.current_timestamp().saturating_sub(mark.timestamp);
            tracing::info!(
                target: "sneldb::show",
                alias = self.context.alias(),
                rows = entry.row_count,
                delta_rows = entry.delta_rows_appended,
                bytes = entry.byte_size,
                delta_bytes = entry.delta_bytes_appended,
                high_water_age = age,
                "Materialized view refreshed"
            );
        } else {
            tracing::info!(
                target: "sneldb::show",
                alias = self.context.alias(),
                rows = entry.row_count,
                delta_rows = entry.delta_rows_appended,
                bytes = entry.byte_size,
                delta_bytes = entry.delta_bytes_appended,
                "Materialized view refreshed"
            );
        }

        ShowRefreshOutcome::new(entry)
    }

    fn persist_outcome(
        &self,
        catalog_handle: &mut CatalogHandle,
        outcome: ShowRefreshOutcome,
    ) -> ShowResult<()> {
        catalog_handle.upsert(outcome.into_entry())
    }

    fn current_timestamp(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}
