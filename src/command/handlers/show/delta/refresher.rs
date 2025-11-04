use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::handlers::show::errors::{ShowError, ShowResult};
use crate::engine::core::read::flow::BatchSender;
use crate::engine::materialize::{
    HighWaterMark, MaterializationEntry, MaterializedSink, MaterializedStore,
};

use super::watermark::WatermarkDeduplicator;

pub struct DeltaRefresher {
    sink: Arc<Mutex<MaterializedSink>>,
    watermark_template: WatermarkDeduplicator,
    has_filtering: bool,
    initial_high_water: HighWaterMark,
}

impl DeltaRefresher {
    pub fn new(
        entry: &MaterializationEntry,
        timestamp_idx: Option<usize>,
        event_idx: Option<usize>,
    ) -> ShowResult<Self> {
        let store = MaterializedStore::open(&entry.storage_path)
            .map_err(|err| ShowError::new(format!("Failed to open sink store: {err}")))?;

        let mut sink = MaterializedSink::new(store, entry.schema.clone())
            .map_err(|err| ShowError::new(format!("Failed to create materialized sink: {err}")))?;

        if let Some(policy) = entry.retention.clone() {
            sink.set_retention_policy(policy);
        }

        let initial_high_water = sink.high_water_mark();
        let has_filtering = timestamp_idx.is_some() && event_idx.is_some();
        let watermark_template =
            WatermarkDeduplicator::new(initial_high_water, timestamp_idx, event_idx);

        Ok(Self {
            sink: Arc::new(Mutex::new(sink)),
            watermark_template,
            has_filtering,
            initial_high_water,
        })
    }

    pub fn initial_high_water(&self) -> HighWaterMark {
        self.initial_high_water
    }

    pub fn has_watermark_filtering(&self) -> bool {
        self.has_filtering
    }

    pub fn spawn_stream_task(
        &self,
        mut stream: QueryBatchStream,
        sender: BatchSender,
        alias: &str,
    ) -> JoinHandle<()> {
        let sink = Arc::clone(&self.sink);
        let mut watermark = self.watermark_template.clone();
        let alias = alias.to_string();

        tokio::spawn(async move {
            let mut batch_count = 0u64;
            let mut total_rows = 0usize;
            let stream_start = Instant::now();

            while let Some(batch) = stream.recv().await {
                if batch.is_empty() {
                    continue;
                }

                let original_rows = batch.len();
                let mut batch = batch;
                if watermark.enabled() {
                    match watermark.filter(batch) {
                        Some(filtered) => {
                            if filtered.len() < original_rows {
                                tracing::warn!(
                                    target: "sneldb::show",
                                    alias = %alias,
                                    batch_num = batch_count + 1,
                                    original_rows = original_rows,
                                    filtered_rows = filtered.len(),
                                    "Delta batch filtered by watermark"
                                );
                            }
                            batch = filtered;
                        }
                        None => continue,
                    }
                }

                if let Err(err) = sink.lock().await.append(batch.as_ref()) {
                    tracing::error!(
                        target: "sneldb::show",
                        error = %err,
                        "Failed to append delta batch"
                    );
                    continue;
                }

                batch_count += 1;
                total_rows += batch.len();

                if sender.send(batch).await.is_err() {
                    break;
                }
            }

            let stream_time = stream_start.elapsed();
            tracing::warn!(
                target: "sneldb::show",
                alias = %alias,
                batch_count = batch_count,
                total_rows = total_rows,
                stream_time_ms = stream_time.as_millis(),
                "Delta stream completed"
            );
        })
    }

    pub fn take_sink(self) -> ShowResult<MaterializedSink> {
        Arc::try_unwrap(self.sink)
            .map_err(|_| ShowError::new("Materialized sink still in use"))
            .map(|mutex| mutex.into_inner())
    }
}
