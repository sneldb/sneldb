use std::sync::Arc;

use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::HighWaterMark;

use crate::engine::types::ScalarValue;

#[derive(Clone)]
pub struct WatermarkDeduplicator {
    watermark: HighWaterMark,
    initial_watermark: HighWaterMark, // Store the original watermark for comparison
    timestamp_idx: Option<usize>,
    event_idx: Option<usize>,
}

impl WatermarkDeduplicator {
    pub fn new(
        watermark: HighWaterMark,
        timestamp_idx: Option<usize>,
        event_idx: Option<usize>,
    ) -> Self {
        Self {
            watermark,
            initial_watermark: watermark, // Store the original watermark
            timestamp_idx,
            event_idx,
        }
    }

    pub fn enabled(&self) -> bool {
        self.timestamp_idx.is_some() && self.event_idx.is_some()
    }

    #[cfg(test)]
    pub fn watermark(&self) -> HighWaterMark {
        self.watermark
    }

    #[cfg(test)]
    pub fn initial_watermark(&self) -> HighWaterMark {
        self.initial_watermark
    }

    pub fn filter(&mut self, batch: Arc<ColumnBatch>) -> Option<Arc<ColumnBatch>> {
        let timestamp_idx = match self.timestamp_idx {
            Some(idx) => idx,
            None => return Some(batch),
        };
        let event_idx = match self.event_idx {
            Some(idx) => idx,
            None => return Some(batch),
        };

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

        // Use the original initial watermark for comparison, not the advancing one
        let initial_watermark = self.initial_watermark;

        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                target: "sneldb::show",
                batch_rows = len,
                watermark_ts = initial_watermark.timestamp,
                watermark_event_id = initial_watermark.event_id,
                "Watermark filter: processing batch"
            );
        }

        let mut keep = Vec::with_capacity(len);
        let mut filtered_out = Vec::new();
        let needs_context_id = tracing::enabled!(tracing::Level::WARN);
        let context_ids = if needs_context_id {
            batch
                .schema()
                .columns()
                .iter()
                .position(|c| c.name == "context_id")
                .and_then(|idx| batch.column(idx).ok())
        } else {
            None
        };

        for row_idx in 0..len {
            let ts = Self::parse_u64(&timestamps[row_idx]);
            let event = Self::parse_u64(&events[row_idx]);

            if let (Some(ts), Some(event)) = (ts, event) {
                // Compare against initial watermark, not the advancing one
                // This ensures rows from later batches aren't incorrectly filtered out
                let passes =
                    (ts, event) > (initial_watermark.timestamp, initial_watermark.event_id);
                if passes {
                    keep.push(row_idx);
                } else {
                    filtered_out.push((ts, event));
                }
            } else {
                keep.push(row_idx);
                if needs_context_id {
                    let context_id = context_ids
                        .as_ref()
                        .and_then(|ids| ids.get(row_idx))
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    tracing::warn!(
                        target: "sneldb::show",
                        context_id = %context_id,
                        "Watermark filter: row kept (missing timestamp/event_id)"
                    );
                }
            }
        }

        if keep.is_empty() {
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    target: "sneldb::show",
                    total_rows = len,
                    filtered_count = filtered_out.len(),
                    "Watermark filter: all rows filtered out"
                );
            }
            return None;
        }

        if !filtered_out.is_empty() && tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                target: "sneldb::show",
                filtered_count = filtered_out.len(),
                kept_count = keep.len(),
                total_rows = len,
                "Watermark filter: filtered some rows"
            );
        }

        // Advance watermark to the maximum (timestamp, event_id) in ALL rows of this batch
        // (including filtered ones), not just kept ones. This ensures we don't filter out
        // rows from later batches that arrive out of order.
        let mut max_ts = initial_watermark.timestamp;
        let mut max_event = initial_watermark.event_id;
        for row_idx in 0..len {
            if let (Some(ts), Some(event)) = (
                Self::parse_u64(&timestamps[row_idx]),
                Self::parse_u64(&events[row_idx]),
            ) {
                if (ts, event) > (max_ts, max_event) {
                    max_ts = ts;
                    max_event = event;
                }
            }
        }
        self.watermark.advance(max_ts, max_event);

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                target: "sneldb::show",
                kept_rows = keep.len(),
                total_rows = len,
                new_watermark_ts = self.watermark.timestamp,
                new_watermark_event_id = self.watermark.event_id,
                "Watermark filter: batch processed"
            );
        }

        if keep.len() == len {
            return Some(batch);
        }

        match Arc::try_unwrap(batch) {
            Ok(batch_owned) => Self::filter_owned(batch_owned, keep),
            Err(arc) => {
                let cloned = Self::clone_detached(&arc);
                Self::filter_owned(cloned, keep)
            }
        }
    }

    fn filter_owned(batch: ColumnBatch, keep: Vec<usize>) -> Option<Arc<ColumnBatch>> {
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
            Ok(filtered) => Some(Arc::new(filtered)),
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

    fn clone_detached(batch: &Arc<ColumnBatch>) -> ColumnBatch {
        let len = batch.len();
        let schema = batch.schema();
        let columns: Vec<Vec<ScalarValue>> = batch.columns();
        ColumnBatch::new(Arc::new(schema.clone()), columns, len, None)
            .expect("Failed to recreate batch")
    }

    fn parse_u64(value: &ScalarValue) -> Option<u64> {
        value.as_u64()
    }
}
