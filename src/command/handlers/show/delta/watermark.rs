use std::sync::Arc;

use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::HighWaterMark;

use serde_json::Value as JsonValue;

#[derive(Clone)]
pub struct WatermarkDeduplicator {
    watermark: HighWaterMark,
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
            timestamp_idx,
            event_idx,
        }
    }

    pub fn enabled(&self) -> bool {
        self.timestamp_idx.is_some() && self.event_idx.is_some()
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

        let mut keep = Vec::with_capacity(len);
        for row_idx in 0..len {
            let ts = Self::parse_u64(&timestamps[row_idx]);
            let event = Self::parse_u64(&events[row_idx]);

            if let (Some(ts), Some(event)) = (ts, event) {
                if (ts, event) > (self.watermark.timestamp, self.watermark.event_id) {
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
            if let (Some(ts), Some(event)) = (
                Self::parse_u64(&timestamps[idx]),
                Self::parse_u64(&events[idx]),
            ) {
                self.watermark.advance(ts, event);
            }
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
        let columns: Vec<Vec<JsonValue>> = batch.columns().map(|col| col.to_vec()).collect();
        ColumnBatch::new(Arc::new(schema.clone()), columns, len, None)
            .expect("Failed to recreate batch")
    }

    fn parse_u64(value: &JsonValue) -> Option<u64> {
        match value {
            JsonValue::Number(number) => number.as_u64(),
            JsonValue::String(s) => s.parse::<u64>().ok(),
            _ => None,
        }
    }
}
