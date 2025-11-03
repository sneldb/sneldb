use crate::engine::materialize::catalog::MaterializationTelemetry;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::shared::time::now;

use super::manifest::ManifestState;

#[derive(Debug, Clone)]
pub struct TelemetrySnapshot {
    pub row_count: u64,
    pub delta_rows_appended: u64,
    pub byte_size: u64,
    pub delta_bytes_appended: u64,
    pub high_water_mark: Option<HighWaterMark>,
    pub updated_at: u64,
}

impl TelemetrySnapshot {
    pub fn into_catalog(self) -> MaterializationTelemetry {
        MaterializationTelemetry {
            row_count: self.row_count,
            delta_rows_appended: self.delta_rows_appended,
            byte_size: self.byte_size,
            delta_bytes_appended: self.delta_bytes_appended,
            high_water_mark: self.high_water_mark,
            updated_at: self.updated_at,
        }
    }
}

pub struct TelemetryTracker;

impl TelemetryTracker {
    pub fn summarize(state: &ManifestState) -> TelemetrySnapshot {
        let mut row_count = 0u64;
        let mut byte_size = 0u64;
        for frame in state.frames() {
            row_count = row_count.saturating_add(frame.row_count as u64);
            byte_size = byte_size.saturating_add(frame.compressed_len as u64);
        }

        let high_water_mark = state.frames().last().map(|frame| frame.high_water_mark);

        TelemetrySnapshot {
            row_count,
            delta_rows_appended: 0,
            byte_size,
            delta_bytes_appended: 0,
            high_water_mark,
            updated_at: now(),
        }
    }
}
