use crate::engine::materialize::high_water::HighWaterMark;

#[derive(Debug, Clone)]
pub struct MaterializationTelemetry {
    pub row_count: u64,
    pub delta_rows_appended: u64,
    pub byte_size: u64,
    pub delta_bytes_appended: u64,
    pub high_water_mark: Option<HighWaterMark>,
    pub updated_at: u64,
}
