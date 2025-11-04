use super::frame::StoredFrameMeta;
use super::manifest::ManifestState;
use super::telemetry::TelemetryTracker;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;

fn meta(rows: u32, bytes: u32, timestamp: u64) -> StoredFrameMeta {
    StoredFrameMeta {
        file_name: format!("{:06}.mat", timestamp),
        schema: vec![SchemaSnapshot::new("timestamp", "Timestamp")],
        schema_hash: 1,
        row_count: rows,
        min_timestamp: timestamp,
        max_timestamp: timestamp,
        max_event_id: timestamp,
        compressed_len: bytes,
        uncompressed_len: bytes * 2,
        null_bitmap_len: 8,
        high_water_mark: HighWaterMark::new(timestamp, timestamp),
    }
}

#[test]
fn telemetry_summarizes_manifest() {
    let mut state = ManifestState::default();
    state.push_frame(meta(10, 100, 1));
    state.push_frame(meta(5, 50, 2));

    let snapshot = TelemetryTracker::summarize(&state);
    assert_eq!(snapshot.row_count, 15);
    assert_eq!(snapshot.byte_size, 150);
    assert!(snapshot.high_water_mark.is_some());
    assert_eq!(snapshot.high_water_mark.unwrap().timestamp, 2);
}
