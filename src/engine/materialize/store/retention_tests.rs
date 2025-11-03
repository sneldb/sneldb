use super::frame::metadata::StoredFrameMeta;
use super::frame::storage::FrameStorage;
use super::manifest::ManifestState;
use super::retention::RetentionEnforcer;
use crate::engine::materialize::catalog::{RetentionPolicy, SchemaSnapshot};
use crate::engine::materialize::high_water::HighWaterMark;
use tempfile::tempdir;

fn make_meta(file_name: &str, row_count: u32, timestamp: u64, event_id: u64) -> StoredFrameMeta {
    StoredFrameMeta {
        file_name: file_name.into(),
        schema: vec![SchemaSnapshot::new("timestamp", "Timestamp")],
        schema_hash: 1,
        row_count,
        min_timestamp: timestamp,
        max_timestamp: timestamp,
        max_event_id: event_id,
        compressed_len: 32,
        uncompressed_len: 64,
        null_bitmap_len: 8,
        high_water_mark: HighWaterMark::new(timestamp, event_id),
    }
}

#[test]
fn retention_drops_old_frames_by_row_count() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let mut state = ManifestState::default();
    state.push_frame(make_meta("000000.mat", 1, 10, 1));
    state.bump_frame_index();
    state.push_frame(make_meta("000001.mat", 1, 20, 2));
    state.bump_frame_index();

    let policy = RetentionPolicy {
        max_rows: Some(1),
        max_age_seconds: None,
    };

    let enforcer = RetentionEnforcer::new(&storage);
    enforcer.apply(Some(&policy), &mut state).unwrap();

    assert_eq!(state.frames().len(), 1);
    assert_eq!(state.frames()[0].file_name, "000001.mat");
}

