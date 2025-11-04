use super::frame::metadata::StoredFrameMeta;
use super::manifest::ManifestStore;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;
use tempfile::tempdir;

fn sample_meta() -> StoredFrameMeta {
    StoredFrameMeta {
        file_name: "000000.mat".into(),
        schema: vec![SchemaSnapshot::new("timestamp", "Timestamp")],
        schema_hash: 123,
        row_count: 1,
        min_timestamp: 10,
        max_timestamp: 20,
        max_event_id: 5,
        compressed_len: 64,
        uncompressed_len: 128,
        null_bitmap_len: 8,
        high_water_mark: HighWaterMark::new(20, 5),
    }
}

#[test]
fn manifest_persist_and_reload() {
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("manifest.bin");

    let (store, mut state) = ManifestStore::open(&manifest_path).unwrap();
    assert!(state.frames().is_empty());

    state.push_frame(sample_meta());
    state.bump_frame_index();
    store.persist(&state).unwrap();

    let (_store, state) = ManifestStore::open(&manifest_path).unwrap();
    assert_eq!(state.frames().len(), 1);
    assert_eq!(state.next_frame_index(), 1);
    assert_eq!(state.frames()[0].max_timestamp, 20);
}
