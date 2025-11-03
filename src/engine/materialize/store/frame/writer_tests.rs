use super::storage::FrameStorage;
use super::writer::FrameWriter;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::store::codec::EncodedFrame;
use tempfile::tempdir;

fn build_encoded_frame() -> EncodedFrame {
    let schema = vec![SchemaSnapshot::new("timestamp", "Timestamp")];
    EncodedFrame {
        schema,
        schema_hash: 0xBEEF,
        row_count: 3,
        min_timestamp: 1_700_000_100,
        max_timestamp: 1_700_000_300,
        max_event_id: 55,
        null_bitmap_len: 4,
        compressed: vec![1, 2, 3, 4, 5, 6],
        uncompressed_len: 128,
    }
}

#[test]
fn writer_persists_frame_and_returns_metadata() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let writer = storage.writer();

    let encoded = build_encoded_frame();
    let meta = writer.write(0, &encoded).expect("write frame");

    assert_eq!(meta.file_name, "000000.mat");
    assert_eq!(meta.schema_hash, encoded.schema_hash);
    assert_eq!(meta.max_timestamp, encoded.max_timestamp);
    assert_eq!(meta.high_water_mark, HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id));

    let frame_path = storage.path().join(&meta.file_name);
    assert!(frame_path.exists());
}

#[test]
fn writer_output_can_be_read_back() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let writer = storage.writer();
    let reader = storage.reader();

    let encoded = build_encoded_frame();
    let meta = writer.write(7, &encoded).expect("write frame");

    let data = reader.read(&meta).expect("read frame");
    assert_eq!(data.header.schema_hash, encoded.schema_hash);
    assert_eq!(data.header.row_count, encoded.row_count);
    assert_eq!(data.header.compressed_len as usize, encoded.compressed.len());
    assert_eq!(data.compressed, encoded.compressed);
}

#[test]
fn writer_propagates_io_error() {
    let encoded = build_encoded_frame();
    // Use an invalid directory path to force an I/O failure (on most systems, /dev/null is a file).
    let invalid_path = std::path::Path::new("/dev/null");
    let writer = FrameWriter::new(invalid_path);
    let err = writer.write(0, &encoded).unwrap_err();
    matches!(err, MaterializationError::Io(_));
}

