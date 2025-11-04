use super::reader::FrameReader;
use super::storage::FrameStorage;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::store::codec::EncodedFrame;
use tempfile::tempdir;

fn build_encoded_frame() -> EncodedFrame {
    EncodedFrame {
        schema: vec![SchemaSnapshot::new("timestamp", "Timestamp")],
        schema_hash: 0xAA55,
        row_count: 2,
        min_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_500,
        max_event_id: 42,
        null_bitmap_len: 2,
        compressed: vec![10, 20, 30, 40],
        uncompressed_len: 256,
    }
}

fn write_frame(storage: &FrameStorage) -> (super::metadata::StoredFrameMeta, EncodedFrame) {
    let encoded = build_encoded_frame();
    let meta = storage.writer().write(0, &encoded).expect("write frame");
    (meta, encoded)
}

#[test]
fn reader_returns_frame_data() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let (meta, encoded) = write_frame(&storage);

    let reader = storage.reader();
    let data = reader.read(&meta).expect("read frame");

    assert_eq!(data.header.schema_hash, encoded.schema_hash);
    assert_eq!(data.header.row_count, encoded.row_count);
    assert_eq!(data.compressed, encoded.compressed);
}

#[test]
fn reader_detects_checksum_mismatch() {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let (meta, _encoded) = write_frame(&storage);

    let frame_path = storage.path().join(&meta.file_name);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&frame_path)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[0xFF]).unwrap();

    let reader = storage.reader();
    let err = reader.read(&meta).unwrap_err();
    match err {
        MaterializationError::Corrupt(msg) => {
            assert!(msg.contains("Checksum mismatch"));
        }
        other => panic!("expected corrupt error, got {other:?}"),
    }
}

#[test]
fn reader_rejects_schema_hash_mismatch() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let (mut meta, _) = write_frame(&storage);
    meta.schema_hash ^= 0xFFFF;

    let reader = FrameReader::new(storage.path());
    let err = reader.read(&meta).unwrap_err();
    match err {
        MaterializationError::Corrupt(msg) => {
            assert!(msg.contains("Schema hash mismatch"));
        }
        other => panic!("expected corrupt error, got {other:?}"),
    }
}
