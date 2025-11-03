use super::header::{FRAME_VERSION, FrameHeader};
use crate::engine::materialize::MaterializationError;

fn build_header() -> FrameHeader {
    FrameHeader {
        schema_hash: 0xBADF00D,
        row_count: 128,
        column_count: 5,
        min_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_333,
        max_event_id: 777,
        uncompressed_len: 4096,
        compressed_len: 1024,
        null_bitmap_len: 64,
        checksum: 0xCAFEBABE,
    }
}

#[test]
fn header_write_then_read_roundtrips() {
    let header = build_header();
    let mut buf = Vec::new();

    header.write_to(&mut buf).expect("serialize header");
    assert_eq!(buf.len(), 8 * 4 + 4 * 6); // four u64 + six u32 fields

    let decoded = FrameHeader::read_from(&buf[..]).expect("deserialize header");
    assert_eq!(decoded.schema_hash, header.schema_hash);
    assert_eq!(decoded.max_event_id, header.max_event_id);
    assert_eq!(decoded.checksum, header.checksum);
}

#[test]
fn header_read_from_short_buffer_errors() {
    let buf = vec![0u8; 8];
    let err = FrameHeader::read_from(&buf[..]).unwrap_err();
    match err {
        MaterializationError::Io(_) => {}
        other => panic!("expected io error, got {other:?}"),
    }
}

#[test]
fn frame_version_constant_is_stable() {
    assert_eq!(FRAME_VERSION, 1, "bump tests if frame format changes");
}
