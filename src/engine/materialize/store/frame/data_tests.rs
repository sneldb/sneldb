use super::data::FrameData;
use super::header::FrameHeader;

fn sample_header() -> FrameHeader {
    FrameHeader {
        schema_hash: 0xDEADBEEF,
        row_count: 42,
        column_count: 3,
        min_timestamp: 1_700_000_000,
        max_timestamp: 1_700_000_123,
        max_event_id: 999,
        uncompressed_len: 1024,
        compressed_len: 512,
        null_bitmap_len: 16,
        checksum: 0xABCD1234,
    }
}

#[test]
fn frame_data_retains_header_and_payload() {
    let header = sample_header();
    let payload = vec![1u8, 2, 3, 4, 5];

    let data = FrameData {
        header: header.clone(),
        compressed: payload.clone(),
    };

    assert_eq!(data.header.schema_hash, header.schema_hash);
    assert_eq!(data.header.max_event_id, header.max_event_id);
    assert_eq!(data.compressed, payload);
}
