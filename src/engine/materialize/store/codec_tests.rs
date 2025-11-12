use super::codec::{
    BatchCodec, EncodedFrame, Lz4BatchCodec, batch_schema_to_snapshots, schema_hash,
};
use super::frame::data::FrameData;
use super::frame::header::FrameHeader;
use super::frame::metadata::StoredFrameMeta;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::types::ScalarValue;
use crc32fast::Hasher as Crc32Hasher;
use serde_json::{Value, json};
use std::sync::Arc;

fn build_schema() -> BatchSchema {
    BatchSchema::new(vec![
        ColumnSpec {
            name: "timestamp".into(),
            logical_type: "Timestamp".into(),
        },
        ColumnSpec {
            name: "event_id".into(),
            logical_type: "Integer".into(),
        },
    ])
    .expect("valid schema")
}

fn build_batch(schema: &Arc<BatchSchema>) -> ColumnBatch {
    let pool = BatchPool::new(4).unwrap();
    let mut builder = pool.acquire(Arc::clone(schema));
    builder
        .push_row(&[
            ScalarValue::from(json!(1_700_000_000_u64)),
            ScalarValue::from(json!(10_u64)),
        ])
        .unwrap();
    builder
        .push_row(&[
            ScalarValue::from(json!(1_700_000_005_u64)),
            ScalarValue::Null,
        ])
        .unwrap();
    builder.finish().unwrap()
}

#[test]
fn schema_hash_is_deterministic() {
    let schema = build_schema();
    let snapshots_a = batch_schema_to_snapshots(&schema);
    let snapshots_b = batch_schema_to_snapshots(&schema);
    assert_eq!(schema_hash(&snapshots_a), schema_hash(&snapshots_b));
}

#[test]
fn lz4_codec_roundtrip() {
    let schema = Arc::new(build_schema());
    let snapshots = batch_schema_to_snapshots(&schema);
    let batch = build_batch(&schema);

    let codec = Lz4BatchCodec::default();
    let encoded: EncodedFrame = codec.encode(&snapshots, &batch).unwrap();

    let mut crc = Crc32Hasher::new();
    crc.update(&encoded.compressed);
    let checksum = crc.finalize();

    let header = FrameHeader {
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count,
        column_count: snapshots.len() as u32,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        uncompressed_len: encoded.uncompressed_len,
        compressed_len: encoded.compressed.len() as u32,
        null_bitmap_len: encoded.null_bitmap_len,
        checksum,
    };
    let frame_data = FrameData {
        header: header.clone(),
        compressed: encoded.compressed.clone(),
    };

    let meta = StoredFrameMeta {
        file_name: "000000.mat".into(),
        schema: snapshots.clone(),
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        compressed_len: encoded.compressed.len() as u32,
        uncompressed_len: encoded.uncompressed_len,
        null_bitmap_len: encoded.null_bitmap_len,
        high_water_mark: HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id),
    };

    let decoded = codec.decode(&meta, frame_data).unwrap();
    assert_eq!(decoded.len(), batch.len());
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1_700_000_000_u64))
    );
    assert!(decoded.column(1).unwrap()[1].is_null());
}
