use super::batch_codec::{BatchCodec, Lz4BatchCodec};
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::store::codec::batch_schema_to_snapshots;
use crate::engine::materialize::store::frame::data::FrameData;
use crate::engine::materialize::store::frame::header::FrameHeader;
use crate::engine::materialize::store::frame::metadata::StoredFrameMeta;
use crate::engine::materialize::{MaterializationError, high_water::HighWaterMark};
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

fn build_batch(schema: &Arc<BatchSchema>, rows: Vec<(u64, u64)>) -> ColumnBatch {
    let pool = BatchPool::new(100).unwrap();
    let mut builder = pool.acquire(Arc::clone(schema));

    for (ts, id) in rows {
        builder
            .push_row(&[ScalarValue::from(json!(ts)), ScalarValue::from(json!(id))])
            .unwrap();
    }

    builder.finish().unwrap()
}

#[test]
fn encode_creates_encoded_frame() {
    let schema = Arc::new(build_schema());
    let snapshots = batch_schema_to_snapshots(&schema);
    let batch = build_batch(&schema, vec![(1700000000, 1), (1700000001, 2)]);

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 2);
    assert_eq!(encoded.schema.len(), 2);
    assert!(encoded.compressed.len() > 0);
    assert!(encoded.uncompressed_len > 0);
    assert_eq!(encoded.min_timestamp, 1700000000);
    assert_eq!(encoded.max_timestamp, 1700000001);
    assert_eq!(encoded.max_event_id, 2);
}

#[test]
fn encode_decode_roundtrip() {
    let schema = Arc::new(build_schema());
    let snapshots = batch_schema_to_snapshots(&schema);
    let original = build_batch(&schema, vec![(1700000000, 1), (1700000001, 2)]);

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &original).unwrap();

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
        file_name: "test.mat".into(),
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

    assert_eq!(decoded.len(), original.len());
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1700000000_u64))
    );
    assert_eq!(
        decoded.column(0).unwrap()[1],
        ScalarValue::from(json!(1700000001_u64))
    );
    assert_eq!(
        decoded.column(1).unwrap()[0],
        ScalarValue::from(json!(1_u64))
    );
    assert_eq!(
        decoded.column(1).unwrap()[1],
        ScalarValue::from(json!(2_u64))
    );
}

#[test]
fn encode_decode_handles_null_values() {
    let schema = Arc::new(
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
        .expect("valid schema"),
    );
    let snapshots = batch_schema_to_snapshots(&schema);

    let pool = BatchPool::new(100).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema));
    builder
        .push_row(&[
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!(1)),
        ])
        .unwrap();
    builder
        .push_row(&[ScalarValue::from(json!(2000_u64)), ScalarValue::Null])
        .unwrap();
    let original = builder.finish().unwrap();

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &original).unwrap();

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
        header,
        compressed: encoded.compressed,
    };

    let meta = StoredFrameMeta {
        file_name: "test.mat".into(),
        schema: snapshots,
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        compressed_len: frame_data.compressed.len() as u32,
        uncompressed_len: encoded.uncompressed_len,
        null_bitmap_len: encoded.null_bitmap_len,
        high_water_mark: HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id),
    };

    let decoded = codec.decode(&meta, frame_data).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1000_u64))
    );
    assert!(decoded.column(1).unwrap()[1].is_null());
}

#[test]
fn encode_decode_handles_all_types() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".into(),
                logical_type: "Timestamp".into(),
            },
            ColumnSpec {
                name: "id".into(),
                logical_type: "Integer".into(),
            },
            ColumnSpec {
                name: "price".into(),
                logical_type: "Float".into(),
            },
            ColumnSpec {
                name: "active".into(),
                logical_type: "Boolean".into(),
            },
            ColumnSpec {
                name: "name".into(),
                logical_type: "String".into(),
            },
            ColumnSpec {
                name: "metadata".into(),
                logical_type: "JSON".into(),
            },
        ])
        .expect("valid schema"),
    );
    let snapshots = batch_schema_to_snapshots(&schema);

    let pool = BatchPool::new(100).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema));
    builder
        .push_row(&[
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!(42)),
            ScalarValue::from(json!(19.99)),
            ScalarValue::from(json!(true)),
            ScalarValue::from(json!("product")),
            ScalarValue::from(json!({"category": "electronics"})),
        ])
        .unwrap();
    let original = builder.finish().unwrap();

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &original).unwrap();

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
        header,
        compressed: encoded.compressed,
    };

    let meta = StoredFrameMeta {
        file_name: "test.mat".into(),
        schema: snapshots,
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        compressed_len: frame_data.compressed.len() as u32,
        uncompressed_len: encoded.uncompressed_len,
        null_bitmap_len: encoded.null_bitmap_len,
        high_water_mark: HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id),
    };

    let decoded = codec.decode(&meta, frame_data).unwrap();

    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1000_u64))
    );
    assert_eq!(decoded.column(1).unwrap()[0], ScalarValue::from(json!(42)));
    assert_eq!(
        decoded.column(3).unwrap()[0],
        ScalarValue::from(json!(true))
    );
    assert_eq!(
        decoded.column(4).unwrap()[0],
        ScalarValue::from(json!("product"))
    );
}

#[test]
fn decode_fails_on_payload_length_mismatch() {
    let schema = Arc::new(build_schema());
    let snapshots = batch_schema_to_snapshots(&schema);
    let batch = build_batch(&schema, vec![(1000, 1)]);

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &batch).unwrap();

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
        uncompressed_len: encoded.uncompressed_len + 100, // Wrong length
        compressed_len: encoded.compressed.len() as u32,
        null_bitmap_len: encoded.null_bitmap_len,
        checksum,
    };

    let frame_data = FrameData {
        header,
        compressed: encoded.compressed,
    };

    let meta = StoredFrameMeta {
        file_name: "test.mat".into(),
        schema: snapshots,
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        compressed_len: frame_data.compressed.len() as u32,
        uncompressed_len: encoded.uncompressed_len + 100,
        null_bitmap_len: encoded.null_bitmap_len,
        high_water_mark: HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id),
    };

    let err = codec.decode(&meta, frame_data).unwrap_err();
    // The error may come from decompression (LZ4) or length mismatch check
    assert!(matches!(err, MaterializationError::Corrupt(_)));
}

#[test]
fn encode_compresses_data() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "data".into(),
            logical_type: "String".into(),
        }])
        .expect("valid schema"),
    );
    let snapshots = batch_schema_to_snapshots(&schema);

    let pool = BatchPool::new(100).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema));
    // Create data that should compress well
    let repeated = "x".repeat(1000);
    builder
        .push_row(&[ScalarValue::from(json!(repeated))])
        .unwrap();
    let batch = builder.finish().unwrap();

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &batch).unwrap();

    // Compressed size should be smaller than uncompressed
    assert!(encoded.compressed.len() < encoded.uncompressed_len as usize);
}
