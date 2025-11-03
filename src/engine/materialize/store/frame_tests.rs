use super::codec::{BatchCodec, Lz4BatchCodec, batch_schema_to_snapshots};
use super::frame::storage::FrameStorage;
use crate::engine::core::read::flow::{BatchPool, BatchSchema};
use crate::engine::core::read::result::ColumnSpec;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

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

#[test]
fn frame_writer_reader_roundtrip() {
    let dir = tempdir().unwrap();
    let schema = Arc::new(build_schema());
    let snapshots = batch_schema_to_snapshots(&schema);

    let pool = BatchPool::new(8).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema));
    builder
        .push_row(&[json!(1_700_000_000_u64), json!(42_u64)])
        .unwrap();
    let batch = builder.finish().unwrap();

    let codec = Lz4BatchCodec::default();
    let encoded = codec.encode(&snapshots, &batch).unwrap();

    let storage = FrameStorage::create(dir.path()).unwrap();
    let writer = storage.writer();
    let meta = writer.write(0, &encoded).unwrap();

    let reader = storage.reader();
    let data = reader.read(&meta).unwrap();
    let decoded = codec.decode(&meta, data).unwrap();

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded.column(1).unwrap()[0], json!(42));
}
