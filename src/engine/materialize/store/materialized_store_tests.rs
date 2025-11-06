use super::{MaterializedStore, StoredFrameMeta, batch_schema_to_snapshots};
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::high_water::HighWaterMark;
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::tempdir;

fn build_schema() -> BatchSchema {
    BatchSchema::new(vec![
        ColumnSpec {
            name: "timestamp".into(),
            logical_type: "Timestamp".into(),
        },
        ColumnSpec {
            name: "context_id".into(),
            logical_type: "String".into(),
        },
        ColumnSpec {
            name: "event_id".into(),
            logical_type: "Integer".into(),
        },
    ])
    .expect("valid schema")
}

fn batch_to_rows(batch: &ColumnBatch) -> Vec<Vec<serde_json::Value>> {
    use crate::engine::types::ScalarValue;
    let column_count = batch.schema().column_count();
    let mut rows = vec![vec![serde_json::Value::Null; column_count]; batch.len()];
    for col_idx in 0..column_count {
        let values = batch.column(col_idx).expect("column access");
        for (row_idx, value) in values.iter().enumerate() {
            rows[row_idx][col_idx] = value.to_json();
        }
    }
    rows
}

#[test]
fn materialized_store_roundtrip() {
    let dir = tempdir().unwrap();
    let mut store = MaterializedStore::open(dir.path()).unwrap();

    use crate::engine::types::ScalarValue;
    let schema = build_schema();
    let schema_arc = Arc::new(schema);
    let pool = BatchPool::new(16).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema_arc));
    builder
        .push_row(&[
            ScalarValue::from(json!(1_700_000_000_u64)),
            ScalarValue::from(json!("ctx-a")),
            ScalarValue::from(json!(1001_u64)),
        ])
        .unwrap();
    builder
        .push_row(&[
            ScalarValue::from(json!(1_700_000_100_u64)),
            ScalarValue::from(json!("ctx-b")),
            ScalarValue::from(json!(1002_u64)),
        ])
        .unwrap();
    let batch = builder.finish().unwrap();

    let snapshots = batch_schema_to_snapshots(&schema_arc);
    let meta = store.append_batch(&snapshots, &batch).unwrap();

    assert_eq!(meta.row_count, 2);
    assert_eq!(meta.min_timestamp, 1_700_000_000);
    assert_eq!(meta.max_timestamp, 1_700_000_100);
    assert_eq!(meta.max_event_id, 1002);
    assert_eq!(
        meta.high_water_mark,
        HighWaterMark::new(1_700_000_100, 1002)
    );

    let read_batch = store.read_frame(&meta).unwrap();
    let rows = batch_to_rows(&*read_batch);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], json!("ctx-a"));
    assert_eq!(rows[1][2], json!(1002));
}

#[test]
fn manifest_persists_across_reopen() {
    let dir = tempdir().unwrap();
    let schema = Arc::new(build_schema());
    let pool = BatchPool::new(8).unwrap();

    {
        use crate::engine::types::ScalarValue;
        let mut store = MaterializedStore::open(dir.path()).unwrap();
        let mut builder = pool.acquire(Arc::clone(&schema));
        builder
            .push_row(&[
                ScalarValue::from(json!(1_700_000_500_u64)),
                ScalarValue::from(json!("ctx-c")),
                ScalarValue::from(json!(42_u64)),
            ])
            .unwrap();
        let batch = builder.finish().unwrap();
        let snapshots = batch_schema_to_snapshots(&schema);
        let meta = store.append_batch(&snapshots, &batch).unwrap();
        assert_eq!(meta.high_water_mark, HighWaterMark::new(1_700_000_500, 42));
    }

    let store = MaterializedStore::open(dir.path()).unwrap();
    assert_eq!(store.frames().len(), 1);
    let meta: &StoredFrameMeta = &store.frames()[0];
    let batch = store.read_frame(meta).unwrap();
    let rows = batch_to_rows(&*batch);
    assert_eq!(rows[0][1], json!("ctx-c"));
}
