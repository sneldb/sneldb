use super::sink::MaterializedSink;
use super::store::{MaterializedStore, batch_schema_to_snapshots};
use crate::engine::core::read::flow::{BatchPool, BatchSchema};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::materialize::{MaterializationError, SchemaSnapshot};
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

#[test]
fn sink_appends_and_tracks_stats() {
    let dir = tempdir().unwrap();
    let store = MaterializedStore::open(dir.path()).unwrap();
    let schema = build_schema();
    let schema_arc = Arc::new(schema);

    let mut sink = MaterializedSink::from_batch_schema(store, &schema_arc).unwrap();
    assert_eq!(sink.total_rows(), 0);
    assert_eq!(sink.high_water_mark(), HighWaterMark::default());

    use crate::engine::types::ScalarValue;
    let pool = BatchPool::new(8).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema_arc));
    builder
        .push_row(&[ScalarValue::from(json!(1_700_000_000_u64)), ScalarValue::from(json!("ctx")), ScalarValue::from(json!(123_u64))])
        .unwrap();
    let batch = builder.finish().unwrap();

    sink.append(&batch).unwrap();
    assert_eq!(sink.total_rows(), 1);
    assert_eq!(
        sink.high_water_mark(),
        HighWaterMark::new(1_700_000_000, 123)
    );
    assert_eq!(sink.last_rows_appended(), 1);

    let store = sink.into_store();
    assert_eq!(store.frames().len(), 1);
}

#[test]
fn sink_rejects_schema_mismatch_with_existing_frames() {
    let dir = tempdir().unwrap();
    let store = MaterializedStore::open(dir.path()).unwrap();
    let schema = build_schema();
    let schema_arc = Arc::new(schema);

    use crate::engine::types::ScalarValue;
    let mut sink = MaterializedSink::from_batch_schema(store, &schema_arc).unwrap();
    let pool = BatchPool::new(4).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema_arc));
    builder
        .push_row(&[ScalarValue::from(json!(1_700_000_000_u64)), ScalarValue::from(json!("ctx")), ScalarValue::from(json!(1_u64))])
        .unwrap();
    sink.append(&builder.finish().unwrap()).unwrap();
    let store = sink.into_store();

    let mut snapshots = batch_schema_to_snapshots(&schema_arc);
    snapshots[1] = SchemaSnapshot::new("other_context", "String");
    let err = match MaterializedSink::new(store, snapshots) {
        Ok(_) => panic!("expected schema mismatch"),
        Err(err) => err,
    };
    assert!(matches!(err, MaterializationError::Corrupt(_)));
}

#[test]
fn sink_rejects_mismatched_batch_schema() {
    let dir = tempdir().unwrap();
    let store = MaterializedStore::open(dir.path()).unwrap();
    let schema = build_schema();
    let schema_arc = Arc::new(schema);

    let mut sink = MaterializedSink::from_batch_schema(store, &schema_arc).unwrap();

    let mismatched_schema = BatchSchema::new(vec![
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
            logical_type: "String".into(),
        },
    ])
    .expect("valid schema");
    use crate::engine::types::ScalarValue;
    let mismatched_arc = Arc::new(mismatched_schema);
    let pool = BatchPool::new(4).unwrap();
    let mut builder = pool.acquire(Arc::clone(&mismatched_arc));
    builder
        .push_row(&[ScalarValue::from(json!(1_700_000_000_u64)), ScalarValue::from(json!("ctx")), ScalarValue::from(json!("id"))])
        .unwrap();
    let batch = builder.finish().unwrap();

    let err = sink.append(&batch).unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
}
