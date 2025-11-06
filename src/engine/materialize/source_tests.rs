use super::source::MaterializedSource;
use super::store::MaterializedStore;
use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowSource, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::sink::MaterializedSink;
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

fn batch_to_rows(batch: &crate::engine::core::read::flow::ColumnBatch) -> Vec<Vec<serde_json::Value>> {
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

#[tokio::test]
async fn materialized_source_streams_frames() {
    let dir = tempdir().unwrap();
    let store = MaterializedStore::open(dir.path()).unwrap();
    let schema = build_schema();
    let schema_arc = Arc::new(schema);
    let mut sink = MaterializedSink::from_batch_schema(store, &schema_arc).unwrap();

    use crate::engine::types::ScalarValue;
    let pool = BatchPool::new(8).unwrap();
    let mut builder = pool.acquire(Arc::clone(&schema_arc));
    builder
        .push_row(&[ScalarValue::from(json!(1_700_000_000_u64)), ScalarValue::from(json!("ctx-a")), ScalarValue::from(json!(1_u64))])
        .unwrap();
    builder
        .push_row(&[ScalarValue::from(json!(1_700_000_050_u64)), ScalarValue::from(json!("ctx-b")), ScalarValue::from(json!(2_u64))])
        .unwrap();
    sink.append(&builder.finish().unwrap()).unwrap();
    drop(sink);

    let store = MaterializedStore::open(dir.path()).unwrap();
    let source = MaterializedSource::new(store);

    let metrics = FlowMetrics::new();
    let pool = BatchPool::new(16).unwrap();
    let ctx = Arc::new(FlowContext::new(
        16,
        pool,
        Arc::clone(&metrics),
        Option::<&str>::None,
        FlowTelemetry::default(),
    ));
    let (sender, mut receiver) = FlowChannel::bounded(8, metrics);

    let handle = tokio::spawn(async move {
        source
            .run(sender, ctx)
            .await
            .expect("materialized source run");
    });

    let mut collected = Vec::new();
    while let Some(batch) = receiver.recv().await {
        collected.extend(batch_to_rows(&batch));
    }
    handle.await.unwrap();

    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0][1], json!("ctx-a"));
    assert_eq!(collected[1][2], json!(2));
}
