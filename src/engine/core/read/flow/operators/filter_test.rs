use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::flow::{
    BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowOperator, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;

use super::{FilterOp, FilterPredicate};

fn test_context() -> Arc<FlowContext> {
    let metrics = FlowMetrics::new();
    let pool = crate::engine::core::read::flow::BatchPool::new(8).unwrap();
    Arc::new(FlowContext::new(
        8,
        pool,
        metrics,
        None::<&str>,
        FlowTelemetry::default(),
    ))
}

fn build_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "value".into(),
            logical_type: "Integer".into(),
        }])
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_op_drops_rows() {
    let ctx = test_context();
    let schema = build_schema();

    let predicate: FilterPredicate = Arc::new(|row| {
        row.get(0)
            .and_then(|v| v.as_i64())
            .map(|n| n % 2 == 0)
            .unwrap_or(false)
    });

    let op = FilterOp::new(predicate);
    let (tx, rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    let mut builder = ctx.pool().acquire(Arc::clone(&schema));
    for value in 0..6 {
        builder.push_row(&[json!(value)]).expect("row inserted");
    }
    let batch = builder.finish().expect("batch builds");
    tx.send(batch).await.expect("send batch");
    drop(tx);

    let ctx_clone = Arc::clone(&ctx);
    tokio::spawn(async move {
        op.run(rx, out_tx, ctx_clone).await.unwrap();
    });

    let mut collected = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        for col in 0..batch.schema().column_count() {
            let data = batch.column(col).unwrap();
            collected.extend(data.iter().cloned());
        }
    }

    assert_eq!(collected, vec![json!(0), json!(2), json!(4)]);
}
