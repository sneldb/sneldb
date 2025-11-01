use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::flow::{
    BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowOperator, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;

use super::{ProjectOp, Projection};

fn make_context() -> Arc<FlowContext> {
    let metrics = FlowMetrics::new();
    let pool = crate::engine::core::read::flow::BatchPool::new(4).unwrap();
    Arc::new(FlowContext::new(
        4,
        pool,
        metrics,
        None::<&str>,
        FlowTelemetry::default(),
    ))
}

fn schema(columns: &[(&str, &str)]) -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(
            columns
                .iter()
                .map(|(name, ty)| ColumnSpec {
                    name: (*name).into(),
                    logical_type: (*ty).into(),
                })
                .collect(),
        )
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn project_op_reorders_and_drops_columns() {
    let ctx = make_context();
    let input_schema = schema(&[("a", "Integer"), ("b", "String"), ("c", "Integer")]);

    let projection_schema = schema(&[("c", "Integer"), ("a", "Integer")]);
    let projection = Projection {
        indices: vec![2, 0],
        schema: Arc::clone(&projection_schema),
    };

    let op = ProjectOp::new(projection);

    let (tx, rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    let mut builder = ctx.pool().acquire(input_schema.clone());
    for idx in 0..3 {
        builder
            .push_row(&[json!(idx), json!(format!("s{}", idx)), json!(idx * 10)])
            .unwrap();
    }
    tx.send(builder.finish().unwrap()).await.unwrap();
    drop(tx);

    let ctx_clone = Arc::clone(&ctx);
    tokio::spawn(async move {
        op.run(rx, out_tx, ctx_clone).await.unwrap();
    });

    let mut rows = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        for row_idx in 0..batch.len() {
            let c = batch.column(0).unwrap()[row_idx].clone();
            let a = batch.column(1).unwrap()[row_idx].clone();
            rows.push((c, a));
        }
    }

    assert_eq!(
        rows,
        vec![
            (json!(0), json!(0)),
            (json!(10), json!(1)),
            (json!(20), json!(2)),
        ]
    );
}
