use std::sync::Arc;

use serde_json::json;

use crate::command::types::AggSpec;
use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowOperator, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};

use super::{AggregateOp, AggregateOpConfig};

fn flow_context(batch_size: usize) -> Arc<FlowContext> {
    let metrics = FlowMetrics::new();
    let pool = BatchPool::new(batch_size).unwrap();
    Arc::new(FlowContext::new(
        batch_size,
        pool,
        metrics,
        None::<&str>,
        FlowTelemetry::default(),
    ))
}

fn base_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".into(),
                logical_type: "String".into(),
            },
            ColumnSpec {
                name: "value".into(),
                logical_type: "Integer".into(),
            },
            ColumnSpec {
                name: "timestamp".into(),
                logical_type: "Timestamp".into(),
            },
            ColumnSpec {
                name: "event_id".into(),
                logical_type: "Integer".into(),
            },
        ])
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_op_computes_count_and_total() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "acct_event",
            &[
                ("context_id", "string"),
                ("value", "int"),
                ("timestamp", "int"),
            ],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("acct_event")
        .with_aggs(vec![
            AggSpec::Count { unique_field: None },
            AggSpec::Total {
                field: "value".into(),
            },
        ])
        .with_group_by(vec!["context_id"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let aggregate_plan = plan
        .aggregate_plan
        .clone()
        .expect("aggregate plan available");

    let plan_arc = Arc::new(plan.clone());

    let ctx = flow_context(4);
    let schema = base_schema();

    let mut builder = ctx.pool().acquire(Arc::clone(&schema));
    let rows = vec![
        ("ctx1", 10, 1_u64, 100_u64),
        ("ctx1", 15, 2_u64, 101_u64),
        ("ctx2", 7, 3_u64, 102_u64),
    ];
    for (context, value, ts, event_id) in rows {
        builder
            .push_row(&[
                ScalarValue::from(json!(context)),
                ScalarValue::from(json!(value)),
                ScalarValue::from(json!(ts)),
                ScalarValue::from(json!(event_id)),
            ])
            .unwrap();
    }
    let batch = builder.finish().unwrap();

    let (tx, rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));
    tx.send(Arc::new(batch)).await.unwrap();
    drop(tx);

    let op = AggregateOp::new(AggregateOpConfig {
        plan: Arc::clone(&plan_arc),
        aggregate: aggregate_plan.clone(),
    });

    let ctx_clone = Arc::clone(&ctx);
    tokio::spawn(async move {
        op.run(rx, out_tx, ctx_clone).await.unwrap();
    });

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        for row_idx in 0..batch.len() {
            let ctx_id = batch.column(0).unwrap()[row_idx].clone();
            let count = batch.column(1).unwrap()[row_idx].clone();
            let total = batch.column(2).unwrap()[row_idx].clone();
            results.push((ctx_id, count, total));
        }
    }

    results.sort_by(|a, b| {
        let left = a.0.as_str().unwrap_or("");
        let right = b.0.as_str().unwrap_or("");
        left.cmp(right)
    });

    assert_eq!(
        results,
        vec![
            (
                ScalarValue::from(json!("ctx1")),
                ScalarValue::from(json!(2)),
                ScalarValue::from(json!(25))
            ),
            (
                ScalarValue::from(json!("ctx2")),
                ScalarValue::from(json!(1)),
                ScalarValue::from(json!(7))
            )
        ]
    );
}
