use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::timeout;

use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::errors::QueryExecutionError;
use crate::engine::query::streaming::context::StreamingContext;
use crate::engine::query::streaming::merger::ShardFlowMerger;
use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};

async fn build_context(command: crate::command::types::Command) -> StreamingContext {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("payload", "object"),
                ("value", "int"),
            ],
        )
        .await
        .expect("schema defined");

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry_factory.registry())
        .create()
        .await;

    let passive_buffers = Arc::new(PassiveBufferSet::new(0));
    StreamingContext::new(Arc::new(plan), &passive_buffers, 8)
        .await
        .expect("context")
}

fn sample_schema(column_name: &str) -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: column_name.to_string(),
                logical_type: "Integer".to_string(),
            },
        ])
        .expect("schema"),
    )
}

async fn build_handle(
    schema: Arc<BatchSchema>,
    rows: Vec<Vec<serde_json::Value>>,
) -> ShardFlowHandle {
    let metrics = Arc::new(FlowMetrics::new());
    let (tx, rx) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let schema_for_task = Arc::clone(&schema);

    let task = tokio::spawn(async move {
        let pool = BatchPool::new(4).expect("pool");
        let mut builder = pool.acquire(schema_for_task);
        for row in rows {
            builder.push_row(&row).expect("push row");
        }
        if builder.len() > 0 {
            let batch = builder.finish().expect("batch");
            let _ = tx.send(batch).await;
        }
        drop(tx);
    });

    ShardFlowHandle::new(rx, schema, vec![task])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_orders_rows_when_ordering_requested() {
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .with_order_by("value", false)
        .create();
    let context = build_context(command).await;

    let schema = sample_schema("value");
    let handle_a = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-a"), json!(1)],
            vec![json!("ctx-b"), json!(4)],
        ],
    )
    .await;
    let handle_b = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-c"), json!(2)],
            vec![json!("ctx-d"), json!(5)],
        ],
    )
    .await;

    let merged = ShardFlowMerger::merge(&context, vec![handle_a, handle_b])
        .await
        .expect("merge");

    let value_idx = merged
        .schema
        .columns()
        .iter()
        .position(|col| col.name == "value")
        .expect("value column");

    let mut receiver = merged.receiver;
    let mut values = Vec::new();
    while let Some(batch) = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout")
    {
        let column = batch.column(value_idx).expect("value data");
        for value in column {
            if let Some(number) = value.as_i64() {
                values.push(number);
            }
        }
    }

    assert_eq!(values, vec![1, 2, 4, 5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_concatenates_when_unordered() {
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();
    let context = build_context(command).await;

    let schema = sample_schema("value");
    let handle_a = build_handle(Arc::clone(&schema), vec![vec![json!("ctx-a"), json!(2)]]).await;
    let handle_b = build_handle(Arc::clone(&schema), vec![vec![json!("ctx-b"), json!(7)]]).await;

    let merged = ShardFlowMerger::merge(&context, vec![handle_a, handle_b])
        .await
        .expect("merge");

    let value_idx = merged
        .schema
        .columns()
        .iter()
        .position(|col| col.name == "value")
        .expect("value column");

    let mut receiver = merged.receiver;
    let mut values = Vec::new();
    while let Some(batch) = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout")
    {
        let column = batch.column(value_idx).expect("value data");
        for value in column {
            if let Some(number) = value.as_i64() {
                values.push(number);
            }
        }
    }

    values.sort();
    assert_eq!(values, vec![2, 7]);
}

#[tokio::test]
async fn merge_infers_schema_when_handles_absent() {
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();
    let context = build_context(command).await;

    let merged = ShardFlowMerger::merge(&context, Vec::new())
        .await
        .expect("merge");

    let columns: HashSet<_> = merged
        .schema
        .columns()
        .iter()
        .map(|col| col.name.as_str())
        .collect();
    assert!(columns.contains("context_id"));
    assert!(columns.contains("event_type"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_fails_on_incompatible_schemas() {
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();
    let context = build_context(command).await;

    let schema_a = sample_schema("value");
    let schema_b = sample_schema("amount");

    let handle_a = build_handle(Arc::clone(&schema_a), vec![vec![json!("ctx-a"), json!(1)]]).await;
    let handle_b = build_handle(Arc::clone(&schema_b), vec![vec![json!("ctx-b"), json!(2)]]).await;

    let result = ShardFlowMerger::merge(&context, vec![handle_a, handle_b]).await;
    assert!(matches!(result, Err(QueryExecutionError::Aborted)));
}
