use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::merge::streaming::{
    OrderedStreamMerger, UnorderedStreamMerger,
};
use crate::command::types::Command;
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::SchemaRegistryFactory;
use serde_json::json;
use std::sync::Arc;

fn sample_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            crate::engine::core::read::result::ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            crate::engine::core::read::result::ColumnSpec {
                name: "value".to_string(),
                logical_type: "Integer".to_string(),
            },
        ])
        .expect("schema"),
    )
}

fn create_context() -> QueryContext<'static> {
    let command = Box::leak(Box::new(Command::Query {
        event_type: "test".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    }));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    QueryContext::new(command, manager, registry)
}

async fn build_handle(
    schema: Arc<BatchSchema>,
    rows: Vec<Vec<serde_json::Value>>,
) -> ShardFlowHandle {
    let metrics = FlowMetrics::new();
    let (tx, rx) = FlowChannel::bounded(16, Arc::clone(&metrics));

    let pool = BatchPool::new(16).expect("batch pool");
    let mut builder = pool.acquire(Arc::clone(&schema));
    for row in rows.into_iter() {
        builder.push_row(&row).expect("push row");
    }
    let batch = builder.finish().expect("finish batch");

    let send_task = tokio::spawn(async move {
        let _ = tx.send(batch).await;
    });

    ShardFlowHandle::new(rx, schema, vec![send_task])
}

#[tokio::test]
async fn unordered_merger_combines_rows() {
    let schema = sample_schema();
    let handle_a = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-a"), json!(1)],
            vec![json!("ctx-b"), json!(2)],
        ],
    )
    .await;
    let handle_b = build_handle(Arc::clone(&schema), vec![vec![json!("ctx-c"), json!(3)]]).await;

    let ctx = create_context();
    let merger = UnorderedStreamMerger::new();
    let mut stream = merger
        .merge(&ctx, vec![handle_a, handle_b])
        .expect("stream created");

    let mut observed = Vec::new();
    while let Some(batch) = stream.recv().await {
        for row in 0..batch.len() {
            let ctx_val = batch.column(0).unwrap()[row]
                .as_str()
                .expect("ctx")
                .to_string();
            let value = batch.column(1).unwrap()[row].as_i64().expect("value");
            observed.push((ctx_val, value));
        }
    }

    assert_eq!(
        observed,
        vec![
            ("ctx-a".to_string(), 1),
            ("ctx-b".to_string(), 2),
            ("ctx-c".to_string(), 3),
        ]
    );
}

#[tokio::test]
async fn unordered_merger_rejects_schema_mismatch() {
    let schema_a = Arc::new(
        BatchSchema::new(vec![crate::engine::core::read::result::ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema a"),
    );
    let schema_b = Arc::new(
        BatchSchema::new(vec![crate::engine::core::read::result::ColumnSpec {
            name: "other".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema b"),
    );

    let handle_a = build_handle(Arc::clone(&schema_a), vec![vec![json!("ctx-a")]]).await;
    let handle_b = build_handle(Arc::clone(&schema_b), vec![vec![json!("ctx-b")]]).await;

    let ctx = create_context();
    let merger = UnorderedStreamMerger::new();
    match merger.merge(&ctx, vec![handle_a, handle_b]) {
        Ok(_) => panic!("expected schema mismatch"),
        Err(err) => assert!(err.contains("schemas differ")),
    }
}

#[tokio::test]
async fn ordered_merger_sorts_rows() {
    let schema = sample_schema();
    let handle_a = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-a"), json!(1)],
            vec![json!("ctx-b"), json!(2)],
        ],
    )
    .await;
    let handle_b = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-c"), json!(3)],
            vec![json!("ctx-d"), json!(4)],
        ],
    )
    .await;

    let ctx = create_context();
    let merger = OrderedStreamMerger::new("value".to_string(), true, None, None);
    let mut stream = merger
        .merge(&ctx, vec![handle_a, handle_b])
        .expect("stream produced");

    let mut values = Vec::new();
    while let Some(batch) = stream.recv().await {
        let column = batch.column(1).unwrap();
        for row_idx in 0..batch.len() {
            values.push(column[row_idx].as_i64().unwrap());
        }
    }
    assert_eq!(values, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn ordered_merger_applies_offset_and_limit() {
    let schema = sample_schema();
    let handle_a = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-a"), json!(1)],
            vec![json!("ctx-b"), json!(2)],
        ],
    )
    .await;
    let handle_b = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-c"), json!(3)],
            vec![json!("ctx-d"), json!(4)],
        ],
    )
    .await;

    let ctx = create_context();
    let merger = OrderedStreamMerger::new("value".to_string(), true, Some(2), Some(1));
    let mut stream = merger
        .merge(&ctx, vec![handle_a, handle_b])
        .expect("stream produced");

    let mut values = Vec::new();
    while let Some(batch) = stream.recv().await {
        let column = batch.column(1).unwrap();
        for row_idx in 0..batch.len() {
            values.push(column[row_idx].as_i64().unwrap());
        }
    }
    assert_eq!(values, vec![2, 3]);
}

#[tokio::test]
async fn ordered_merger_handles_empty_handles() {
    let ctx = create_context();
    let merger = OrderedStreamMerger::new("value".to_string(), true, None, None);

    match merger.merge(&ctx, vec![]) {
        Ok(_) => panic!("expected error for empty handles"),
        Err(err) => assert!(err.contains("no shard handles")),
    }
}

#[tokio::test]
async fn ordered_merger_rejects_missing_field() {
    let schema = sample_schema();
    let handle_a = build_handle(Arc::clone(&schema), vec![vec![json!("ctx-a"), json!(1)]]).await;

    let ctx = create_context();
    let merger = OrderedStreamMerger::new("nonexistent".to_string(), true, None, None);

    match merger.merge(&ctx, vec![handle_a]) {
        Ok(_) => panic!("expected error for missing field"),
        Err(err) => assert!(err.contains("not found in stream schema")),
    }
}

#[tokio::test]
async fn ordered_merger_rejects_schema_mismatch() {
    let schema_a = Arc::new(
        BatchSchema::new(vec![crate::engine::core::read::result::ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema a"),
    );
    let schema_b = Arc::new(
        BatchSchema::new(vec![crate::engine::core::read::result::ColumnSpec {
            name: "other".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema b"),
    );

    let handle_a = build_handle(Arc::clone(&schema_a), vec![vec![json!("ctx-a")]]).await;
    let handle_b = build_handle(Arc::clone(&schema_b), vec![vec![json!("ctx-b")]]).await;

    let ctx = create_context();
    let merger = OrderedStreamMerger::new("context_id".to_string(), true, None, None);
    match merger.merge(&ctx, vec![handle_a, handle_b]) {
        Ok(_) => panic!("expected schema mismatch"),
        Err(err) => assert!(err.contains("schemas differ")),
    }
}

#[tokio::test]
async fn unordered_merger_handles_empty_handles() {
    let ctx = create_context();
    let merger = UnorderedStreamMerger::new();

    match merger.merge(&ctx, vec![]) {
        Ok(_) => panic!("expected error for empty handles"),
        Err(err) => assert!(err.contains("no shards produced schema") || err.contains("shards")),
    }
}

#[tokio::test]
async fn unordered_merger_handles_single_handle() {
    let schema = sample_schema();
    let handle = build_handle(
        Arc::clone(&schema),
        vec![
            vec![json!("ctx-a"), json!(1)],
            vec![json!("ctx-b"), json!(2)],
        ],
    )
    .await;

    let ctx = create_context();
    let merger = UnorderedStreamMerger::new();
    let mut stream = merger.merge(&ctx, vec![handle]).expect("stream created");

    let mut count = 0;
    while let Some(batch) = stream.recv().await {
        count += batch.len();
    }
    assert_eq!(count, 2);
}
