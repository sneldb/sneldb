use std::collections::HashSet;
use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::flow::{BatchPool, FlowContext, FlowMetrics, FlowTelemetry};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};

use crate::engine::core::read::flow::shard_pipeline::{build_memtable_flow, build_segment_flow};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_emits_batches() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "flow_event",
            &[
                ("context_id", "string"),
                ("value", "int"),
                ("timestamp", "int"),
            ],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("timestamp", 10)
            .with("event_type", "flow_event")
            .with("payload", json!({"value": 5}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx2")
            .with("timestamp", 11)
            .with("event_type", "flow_event")
            .with("payload", json!({"value": 7}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let plan_arc = Arc::new(plan.clone());
    let ctx = Arc::new(FlowContext::new(
        4,
        BatchPool::new(4).unwrap(),
        FlowMetrics::new(),
        None::<&str>,
        FlowTelemetry::default(),
    ));

    let handle = build_memtable_flow(
        plan_arc,
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let ctx_index = handle
        .schema
        .columns()
        .iter()
        .position(|c| c.name == "context_id")
        .expect("context_id column present");

    let mut receiver = handle.receiver;
    let mut contexts: HashSet<String> = HashSet::new();
    while let Some(batch) = receiver.recv().await {
        let column = batch.column(ctx_index).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_str() {
                contexts.insert(value.to_string());
            }
        }
    }

    assert!(contexts.contains("ctx1"));
    assert!(contexts.contains("ctx2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_emits_batches() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "flow_event",
            &[
                ("context_id", "string"),
                ("value", "int"),
                ("timestamp", "int"),
            ],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("context_id", "seg_ctx1")
            .with("timestamp", 42)
            .with("event_type", "flow_event")
            .with("value", 11)
            .create(),
        EventFactory::new()
            .with("context_id", "seg_ctx2")
            .with("timestamp", 43)
            .with("event_type", "flow_event")
            .with("value", 13)
            .create(),
    ];

    let plan_arc = Arc::new(plan.clone());
    let ctx = Arc::new(FlowContext::new(
        4,
        BatchPool::new(4).unwrap(),
        FlowMetrics::new(),
        None::<&str>,
        FlowTelemetry::default(),
    ));

    let handle = build_segment_flow(plan_arc, events, Arc::clone(&ctx))
        .await
        .unwrap()
        .expect("segment flow available");

    let ctx_index = handle
        .schema
        .columns()
        .iter()
        .position(|c| c.name == "context_id")
        .expect("context_id column present");

    let mut receiver = handle.receiver;
    let mut contexts: HashSet<String> = HashSet::new();
    while let Some(batch) = receiver.recv().await {
        let column = batch.column(ctx_index).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_str() {
                contexts.insert(value.to_string());
            }
        }
    }

    assert!(contexts.contains("seg_ctx1"));
    assert!(contexts.contains("seg_ctx2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_handles_empty_events() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("context_id", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let plan_arc = Arc::new(plan);
    let ctx = Arc::new(FlowContext::new(
        4,
        BatchPool::new(4).unwrap(),
        FlowMetrics::new(),
        None::<&str>,
        FlowTelemetry::default(),
    ));

    let result = build_segment_flow(plan_arc, vec![], Arc::clone(&ctx))
        .await
        .unwrap();

    assert!(result.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_handles_limit_override() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "flow_event",
            &[
                ("context_id", "string"),
                ("value", "int"),
                ("timestamp", "int"),
            ],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_limit(10)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events: Vec<_> = (0..20)
        .map(|i| {
            EventFactory::new()
                .with("context_id", format!("ctx{}", i))
                .with("timestamp", i)
                .with("event_type", "flow_event")
                .with("payload", json!({"value": i}))
                .create()
        })
        .collect();

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let plan_arc = Arc::new(plan);
    let ctx = Arc::new(FlowContext::new(
        4,
        BatchPool::new(4).unwrap(),
        FlowMetrics::new(),
        None::<&str>,
        FlowTelemetry::default(),
    ));

    let handle = build_memtable_flow(
        Arc::clone(&plan_arc),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        Some(5),
    )
    .await
    .unwrap();

    let mut receiver = handle.receiver;
    let mut count = 0;
    while let Some(batch) = receiver.recv().await {
        count += batch.len();
    }

    assert_eq!(count, 5);
}
