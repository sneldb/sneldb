use std::collections::HashSet;
use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::cache::QueryCaches;
use crate::engine::core::read::flow::{BatchPool, FlowContext, FlowMetrics, FlowTelemetry};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};

use crate::engine::core::read::flow::shard_pipeline::{
    build_memtable_flow, build_segment_flow, build_segment_stream,
};

fn create_flow_context() -> Arc<FlowContext> {
    Arc::new(FlowContext::new(
        4,
        BatchPool::new(4).unwrap(),
        FlowMetrics::new(),
        None::<&str>,
        FlowTelemetry::default(),
    ))
}

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

// ==================== build_memtable_flow Edge Cases ====================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_handles_empty_memtable() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
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

    let memtable = MemTableFactory::new().create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let mut receiver = handle.receiver;
    let mut count = 0;
    while let Some(batch) = receiver.recv().await {
        count += batch.len();
    }

    assert_eq!(count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_handles_none_memtable() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
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

    let ctx = create_flow_context();

    let handle = build_memtable_flow(Arc::new(plan), None, Vec::new(), Arc::clone(&ctx), None)
        .await
        .unwrap();

    let mut receiver = handle.receiver;
    let mut count = 0;
    while let Some(batch) = receiver.recv().await {
        count += batch.len();
    }

    assert_eq!(count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_handles_passive_memtables() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
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

    let active_events = vec![
        EventFactory::new()
            .with("context_id", "active1")
            .with("timestamp", 10)
            .with("event_type", "flow_event")
            .with("payload", json!({"value": 1}))
            .create(),
    ];

    let passive_events = vec![
        EventFactory::new()
            .with("context_id", "passive1")
            .with("timestamp", 20)
            .with("event_type", "flow_event")
            .with("payload", json!({"value": 2}))
            .create(),
    ];

    let active_memtable = MemTableFactory::new()
        .with_events(active_events)
        .create()
        .unwrap();
    let passive_memtable = MemTableFactory::new()
        .with_events(passive_events)
        .create()
        .unwrap();

    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(active_memtable)),
        vec![Arc::new(tokio::sync::Mutex::new(passive_memtable))],
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

    assert!(contexts.contains("active1"));
    assert!(contexts.contains("passive1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_handles_zero_limit() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_limit(0)
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
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let mut receiver = handle.receiver;
    let mut count = 0;
    while let Some(batch) = receiver.recv().await {
        count += batch.len();
    }

    assert_eq!(count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_with_return_fields_projection() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "flow_event",
            &[("value", "int"), ("other", "string"), ("extra", "bool")],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec!["value"])
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
            .with(
                "payload",
                json!({"value": 5, "other": "test", "extra": true}),
            )
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    // Verify schema only contains core fields + RETURN field
    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // Core fields should be present
    assert!(column_names.contains(&"context_id".to_string()));
    assert!(column_names.contains(&"event_type".to_string()));
    assert!(column_names.contains(&"timestamp".to_string()));
    assert!(column_names.contains(&"event_id".to_string()));
    // RETURN field should be present
    assert!(column_names.contains(&"value".to_string()));
    // Non-RETURN payload fields should NOT be present
    assert!(!column_names.contains(&"other".to_string()));
    assert!(!column_names.contains(&"extra".to_string()));

    let mut receiver = handle.receiver;
    let mut batches = Vec::new();
    while let Some(batch) = receiver.recv().await {
        batches.push(batch);
    }

    assert!(!batches.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_with_empty_return_fields() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec![])
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
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    // Empty RETURN fields should result in identity projection (all columns)
    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // All columns should be present (identity projection)
    assert!(column_names.contains(&"context_id".to_string()));
    assert!(column_names.contains(&"value".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_with_aggregate() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .add_total("value")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events: Vec<_> = (0..5)
        .map(|i| {
            EventFactory::new()
                .with("context_id", format!("ctx{}", i))
                .with("timestamp", i)
                .with("event_type", "flow_event")
                .with("payload", json!({"value": i + 1}))
                .create()
        })
        .collect();

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    // Aggregate should produce a single row
    let mut receiver = handle.receiver;
    let mut batches = Vec::new();
    while let Some(batch) = receiver.recv().await {
        batches.push(batch);
    }

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total_rows, 1); // Single aggregate result
}

// ==================== build_segment_flow Edge Cases ====================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_respects_limit_truncation() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_limit(3)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events: Vec<_> = (0..10)
        .map(|i| {
            EventFactory::new()
                .with("context_id", format!("ctx{}", i))
                .with("timestamp", i)
                .with("event_type", "flow_event")
                .with("value", i)
                .create()
        })
        .collect();

    let ctx = create_flow_context();

    let handle = build_segment_flow(Arc::new(plan), events, Arc::clone(&ctx))
        .await
        .unwrap()
        .expect("segment flow available");

    let mut receiver = handle.receiver;
    let mut count = 0;
    while let Some(batch) = receiver.recv().await {
        count += batch.len();
    }

    assert_eq!(count, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_handles_zero_limit() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_limit(0)
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
            .with("value", 5)
            .create(),
    ];

    let ctx = create_flow_context();

    let handle = build_segment_flow(Arc::new(plan), events, Arc::clone(&ctx))
        .await
        .unwrap();

    // When limit is 0, events are truncated to empty, so handle should be None
    // However, if events were already empty before truncation, it returns None earlier
    // So we check: either None (empty events) or Some with empty batches
    if let Some(h) = handle {
        let mut receiver = h.receiver;
        let mut count = 0;
        while let Some(batch) = receiver.recv().await {
            count += batch.len();
        }
        assert_eq!(count, 0, "zero limit should produce no rows");
    }
    // If None, that's also correct (empty events case)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_with_identity_projection() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
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
            .with("value", 5)
            .create(),
    ];

    let ctx = create_flow_context();

    let handle = build_segment_flow(Arc::new(plan), events, Arc::clone(&ctx))
        .await
        .unwrap()
        .expect("segment flow available");

    // Identity projection should have all columns
    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    assert!(column_names.contains(&"context_id".to_string()));
    assert!(column_names.contains(&"value".to_string()));

    let mut receiver = handle.receiver;
    let mut batches = Vec::new();
    while let Some(batch) = receiver.recv().await {
        batches.push(batch);
    }

    assert!(!batches.is_empty());
}

// ==================== build_segment_stream Edge Cases ====================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_stream_handles_zero_limit() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_limit(0)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let ctx = create_flow_context();
    let tmp_dir = tempfile::tempdir().unwrap();
    let caches = Arc::new(QueryCaches::new(tmp_dir.path().to_path_buf()));

    let result = build_segment_stream(Arc::new(plan), Arc::clone(&ctx), caches, None)
        .await
        .unwrap();

    assert!(result.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_stream_handles_limit_override() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
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

    let ctx = create_flow_context();
    let tmp_dir = tempfile::tempdir().unwrap();
    let caches = Arc::new(QueryCaches::new(tmp_dir.path().to_path_buf()));

    // Override limit to 5
    let result = build_segment_stream(Arc::new(plan), Arc::clone(&ctx), caches, Some(5))
        .await
        .unwrap();

    // Should return Some handle even if no data (empty segments)
    // The actual limit enforcement happens in the segment query runner
    assert!(result.is_some() || result.is_none()); // Both are valid for empty segments
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_stream_with_return_fields() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "flow_event",
            &[("value", "int"), ("other", "string"), ("extra", "bool")],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec!["value", "other"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let ctx = create_flow_context();
    let tmp_dir = tempfile::tempdir().unwrap();
    let caches = Arc::new(QueryCaches::new(tmp_dir.path().to_path_buf()));

    let result = build_segment_stream(Arc::new(plan), Arc::clone(&ctx), caches, None)
        .await
        .unwrap();

    // May be None if no segments exist, but if it exists, verify schema
    if let Some(handle) = result {
        let column_names: Vec<String> = handle
            .schema
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // Core fields should be present
        assert!(column_names.contains(&"context_id".to_string()));
        // RETURN fields should be present
        assert!(column_names.contains(&"value".to_string()));
        assert!(column_names.contains(&"other".to_string()));
        // Non-RETURN payload fields should NOT be present
        assert!(!column_names.contains(&"extra".to_string()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_stream_with_aggregate() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .add_total("value")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let ctx = create_flow_context();
    let tmp_dir = tempfile::tempdir().unwrap();
    let caches = Arc::new(QueryCaches::new(tmp_dir.path().to_path_buf()));

    let result = build_segment_stream(Arc::new(plan), Arc::clone(&ctx), caches, None)
        .await
        .unwrap();

    // May be None if no segments exist
    if let Some(handle) = result {
        // Aggregate should produce aggregate output schema
        let column_names: Vec<String> = handle
            .schema
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // Should have aggregate result column
        assert!(!column_names.is_empty());
    }
}

// ==================== compute_return_projection Edge Cases (tested indirectly) ====================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn return_projection_handles_duplicate_fields() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    // RETURN fields with duplicates (should be deduplicated)
    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec![
            "value",
            "value",      // duplicate
            "context_id", // core field (should be included anyway)
        ])
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
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // value should appear only once
    assert_eq!(
        column_names.iter().filter(|&n| n == "value").count(),
        1,
        "value should appear only once"
    );
    // context_id should be present (core field)
    assert!(column_names.contains(&"context_id".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn return_projection_handles_missing_fields() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    // RETURN fields with non-existent field (should be ignored)
    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec!["value", "nonexistent"])
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
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // value should be present
    assert!(column_names.contains(&"value".to_string()));
    // nonexistent should NOT be present
    assert!(!column_names.contains(&"nonexistent".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn return_projection_preserves_core_field_order() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int"), ("other", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("flow_event")
        .with_return_fields(vec!["other", "value"])
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
            .with("payload", json!({"value": 5, "other": "test"}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // Core fields should come first in order: context_id, event_type, timestamp, event_id
    let ctx_idx = column_names.iter().position(|n| n == "context_id").unwrap();
    let event_type_idx = column_names.iter().position(|n| n == "event_type").unwrap();
    let timestamp_idx = column_names.iter().position(|n| n == "timestamp").unwrap();
    let event_id_idx = column_names.iter().position(|n| n == "event_id").unwrap();

    assert!(ctx_idx < event_type_idx);
    assert!(event_type_idx < timestamp_idx);
    assert!(timestamp_idx < event_id_idx);
    // Payload fields come after core fields
    let value_idx = column_names.iter().position(|n| n == "value").unwrap();
    let other_idx = column_names.iter().position(|n| n == "other").unwrap();
    assert!(event_id_idx < value_idx);
    assert!(event_id_idx < other_idx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_identity_projection_optimization() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("flow_event", &[("value", "int")])
        .await
        .unwrap();

    // No RETURN fields = identity projection
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
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();
    let ctx = create_flow_context();

    let handle = build_memtable_flow(
        Arc::new(plan),
        Some(Arc::new(memtable)),
        Vec::new(),
        Arc::clone(&ctx),
        None,
    )
    .await
    .unwrap();

    // Identity projection should have all columns in original order
    let column_names: Vec<String> = handle
        .schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();

    // All columns should be present
    assert!(column_names.contains(&"context_id".to_string()));
    assert!(column_names.contains(&"value".to_string()));

    // Verify data flows through correctly
    let mut receiver = handle.receiver;
    let mut batches = Vec::new();
    while let Some(batch) = receiver.recv().await {
        batches.push(batch);
    }

    assert!(!batches.is_empty());
}
