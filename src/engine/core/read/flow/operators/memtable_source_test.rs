use std::sync::Arc;

use serde_json::json;

use crate::command::types::{CompareOp, Expr};
use crate::engine::core::read::flow::{
    BatchPool, FlowChannel, FlowContext, FlowMetrics, FlowSource, FlowTelemetry,
};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};

use super::{MemTableSource, MemTableSourceConfig};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_emits_all_rows() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields(
            "test_event",
            &[
                ("context_id", "string"),
                ("value", "int"),
                ("timestamp", "int"),
            ],
        )
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("timestamp", 1)
            .with("event_type", "test_event")
            .with("payload", json!({"value": 10}))
            .create(),
        EventFactory::new()
            .with("context_id", "ctx2")
            .with("timestamp", 2)
            .with("event_type", "test_event")
            .with("payload", json!({"value": 20}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_respects_limit() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
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
                .with("event_type", "test_event")
                .with("timestamp", i)
                .with("payload", json!({"value": i}))
                .create()
        })
        .collect();

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_respects_limit_override() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
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
                .with("event_type", "test_event")
                .with("timestamp", i)
                .with("payload", json!({"value": i}))
                .create()
        })
        .collect();

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: Some(5),
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_filters_rows() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("status", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 1)
            .with("payload", json!({"status": "ok"}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 2)
            .with("payload", json!({"status": "fail"}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 3)
            .with("payload", json!({"status": "ok"}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_handles_empty_memtable() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let memtable = MemTableFactory::new().create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_handles_no_memtable() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: None,
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_handles_passive_memtables() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let active_events = vec![
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 1)
            .with("payload", json!({"value": 10}))
            .create(),
    ];

    let passive_events = vec![
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 2)
            .with("payload", json!({"value": 20}))
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

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(active_memtable)),
        passive_memtables: vec![Arc::new(tokio::sync::Mutex::new(passive_memtable))],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_orders_results() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_order_by("timestamp", false)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 3)
            .with("payload", json!({"value": 30}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 1)
            .with("payload", json!({"value": 10}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 2)
            .with("payload", json!({"value": 20}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut timestamps = Vec::new();
    let mut ts_col_idx = None;

    while let Some(batch) = rx.recv().await {
        if ts_col_idx.is_none() {
            ts_col_idx = batch
                .schema()
                .columns()
                .iter()
                .position(|c| c.name == "timestamp");
        }

        if let Some(idx) = ts_col_idx {
            let column = batch.column(idx).unwrap();
            for row_idx in 0..batch.len() {
                if let Some(ts) = column[row_idx].as_u64() {
                    timestamps.push(ts);
                }
            }
        }
    }

    assert_eq!(timestamps, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_source_orders_results_descending() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_order_by("timestamp", true)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let events = vec![
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 1)
            .with("payload", json!({"value": 10}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 3)
            .with("payload", json!({"value": 30}))
            .create(),
        EventFactory::new()
            .with("event_type", "test_event")
            .with("timestamp", 2)
            .with("payload", json!({"value": 20}))
            .create(),
    ];

    let memtable = MemTableFactory::new().with_events(events).create().unwrap();

    let config = MemTableSourceConfig {
        plan: Arc::new(plan),
        memtable: Some(Arc::new(memtable)),
        passive_memtables: vec![],
        limit_override: None,
        mandatory_columns: vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ],
    };

    let source = MemTableSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut timestamps = Vec::new();
    let mut ts_col_idx = None;

    while let Some(batch) = rx.recv().await {
        if ts_col_idx.is_none() {
            ts_col_idx = batch
                .schema()
                .columns()
                .iter()
                .position(|c| c.name == "timestamp");
        }

        if let Some(idx) = ts_col_idx {
            let column = batch.column(idx).unwrap();
            for row_idx in 0..batch.len() {
                if let Some(ts) = column[row_idx].as_u64() {
                    timestamps.push(ts);
                }
            }
        }
    }

    assert_eq!(timestamps, vec![3, 2, 1]);
}

#[tokio::test]
async fn compute_columns_includes_mandatory_columns() {
    let registry = SchemaRegistryFactory::new();
    registry
        .define_with_fields("test_event", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.registry())
        .create()
        .await;

    let mandatory = vec!["context_id".to_string(), "timestamp".to_string()];
    let columns = MemTableSource::compute_columns(&plan, &mandatory)
        .await
        .unwrap();

    let column_names: Vec<_> = columns.iter().map(|c| c.name.clone()).collect();
    assert!(column_names.contains(&"context_id".to_string()));
    assert!(column_names.contains(&"timestamp".to_string()));
}
