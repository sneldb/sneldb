use crate::command::types::{CompareOp, Expr};
use crate::engine::core::MemTableQuery;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn filters_events_from_memtable_correctly() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Step 1: Setup schema registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("signup", &[("status", "string")])
        .await
        .unwrap();

    // Step 2: Create query command with context and filter
    let command = CommandFactory::query()
        .with_event_type("signup")
        .with_context_id("ctx-a")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    // Step 3: Build query plan
    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Step 4: Create memtable with two events
    let event1 = EventFactory::new()
        .with("event_type", "signup")
        .with("context_id", "ctx-a")
        .with("payload", json!({ "status": "ok" }))
        .create();
    let event2 = EventFactory::new()
        .with("event_type", "signup")
        .with("context_id", "ctx-a")
        .with("payload", json!({ "status": "fail" }))
        .create();

    let memtable = MemTableFactory::new()
        .with_event(event1.clone())
        .with_event(event2)
        .create()
        .unwrap();

    // Step 5: Run MemTableQuery
    let query = MemTableQuery::new(&memtable, &plan);
    let result = query.query();

    // Step 6: Verify result
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].context_id, "ctx-a");
    assert_eq!(
        result[0].payload.get("status").and_then(|v| v.as_str()),
        Some("ok")
    );
}

#[tokio::test]
async fn memtable_query_respects_limit_zero() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt", &[("status", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("evt")
        .with_limit(0)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    let memtable = MemTableFactory::new()
        .with_events(vec![
            EventFactory::new()
                .with("event_type", "evt")
                .with("payload", json!({"status":"ok"}))
                .create(),
            EventFactory::new()
                .with("event_type", "evt")
                .with("payload", json!({"status":"ok"}))
                .create(),
        ])
        .create()
        .unwrap();

    let result = MemTableQuery::new(&memtable, &plan).query();
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn memtable_query_enforces_limit_mid_iteration() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("signup_limit", &[("status", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("signup_limit")
        .with_limit(3)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Build 10 matching events to ensure mid-iteration break happens
    let mut builder = MemTableFactory::new();
    for i in 0..10 {
        let e = EventFactory::new()
            .with("event_type", "signup_limit")
            .with("context_id", format!("c-{}", i))
            .with("payload", json!({"status":"ok"}))
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    let result = MemTableQuery::new(&memtable, &plan).query();
    assert_eq!(result.len(), 3);
}
