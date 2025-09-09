use crate::command::types::{CompareOp, Expr};
use crate::engine::core::MemTableQueryRunner;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn runs_memtable_query_runner_and_returns_matching_events() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup schema and registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("order", &[("state", "string")])
        .await
        .unwrap();

    // Create query command with condition on "state"
    let command = CommandFactory::query()
        .with_event_type("order")
        .with_context_id("ctx-123")
        .with_where_clause(Expr::Compare {
            field: "state".into(),
            op: CompareOp::Eq,
            value: json!("shipped"),
        })
        .create();

    // Build query plan
    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create matching and non-matching events
    let matching_event = EventFactory::new()
        .with("event_type", "order")
        .with("context_id", "ctx-123")
        .with("payload", json!({"state": "shipped"}))
        .create();

    let non_matching_event = EventFactory::new()
        .with("event_type", "order")
        .with("context_id", "ctx-123")
        .with("payload", json!({"state": "pending"}))
        .create();

    let memtable = MemTableFactory::new()
        .with_event(matching_event.clone())
        .with_event(non_matching_event)
        .create()
        .unwrap();

    // Run the MemTableQueryRunner
    let runner = MemTableQueryRunner::new(Some(&memtable), None, &plan);
    let result = runner.run().await;

    // Verify: Only the "shipped" event is returned
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].payload.get("state").unwrap(), "shipped");
}

#[tokio::test]
async fn returns_empty_when_memtable_is_none() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("activity", &[("status", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query().with_event_type("activity").create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    let runner = MemTableQueryRunner::new(None, None, &plan);
    let result = runner.run().await;

    // Verify: result is empty when no memtable provided
    assert!(result.is_empty());
}
