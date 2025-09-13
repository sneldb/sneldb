use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{Flusher, QueryExecution};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tracing::info;

#[tokio::test]
async fn executes_query_with_memtable_and_segment() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: Schema registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_with_memtable_and_segment";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Build query command
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    // Build QueryPlan
    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create events and MemTable
    let event1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "ok"}))
        .create();
    let event2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "fail"}))
        .create();

    let memtable = MemTableFactory::new()
        .with_event(event1.clone())
        .with_event(event2.clone())
        .create()
        .unwrap();

    // Run the query execution
    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();

    // Verify: Only the event with status = "ok" should match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].payload.get("status").unwrap(), "ok");
}

#[tokio::test]
async fn executes_query_with_segment_only() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_with_segment_only";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Build query command
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    // Build QueryPlan
    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // No MemTable — test segment-only path
    let mut execution = QueryExecution::new(&plan);
    let results = execution.run().await.unwrap();

    // We expect no results since segments are empty
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn executes_query_with_memtable_but_no_matches() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_with_memtable_but_no_matches";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Build query command
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("missing"),
        })
        .create();

    // Build QueryPlan
    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Insert events with non-matching status
    let event = EventFactory::new()
        .with("event_type", event_type)
        .with("payload", json!({ "status": "ok" }))
        .create();

    let memtable = MemTableFactory::new().with_event(event).create().unwrap();

    // Execute query
    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();

    // No match expected
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn executes_query_with_only_segment_match() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: temp segment dir
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-007");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Setup: schema and registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_with_only_segment_match";
    schema_factory
        .define_with_fields(
            event_type,
            &[("context_id", "string"), ("status", "string")],
        )
        .await
        .unwrap();

    // Store 2 events: one matches query
    let event1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "ok"}))
        .create();
    let event2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({"status": "fail"}))
        .create();
    let event3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "ok"}))
        .create();

    let memtable = crate::test_helpers::factories::MemTableFactory::new()
        .with_events(vec![event1.clone(), event2.clone(), event3.clone()])
        .create()
        .unwrap();

    let flusher = Flusher::new(memtable, 7, &segment_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // Query for events with status = "ok"
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    info!("Query command: {:?}", query_cmd);

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .with_segment_ids(vec!["shard-0/segment-007".into()])
        .create()
        .await;

    // Execute the query (no MemTable involved)
    let mut execution = QueryExecution::new(&plan);
    let results = execution.run().await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].payload.get("status").unwrap(), "ok");
    assert_eq!(results[1].payload.get("status").unwrap(), "ok");
}
