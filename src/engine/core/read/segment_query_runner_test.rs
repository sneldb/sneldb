use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{ExecutionStep, Flusher, SegmentQueryRunner};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn segment_query_runner_returns_matching_events() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Create temp segment dir
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-007");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Setup schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields(
            "purchase",
            &[("context_id", "string"), ("status", "string")],
        )
        .await
        .unwrap();

    // Insert events to disk
    let event_ok = EventFactory::new()
        .with("event_type", "purchase")
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "ok"}))
        .create();
    let event_fail = EventFactory::new()
        .with("event_type", "purchase")
        .with("context_id", "ctx1")
        .with("payload", json!({"status": "fail"}))
        .create();

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![event_ok.clone(), event_fail])
        .create()
        .unwrap();

    let flusher = Flusher::new(memtable, 7, &segment_dir, Arc::clone(&registry));
    flusher.flush().await.expect("Flush failed");

    // Prepare query plan
    let query_cmd = CommandFactory::query()
        .with_event_type("purchase")
        .with_context_id("ctx1")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .with_segment_ids(vec!["shard-0/segment-007".into()])
        .create()
        .await;

    // Manually create execution steps from filter plans
    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|filter| ExecutionStep::new(filter.clone(), &plan))
        .collect();

    // Run SegmentQueryRunner
    let runner = SegmentQueryRunner::new(&plan, steps);
    let results = runner.run().await;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].payload.get("status").unwrap(), "ok");
}
