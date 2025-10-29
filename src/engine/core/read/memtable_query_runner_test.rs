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
    let passives: Vec<&Arc<tokio::sync::Mutex<crate::engine::core::MemTable>>> = Vec::new();
    let runner = MemTableQueryRunner::new(Some(&memtable), &passives, &plan);
    let result = runner.run().await;

    // Verify: Only the "shipped" event is returned
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].payload.get("state").and_then(|v| v.as_str()),
        Some("shipped")
    );
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

    let passives: Vec<&Arc<tokio::sync::Mutex<crate::engine::core::MemTable>>> = Vec::new();
    let runner = MemTableQueryRunner::new(None, &passives, &plan);
    let result = runner.run().await;

    // Verify: result is empty when no memtable provided
    assert!(result.is_empty());
}

/// Tests that LIMIT alone truncates memtable results
#[tokio::test]
async fn respects_limit_without_order_by() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("limit_test", &[("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("limit_test")
        .with_limit(3) // LIMIT 3, no ORDER BY
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create 10 matching events
    let mut builder = MemTableFactory::new();
    for i in 0..10 {
        let e = EventFactory::new()
            .with("event_type", "limit_test")
            .with("context_id", format!("ctx-{}", i))
            .with("payload", json!({"id": i}))
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    let passives: Vec<&Arc<tokio::sync::Mutex<crate::engine::core::MemTable>>> = Vec::new();
    let runner = MemTableQueryRunner::new(Some(&memtable), &passives, &plan).with_limit(Some(3)); // Explicitly set limit

    let result = runner.run().await;

    // Should truncate to 3 events
    assert_eq!(result.len(), 3);
}

/// Tests that LIMIT is deferred when ORDER BY is present
#[tokio::test]
async fn returns_all_events_when_order_by_present() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("order_test", &[("score", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("order_test")
        .with_limit(3) // LIMIT 3 WITH ORDER BY
        .with_order_by("score", false) // ASC
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create 10 matching events with different scores
    let mut builder = MemTableFactory::new();
    for i in 0..10 {
        let e = EventFactory::new()
            .with("event_type", "order_test")
            .with("context_id", format!("ctx-{}", i))
            .with("payload", json!({"score": 10 - i})) // Reverse order
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    let passives: Vec<&Arc<tokio::sync::Mutex<crate::engine::core::MemTable>>> = Vec::new();
    let runner = MemTableQueryRunner::new(Some(&memtable), &passives, &plan).with_limit(Some(3)); // Set limit

    let result = runner.run().await;

    // Should return ALL 10 events (limit deferred) and sorted
    assert_eq!(
        result.len(),
        10,
        "Should return all events when ORDER BY present"
    );

    // Verify they're sorted in ascending order
    let mut prev_score = 0;
    for event in &result {
        let score = event.payload.get("score").and_then(|v| v.as_i64()).unwrap();
        assert!(score >= prev_score, "Events should be sorted by score ASC");
        prev_score = score;
    }
}

/// Tests ORDER BY with descending sort
#[tokio::test]
async fn sorts_events_descending_when_order_by_desc() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("desc_test", &[("rank", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("desc_test")
        .with_order_by("rank", true) // DESC
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create events with ranks in random order
    let mut builder = MemTableFactory::new();
    for rank in vec![5, 2, 8, 1, 9, 3] {
        let e = EventFactory::new()
            .with("event_type", "desc_test")
            .with("context_id", format!("ctx-{}", rank))
            .with("payload", json!({"rank": rank}))
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    let passives: Vec<&Arc<tokio::sync::Mutex<crate::engine::core::MemTable>>> = Vec::new();
    let runner = MemTableQueryRunner::new(Some(&memtable), &passives, &plan);
    let result = runner.run().await;

    assert_eq!(result.len(), 6);

    // Verify descending order
    let mut prev_rank = i64::MAX;
    for event in &result {
        let rank = event.payload.get("rank").and_then(|v| v.as_i64()).unwrap();
        assert!(rank <= prev_rank, "Events should be sorted by rank DESC");
        prev_rank = rank;
    }
}

/// Tests that passive memtables are queried correctly
#[tokio::test]
async fn queries_passive_memtables_with_limit() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("passive_test", &[("value", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("passive_test")
        .with_limit(5)
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create active memtable with 3 events
    let mut active_builder = MemTableFactory::new();
    for i in 0..3 {
        let e = EventFactory::new()
            .with("event_type", "passive_test")
            .with("context_id", format!("active-{}", i))
            .with("payload", json!({"value": i}))
            .create();
        active_builder = active_builder.with_event(e);
    }
    let active_memtable = active_builder.create().unwrap();

    // Create passive memtable with 5 events
    let mut passive_builder = MemTableFactory::new();
    for i in 0..5 {
        let e = EventFactory::new()
            .with("event_type", "passive_test")
            .with("context_id", format!("passive-{}", i))
            .with("payload", json!({"value": i + 100}))
            .create();
        passive_builder = passive_builder.with_event(e);
    }
    let passive_memtable = Arc::new(tokio::sync::Mutex::new(passive_builder.create().unwrap()));

    let passives = vec![&passive_memtable];
    let runner =
        MemTableQueryRunner::new(Some(&active_memtable), &passives, &plan).with_limit(Some(5));

    let result = runner.run().await;

    // Should get 3 from active + 2 from passive (total 5, respecting limit)
    assert_eq!(result.len(), 5);
}

/// Tests ORDER BY across active and passive memtables
#[tokio::test]
async fn sorts_across_active_and_passive_memtables() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("sort_mem_test", &[("priority", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("sort_mem_test")
        .with_order_by("priority", false) // ASC
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Create active memtable with some priorities
    let mut active_builder = MemTableFactory::new();
    for priority in vec![5, 2, 8] {
        let e = EventFactory::new()
            .with("event_type", "sort_mem_test")
            .with("context_id", format!("active-{}", priority))
            .with("payload", json!({"priority": priority}))
            .create();
        active_builder = active_builder.with_event(e);
    }
    let active_memtable = active_builder.create().unwrap();

    // Create passive memtable with other priorities
    let mut passive_builder = MemTableFactory::new();
    for priority in vec![1, 3, 7] {
        let e = EventFactory::new()
            .with("event_type", "sort_mem_test")
            .with("context_id", format!("passive-{}", priority))
            .with("payload", json!({"priority": priority}))
            .create();
        passive_builder = passive_builder.with_event(e);
    }
    let passive_memtable = Arc::new(tokio::sync::Mutex::new(passive_builder.create().unwrap()));

    let passives = vec![&passive_memtable];
    let runner = MemTableQueryRunner::new(Some(&active_memtable), &passives, &plan);

    let result = runner.run().await;

    assert_eq!(result.len(), 6);

    // Verify sorted ascending: 1, 2, 3, 5, 7, 8
    let priorities: Vec<i64> = result
        .iter()
        .map(|e| e.payload.get("priority").and_then(|v| v.as_i64()).unwrap())
        .collect();

    assert_eq!(priorities, vec![1, 2, 3, 5, 7, 8]);
}
