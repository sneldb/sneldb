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

    let flusher = Flusher::new(
        memtable,
        7,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
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

#[tokio::test]
async fn executes_query_respects_limit_memtable_only() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_respects_limit_memtable_only";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(2)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // 5 matching events in memtable
    let mut builder = MemTableFactory::new();
    for i in 0..5 {
        let e = EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", format!("ctx-{}", i))
            .with("payload", json!({"status": "ok"}))
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn executes_query_limit_short_circuits_before_segments_when_memtable_satisfies() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Prepare segment directory
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-123");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Schema
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_short_circuits_before_segments_when_memtable_satisfies";
    schema_factory
        .define_with_fields(event_type, &[("status", "string"), ("source", "string")])
        .await
        .unwrap();

    // Memtable has 3 matching events (limit=2)
    let m1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-1")
        .with("payload", json!({"status": "ok", "source": "memtable"}))
        .create();
    let m2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-2")
        .with("payload", json!({"status": "ok", "source": "memtable"}))
        .create();
    let m3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-3")
        .with("payload", json!({"status": "ok", "source": "memtable"}))
        .create();
    let memtable_to_flush = MemTableFactory::new()
        .with_events(vec![m1.clone(), m2.clone(), m3.clone()])
        .create()
        .unwrap();

    // Segment will contain additional matching events with a different source marker
    let s1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "s-1")
        .with("payload", json!({"status": "ok", "source": "segment"}))
        .create();
    let s2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "s-2")
        .with("payload", json!({"status": "ok", "source": "segment"}))
        .create();
    let seg_memtable = MemTableFactory::new()
        .with_events(vec![s1.clone(), s2.clone()])
        .create()
        .unwrap();

    // Flush segment
    let flusher = Flusher::new(
        seg_memtable,
        2,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Build query (limit=2)
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(2)
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
        .with_segment_ids(vec!["shard-0/segment-123".into()])
        .create()
        .await;

    // Active memtable has enough to satisfy the limit
    let active_memtable = MemTableFactory::new()
        .with_events(vec![m1, m2, m3])
        .create()
        .unwrap();

    let mut execution = QueryExecution::new(&plan).with_memtable(&active_memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 2);
    // Ensure we returned only memtable-sourced rows (segment not consulted)
    for e in results {
        assert_eq!(e.payload.get("source").unwrap(), "memtable");
    }
}

#[tokio::test]
async fn executes_query_limit_combines_memtable_and_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-456");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_combines_memtable_and_segments";
    schema_factory
        .define_with_fields(event_type, &[("status", "string"), ("source", "string")])
        .await
        .unwrap();

    // Segment with 5 matching
    let mut seg_builder = MemTableFactory::new();
    for i in 0..5 {
        let e = EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", format!("s-{}", i))
            .with("payload", json!({"status": "ok", "source": "segment"}))
            .create();
        seg_builder = seg_builder.with_event(e);
    }
    let seg_memtable = seg_builder.create().unwrap();
    let flusher = Flusher::new(
        seg_memtable,
        5,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Memtable with 1 matching
    let m = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-1")
        .with("payload", json!({"status": "ok", "source": "memtable"}))
        .create();
    let memtable = MemTableFactory::new().with_event(m).create().unwrap();

    // Query limit = 3
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(3)
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
        .with_segment_ids(vec!["shard-0/segment-456".into()])
        .create()
        .await;

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn executes_query_limit_zero_returns_empty_no_scan() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_zero_returns_empty_no_scan";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Build query with LIMIT 0
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(0)
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    let tmp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp_dir.path())
        .create()
        .await;

    // Memtable with matches should be ignored due to limit 0
    let e = EventFactory::new()
        .with("event_type", event_type)
        .with("payload", json!({"status":"ok"}))
        .create();
    let memtable = MemTableFactory::new().with_event(e).create().unwrap();

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn executes_query_limit_one_memtable_and_segments_mixed_order() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-111");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_one_memtable_and_segments_mixed_order";
    schema_factory
        .define_with_fields(event_type, &[("status", "string"), ("source", "string")])
        .await
        .unwrap();

    // Segment with 2 matches
    let s1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "s-1")
        .with("payload", json!({"status": "ok", "source": "segment"}))
        .create();
    let s2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "s-2")
        .with("payload", json!({"status": "ok", "source": "segment"}))
        .create();
    let seg_memtable = MemTableFactory::new()
        .with_events(vec![s1, s2])
        .create()
        .unwrap();
    let flusher = Flusher::new(
        seg_memtable,
        2,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Memtable with 1 match
    let m = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-1")
        .with("payload", json!({"status": "ok", "source": "memtable"}))
        .create();
    let memtable = MemTableFactory::new().with_event(m).create().unwrap();

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(1)
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
        .with_segment_ids(vec!["shard-0/segment-111".into()])
        .create()
        .await;

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn executes_query_limit_exact_boundary_between_memtable_and_segment() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-222");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_exact_boundary_between_memtable_and_segment";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Segment with 2 matches
    let seg_memtable = MemTableFactory::new()
        .with_events(vec![
            EventFactory::new()
                .with("event_type", event_type)
                .with("payload", json!({"status":"ok"}))
                .create(),
            EventFactory::new()
                .with("event_type", event_type)
                .with("payload", json!({"status":"ok"}))
                .create(),
        ])
        .create()
        .unwrap();
    let flusher = Flusher::new(
        seg_memtable,
        2,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Memtable with 3 matches
    let mut builder = MemTableFactory::new();
    for _ in 0..3 {
        let e = EventFactory::new()
            .with("event_type", event_type)
            .with("payload", json!({"status":"ok"}))
            .create();
        builder = builder.with_event(e);
    }
    let memtable = builder.create().unwrap();

    // Limit equals exactly memtable matches
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(3)
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
        .with_segment_ids(vec!["shard-0/segment-222".into()])
        .create()
        .await;

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    // Should not touch segments because memtable already satisfies exactly
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn executes_query_limit_larger_than_total_results() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-789");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "executes_query_limit_larger_than_total_results";
    schema_factory
        .define_with_fields(event_type, &[("status", "string")])
        .await
        .unwrap();

    // Segment with 1 match
    let seg_event = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "s-1")
        .with("payload", json!({"status": "ok"}))
        .create();
    let seg_memtable = MemTableFactory::new()
        .with_event(seg_event)
        .create()
        .unwrap();
    let flusher = Flusher::new(
        seg_memtable,
        1,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Memtable with 2 matches
    let m1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-1")
        .with("payload", json!({"status": "ok"}))
        .create();
    let m2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "m-2")
        .with("payload", json!({"status": "ok"}))
        .create();
    let memtable = MemTableFactory::new()
        .with_events(vec![m1, m2])
        .create()
        .unwrap();

    // Limit larger than total (10)
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(10)
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
        .with_segment_ids(vec!["shard-0/segment-789".into()])
        .create()
        .await;

    let mut execution = QueryExecution::new(&plan).with_memtable(&memtable);
    let results = execution.run().await.unwrap();
    assert_eq!(results.len(), 3);
}
