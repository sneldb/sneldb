use crate::command::types::{AggSpec, CompareOp, Expr, TimeGranularity};
use crate::engine::core::read::result::{QueryResult, ResultTable};
use crate::engine::core::read::sink::AggregateSink;
use crate::engine::core::{ExecutionStep, Flusher, QueryPlan, SegmentAggregateRunner};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

async fn make_plan(
    event_type: &str,
    registry: &Arc<tokio::sync::RwLock<crate::engine::schema::registry::SchemaRegistry>>,
    segment_base_dir: &std::path::Path,
    segment_ids: Vec<String>,
    aggs: Vec<AggSpec>,
    group_by: Option<Vec<&str>>,
    time_bucket: Option<TimeGranularity>,
    where_clause: Option<Expr>,
) -> QueryPlan {
    let mut cmdf = CommandFactory::query().with_event_type(event_type);
    if let Some(expr) = where_clause {
        cmdf = cmdf.with_where_clause(expr);
    }
    if !aggs.is_empty() {
        cmdf = cmdf.with_aggs(aggs);
    }
    if let Some(gb) = group_by {
        cmdf = cmdf.with_group_by(gb);
    }
    if let Some(tb) = time_bucket {
        cmdf = cmdf.with_time_bucket(tb);
    }
    let cmd = cmdf.create();

    QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(registry))
        .with_segment_base_dir(segment_base_dir)
        .with_segment_ids(segment_ids)
        .create()
        .await
}

async fn make_plan_with_since(
    event_type: &str,
    registry: &Arc<tokio::sync::RwLock<crate::engine::schema::registry::SchemaRegistry>>,
    segment_base_dir: &std::path::Path,
    segment_ids: Vec<String>,
    aggs: Vec<AggSpec>,
    group_by: Option<Vec<&str>>,
    time_bucket: Option<TimeGranularity>,
    where_clause: Option<Expr>,
    since: Option<&str>,
) -> QueryPlan {
    let mut cmdf = CommandFactory::query().with_event_type(event_type);
    if let Some(expr) = where_clause {
        cmdf = cmdf.with_where_clause(expr);
    }
    if !aggs.is_empty() {
        cmdf = cmdf.with_aggs(aggs);
    }
    if let Some(gb) = group_by {
        cmdf = cmdf.with_group_by(gb);
    }
    if let Some(tb) = time_bucket {
        cmdf = cmdf.with_time_bucket(tb);
    }
    if let Some(s) = since {
        cmdf = cmdf.with_since(s);
    }
    let cmd = cmdf.create();

    QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(registry))
        .with_segment_base_dir(segment_base_dir)
        .with_segment_ids(segment_ids)
        .create()
        .await
}

fn table_to_map(table: ResultTable) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let cols: Vec<String> = table.columns.into_iter().map(|c| c.name).collect();
    (cols, table.rows)
}

#[tokio::test]
async fn segment_aggregate_runner_count_unique_mixed_missing_and_empty() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00050");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt5", &[("user", "string")])
        .await
        .unwrap();

    let e1 = EventFactory::new()
        .with("event_type", "evt5")
        .with("payload", json!({"user":"u1"}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", "evt5")
        .with("payload", json!({"user":"u2"}))
        .create();
    let e3 = EventFactory::new()
        .with("event_type", "evt5")
        .with("payload", json!({"user":"u1"}))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", "evt5")
        .with("payload", json!({})) // missing -> ""
        .create();
    let e5 = EventFactory::new()
        .with("event_type", "evt5")
        .with("payload", json!({"user":""})) // empty -> ""
        .create();

    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![e1, e2, e3, e4, e5])
        .create()
        .unwrap();

    Flusher::new(
        mem,
        50,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let plan = make_plan(
        "evt5",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00050".into()],
        vec![AggSpec::Count {
            unique_field: Some("user".into()),
        }],
        None,
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let events = sink.into_events(&plan);
    eprintln!("events: {:?}", events);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    eprintln!("p: {:?}", p);
    // With columnar storage, all fields written for all events (for proper column alignment)
    // Missing and explicit empty both become "", so unique set is {"u1","u2",""}
    assert_eq!(p["count_unique_user"], json!(3));
}

#[tokio::test]
async fn segment_aggregate_runner_min_max_numeric_preference() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00060");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt6", &[("name", "string")])
        .await
        .unwrap();

    let e1 = EventFactory::new()
        .with("event_type", "evt6")
        .with("payload", json!({"name":"10"}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", "evt6")
        .with("payload", json!({"name":"2"}))
        .create();
    let e3 = EventFactory::new()
        .with("event_type", "evt6")
        .with("payload", json!({"name":"bob"}))
        .create();

    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![e1, e2, e3])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        60,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let plan = make_plan(
        "evt6",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00060".into()],
        vec![
            AggSpec::Min {
                field: "name".into(),
            },
            AggSpec::Max {
                field: "name".into(),
            },
        ],
        None,
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    assert_eq!(p["min_name"], json!("2"));
    assert_eq!(p["max_name"], json!("10"));
}

#[tokio::test]
async fn segment_aggregate_runner_count_field_missing_is_zero() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00070");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt7", &[("amount", "int")])
        .await
        .unwrap();

    let e1 = EventFactory::new()
        .with("event_type", "evt7")
        .with("payload", json!({"amount":1}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", "evt7")
        .with("payload", json!({"amount":2}))
        .create();

    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![e1, e2])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        70,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let plan = make_plan(
        "evt7",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00070".into()],
        vec![AggSpec::CountField {
            field: "visits".into(),
        }],
        None,
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    assert_eq!(p["count_visits"], json!(0));
}

#[tokio::test]
async fn segment_aggregate_runner_since_ignored_in_aggregation() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00080");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt8", &[("id", "int")])
        .await
        .unwrap();

    let e1 = EventFactory::new()
        .with("event_type", "evt8")
        .with("timestamp", json!(100u64))
        .with("payload", json!({"id":1}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", "evt8")
        .with("timestamp", json!(200u64))
        .with("payload", json!({"id":2}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![e1, e2])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        80,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Set since far in the future; aggregator builder should skip special-field injection
    let plan = make_plan_with_since(
        "evt8",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00080".into()],
        vec![AggSpec::Count { unique_field: None }],
        None,
        None,
        None,
        Some("999999"),
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    // If since were enforced, this would be 0; expected to count both
    assert_eq!(p["count"], json!(2));
}

#[tokio::test]
async fn segment_aggregate_runner_count_across_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Directories and schema
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg1_dir = shard_dir.join("001");
    let seg2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&seg1_dir).unwrap();
    std::fs::create_dir_all(&seg2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt", &[("amount", "int"), ("country", "string")])
        .await
        .unwrap();

    // Flush two segments worth of events
    let mem1 = MemTableFactory::new()
        .with_capacity(10)
        .with_events(
            EventFactory::new()
                .with("event_type", "evt")
                .with("payload", json!({"amount": 1, "country":"US"}))
                .create_list(3),
        )
        .create()
        .unwrap();
    Flusher::new(
        mem1,
        1,
        &seg1_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let mem2 = MemTableFactory::new()
        .with_capacity(10)
        .with_events(
            EventFactory::new()
                .with("event_type", "evt")
                .with("payload", json!({"amount": 2, "country":"DE"}))
                .create_list(2),
        )
        .create()
        .unwrap();
    Flusher::new(
        mem2,
        2,
        &seg2_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Plan: COUNT
    let plan = make_plan(
        "evt",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/001".into(), "shard-0/002".into()],
        vec![AggSpec::Count { unique_field: None }],
        None,
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    // 3 + 2 events across segments
    let result = sink.into_partial();
    let table = QueryResult::Aggregation(crate::engine::core::read::result::AggregateResult {
        group_by: result.group_by,
        time_bucket: result.time_bucket,
        specs: result.specs.clone(),
        groups: result.groups.clone(),
    })
    .finalize_table();
    let (cols, rows) = table_to_map(table);
    assert_eq!(cols, vec!["count".to_string()]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!(5));
}

#[tokio::test]
async fn segment_aggregate_runner_respects_where_predicate() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00010");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt2", &[("amount", "int"), ("country", "string")])
        .await
        .unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", "evt2")
            .with("payload", json!({"amount": 10, "country":"US"}))
            .create(),
        EventFactory::new()
            .with("event_type", "evt2")
            .with("payload", json!({"amount": 3, "country":"DE"}))
            .create(),
        EventFactory::new()
            .with("event_type", "evt2")
            .with("payload", json!({"amount": 7, "country":"US"}))
            .create(),
    ];
    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();
    Flusher::new(
        mem,
        10,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // WHERE amount >= 7, COUNT
    let where_clause = Expr::Compare {
        field: "amount".into(),
        op: CompareOp::Gte,
        value: json!(7),
    };
    let plan = make_plan(
        "evt2",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00010".into()],
        vec![AggSpec::Count { unique_field: None }],
        None,
        None,
        Some(where_clause),
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let table = QueryResult::Aggregation(sink.into_partial().into()).finalize_table();
    let (_, rows) = table_to_map(table);
    // amount >= 7 matches 2 of 3
    assert_eq!(rows[0][0], serde_json::json!(2));
}

#[tokio::test]
async fn segment_aggregate_runner_group_by_and_bucket_total() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00020");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("order", &[("amount", "int"), ("country", "string")])
        .await
        .unwrap();

    // Same month bucket for all
    let ts1 = 3_000_000u64;
    let ts2 = 3_000_100u64;
    let ts3 = 3_000_200u64;
    let e1 = EventFactory::new()
        .with("event_type", "order")
        .with("timestamp", json!(ts1))
        .with("payload", json!({"amount": 10, "country":"US"}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", "order")
        .with("timestamp", json!(ts2))
        .with("payload", json!({"amount": 5, "country":"US"}))
        .create();
    let e3 = EventFactory::new()
        .with("event_type", "order")
        .with("timestamp", json!(ts3))
        .with("payload", json!({"amount": 3, "country":"DE"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(10)
        .with_events(vec![e1, e2, e3])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        20,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let plan = make_plan(
        "order",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00020".into()],
        vec![
            AggSpec::Total {
                field: "amount".into(),
            },
            AggSpec::Count { unique_field: None },
        ],
        Some(vec!["country"]),
        Some(TimeGranularity::Month),
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    // Convert to table and check per country totals
    let table = QueryResult::Aggregation(sink.into_partial().into()).finalize_table();
    let (cols, rows) = table_to_map(table);
    assert!(cols.contains(&"bucket".to_string()));
    assert!(cols.contains(&"country".to_string()));
    assert!(cols.contains(&"total_amount".to_string()));
    assert!(cols.contains(&"count".to_string()));

    // Build map by country
    let mut by_country: HashMap<String, (i64, i64)> = HashMap::new();
    let idx_bucket = cols.iter().position(|c| c == "bucket");
    let idx_country = cols.iter().position(|c| c == "country").unwrap();
    let idx_total = cols.iter().position(|c| c == "total_amount").unwrap();
    let idx_count = cols.iter().position(|c| c == "count").unwrap();
    for r in rows {
        let _bucket = idx_bucket.and_then(|i| r[i].as_i64());
        let ctry = r[idx_country].as_str().unwrap().to_string();
        let total = r[idx_total].as_i64().unwrap();
        let count = r[idx_count].as_i64().unwrap();
        by_country.insert(ctry, (total, count));
    }
    assert_eq!(by_country.get("US"), Some(&(15, 2)));
    assert_eq!(by_country.get("DE"), Some(&(3, 1)));
}

#[tokio::test]
async fn segment_aggregate_runner_missing_groupby_field_emits_empty_string() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00030");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt3", &[("plan", "string")])
        .await
        .unwrap();

    let e = EventFactory::new()
        .with("event_type", "evt3")
        .with("payload", json!({"plan":"pro"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(4)
        .with_events(vec![e])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        30,
        &seg_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // GROUP BY country (missing), COUNT
    let plan = make_plan(
        "evt3",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00030".into()],
        vec![AggSpec::Count { unique_field: None }],
        Some(vec!["country", "plan"]),
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    let table = QueryResult::Aggregation(sink.into_partial().into()).finalize_table();
    let (cols, rows) = table_to_map(table);
    let idx_country = cols.iter().position(|c| c == "country").unwrap();
    let idx_plan = cols.iter().position(|c| c == "plan").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][idx_country], serde_json::json!(""));
    assert_eq!(rows[0][idx_plan], serde_json::json!("pro"));
}

#[tokio::test]
async fn segment_aggregate_runner_empty_results_default_zero() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let seg_dir = shard_dir.join("00040");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("evt4", &[("amount", "int")])
        .await
        .unwrap();

    // No events -> no rows emitted

    let plan = make_plan(
        "evt4",
        &registry,
        tmp_dir.path(),
        vec!["shard-0/00040".into()],
        vec![AggSpec::Avg {
            field: "amount".into(),
        }],
        None,
        None,
        None,
    )
    .await;

    let steps: Vec<ExecutionStep> = plan
        .filter_plans
        .iter()
        .map(|f| ExecutionStep::new(f.clone(), &plan))
        .collect();

    let mut sink = AggregateSink::from_plan(plan.aggregate_plan.as_ref().unwrap());
    SegmentAggregateRunner::new(&plan, steps)
        .with_caches(None)
        .run(&mut sink)
        .await;

    // Use into_events to trigger default zero-output behavior when no rows matched
    let events = sink.into_events(&plan);
    assert_eq!(events.len(), 1);
    let p = events[0].payload.as_object().unwrap();
    assert_eq!(p["avg_amount"].as_f64().unwrap(), 0.0);
}

// Helper to convert AggPartial to AggregateResult inline for finalization
impl From<crate::engine::core::read::aggregate::partial::AggPartial>
    for crate::engine::core::read::result::AggregateResult
{
    fn from(p: crate::engine::core::read::aggregate::partial::AggPartial) -> Self {
        crate::engine::core::read::result::AggregateResult {
            group_by: p.group_by,
            time_bucket: p.time_bucket,
            specs: p.specs,
            groups: p.groups,
        }
    }
}
