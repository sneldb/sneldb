use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{Flusher, ZoneCollector};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, ExecutionStepFactory, FilterPlanFactory, MemTableFactory,
    QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tracing::info;

#[tokio::test]
// Combines zones across segments for simple Eq filter
async fn zone_collector_combines_zones_across_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment2_id = 2;
    let segment1_dir = shard_dir.join("segment-001");
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment1_dir).unwrap();
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_created";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    let events1 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx1")
            .with("payload", json!({ "id": 1 }))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx2")
            .with("payload", json!({ "id": 2 }))
            .create(),
    ];

    let events2 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx3")
            .with("payload", json!({ "id": 3 }))
            .create(),
    ];

    let memtable1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events1)
        .create()
        .unwrap();

    let memtable2 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events2)
        .create()
        .unwrap();

    Flusher::new(memtable1, segment1_id, &segment1_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();

    Flusher::new(memtable2, segment2_id, &segment2_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();

    // Create Command
    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: json!(1),
        })
        .create();

    // Create QueryPlan
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(shard_dir)
        .with_segment_ids(vec!["segment-001".to_string(), "segment-002".to_string()])
        .create()
        .await;

    let filter_id = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let filter_context_id = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(json!("ctx2"))
        .with_uid(&uid)
        .create();

    let step_id = ExecutionStepFactory::new()
        .with_filter(filter_id)
        .with_plan(&plan)
        .create();

    let step_context_id = ExecutionStepFactory::new()
        .with_filter(filter_context_id)
        .with_plan(&plan)
        .create();

    info!("Step context id: {:?}", step_context_id);

    let mut collector = ZoneCollector::new(&plan, vec![step_id, step_context_id]);
    let zones = collector.collect_zones();

    assert_eq!(zones.len(), 1);
    assert_eq!(zones[0].segment_id, "segment-001");
    assert_eq!(zones[0].zone_id, 0);
}

#[tokio::test]
// Reorders context step first under AND; result matches expected segment
async fn zone_collector_reorders_context_first_and_yields_same_result() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment2_id = 2;
    let segment1_dir = shard_dir.join("segment-001");
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment1_dir).unwrap();
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_created";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // seg1 has ctx2 id=2, seg2 has ctx3 id=3
    let events1 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx2")
            .with("payload", json!({ "id": 2 }))
            .create(),
    ];
    let events2 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx3")
            .with("payload", json!({ "id": 3 }))
            .create(),
    ];

    let memtable1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events1)
        .create()
        .unwrap();
    let memtable2 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events2)
        .create()
        .unwrap();

    Flusher::new(memtable1, segment1_id, &segment1_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();
    Flusher::new(memtable2, segment2_id, &segment2_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();

    // Command: AND semantics (single comparison treated as AND), but we'll supply both steps
    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .with_segment_base_dir(shard_dir)
        .with_segment_ids(vec!["segment-001".to_string(), "segment-002".to_string()])
        .create()
        .await;

    // Steps provided in reverse (id then context); planner should run context first
    let filter_id = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(2))
        .with_uid(&uid)
        .create();
    let filter_context = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(json!("ctx2"))
        .with_uid(&uid)
        .create();
    let step_id = ExecutionStepFactory::new()
        .with_filter(filter_id)
        .with_plan(&plan)
        .create();
    let step_ctx = ExecutionStepFactory::new()
        .with_filter(filter_context)
        .with_plan(&plan)
        .create();

    let mut collector = ZoneCollector::new(&plan, vec![step_id, step_ctx]);
    let zones = collector.collect_zones();

    assert_eq!(zones.len(), 1);
    assert_eq!(zones[0].segment_id, "segment-001");
}

#[tokio::test]
// Under OR, collector unions zones from multiple steps without pruning
async fn zone_collector_or_combines_union_without_pruning() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment1_dir = shard_dir.join("segment-001");
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment1_dir).unwrap();
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt";
    schema_factory
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    // seg1 has id=1, seg2 has id=3
    let events1 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("payload", json!({ "id": 1 }))
            .create(),
    ];
    let events2 = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("payload", json!({ "id": 3 }))
            .create(),
    ];

    let m1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events1)
        .create()
        .unwrap();
    let m2 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events2)
        .create()
        .unwrap();
    Flusher::new(m1, 1, &segment1_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();
    Flusher::new(m2, 2, &segment2_dir, registry.clone(), Arc::new(tokio::sync::Mutex::new(())))
        .flush()
        .await
        .unwrap();

    // where: id=1 OR id=3
    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Or(
            Box::new(Expr::Compare {
                field: "id".into(),
                op: CompareOp::Eq,
                value: json!(1),
            }),
            Box::new(Expr::Compare {
                field: "id".into(),
                op: CompareOp::Eq,
                value: json!(3),
            }),
        ))
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry.clone())
        .with_segment_base_dir(shard_dir)
        .with_segment_ids(vec!["segment-001".into(), "segment-002".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");
    let fp1 = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();
    let fp3 = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(3))
        .with_uid(&uid)
        .create();
    let s1 = ExecutionStepFactory::new()
        .with_filter(fp1)
        .with_plan(&plan)
        .create();
    let s3 = ExecutionStepFactory::new()
        .with_filter(fp3)
        .with_plan(&plan)
        .create();

    let mut collector = ZoneCollector::new(&plan, vec![s1, s3]);
    let zones = collector.collect_zones();

    // Union should include both segments' zones
    let segs: std::collections::HashSet<_> = zones.iter().map(|z| z.segment_id.as_str()).collect();
    assert_eq!(
        segs,
        std::collections::HashSet::from(["segment-001", "segment-002"])
    );
}
