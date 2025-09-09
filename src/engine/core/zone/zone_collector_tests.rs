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

    Flusher::new(memtable1, segment1_id, &segment1_dir, registry.clone())
        .flush()
        .await
        .unwrap();

    Flusher::new(memtable2, segment2_id, &segment2_dir, registry.clone())
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
