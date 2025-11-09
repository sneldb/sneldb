use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{Flusher, ZoneHydrator};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, ExecutionStepFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tracing::info;

#[tokio::test]
async fn hydrates_candidate_zones_with_values() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: schema + registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "purchase";

    schema_factory
        .define_with_fields(event_type, &[("amount", "integer"), ("region", "string")])
        .await
        .unwrap();

    // Create a temp segment with flushed events
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"amount": 100, "region": "EU"}))
        .create_list(5);

    info!("Events: {:?}", events);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    // Create query command and plan
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "amount".into(),
            op: CompareOp::Eq,
            value: json!(100),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Create execution steps from all filter groups in the plan
    let steps: Vec<_> = plan.filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
        .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Run hydrator
    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Assert zones are returned and enriched
    assert!(!zones.is_empty(), "Expected candidate zones");
    assert!(
        zones[0].values.contains_key("amount"),
        "Expected zone to have amount values loaded"
    );
}
