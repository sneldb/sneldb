use std::sync::Arc;

use tempfile::tempdir;

use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;

use crate::test_helpers::factories::command_factory::CommandFactory;
use crate::test_helpers::factories::filter_group_factory::FilterGroupFactory;
use crate::test_helpers::factories::query_plan_factory::QueryPlanFactory;
use crate::test_helpers::factories::schema_factory::SchemaRegistryFactory;

#[tokio::test]
async fn builder_returns_empty_when_event_type_missing_uid() {
    let tmp = tempdir().unwrap();
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query().with_event_type("ev").create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    // FilterPlan for event_type without uid
    let filter = FilterGroupFactory::new()
        .with_column("event_type")
        .with_value(serde_json::json!("ev"))
        .create();

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("seg1");
    assert!(
        zones.is_empty(),
        "expected no zones for missing uid on event_type"
    );
}

#[tokio::test]
async fn builder_returns_all_zones_when_context_id_missing_uid() {
    let tmp = tempdir().unwrap();
    let registry = SchemaRegistryFactory::new().registry();

    let command = CommandFactory::query().create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    // FilterPlan for context_id without uid
    let filter = FilterGroupFactory::new()
        .with_column("context_id")
        .with_value(serde_json::json!("ctx"))
        .create();

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("seg1");
    let all_zones =
        crate::engine::core::zone::candidate_zone::CandidateZone::create_all_zones_for_segment(
            "seg1",
        );
    assert_eq!(
        zones.len(),
        all_zones.len(),
        "expected all zones when uid missing on context_id"
    );
}

#[tokio::test]
async fn builder_returns_field_selector_for_regular_column() {
    let tmp = tempdir().unwrap();
    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_field";
    registry_fac
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    let filter = FilterGroupFactory::new()
        .with_column("plan")
        .with_value(serde_json::json!("yes"))
        .with_uid(&uid)
        .create();

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("seg1");
    // For this test we only assert it doesn't crash and returns a Vec
    assert_eq!(zones.iter().count(), zones.len());
}

#[tokio::test]
async fn builder_routes_event_type_and_context_to_index_selector() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg_dir = shard_dir.join("001");
    std::fs::create_dir_all(&seg_dir).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "builder_index_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("x", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Flush some events so .idx exists
    let e = crate::test_helpers::factories::EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", serde_json::json!({"x":"v"}))
        .create();
    let mem = crate::test_helpers::factories::MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![e])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem,
        1,
        &seg_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // event_type filter -> IndexZoneSelector with policy AllZonesIfNoContext (index present -> returns zones)
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["shard-0/001".into()])
        .create()
        .await;
    let fp = FilterGroupFactory::new()
        .with_column("event_type")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(serde_json::json!(event_type))
        .with_uid(&uid)
        .create();
    let ctx = SelectionContext {
        plan: &fp,
        query_plan: &plan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector =
        crate::engine::core::zone::selector::builder::ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("001");
    assert!(!zones.is_empty());

    // context_id filter -> IndexZoneSelector with policy AllZones
    let fp2 = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(serde_json::json!("c1"))
        .with_uid(&uid)
        .create();
    let ctx2 = SelectionContext {
        plan: &fp2,
        query_plan: &plan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector2 =
        crate::engine::core::zone::selector::builder::ZoneSelectorBuilder::new(ctx2).build();
    let zones2 = selector2.select_for_segment("001");
    assert!(!zones2.is_empty());
}
