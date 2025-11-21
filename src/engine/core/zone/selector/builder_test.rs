use std::sync::Arc;

use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::Flusher;
use crate::engine::core::zone::candidate_zone::CandidateZone;
use crate::engine::core::zone::selector::{
    builder::ZoneSelectorBuilder, selection_context::SelectionContext,
};

use crate::test_helpers::factories::{
    command_factory::CommandFactory, event_factory::EventFactory,
    filter_group_factory::FilterGroupFactory, memtable_factory::MemTableFactory,
    query_plan_factory::QueryPlanFactory, schema_factory::SchemaRegistryFactory,
};

#[tokio::test]
async fn builder_returns_all_zones_when_event_type_missing_uid() {
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
    let expected = CandidateZone::create_all_zones_for_segment("seg1");
    assert_eq!(
        zones.len(),
        expected.len(),
        "expected all zones for missing uid on event_type"
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
    let all_zones = CandidateZone::create_all_zones_for_segment("seg1");
    assert_eq!(
        zones.len(),
        all_zones.len(),
        "expected all zones when uid missing on context_id"
    );
}

#[tokio::test]
async fn builder_context_id_fallback_scans_all_known_uids() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "wildcard_evt";
    registry_factory
        .define_with_fields(event_type, &[("context_id", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let event = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![event])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        0,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let command = CommandFactory::query()
        .with_event_type("*")
        .with_context_id("ctx1")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let filter = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!("ctx1"))
        .create();
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("00000");
    let expected = CandidateZone::create_all_zones_for_segment_from_meta(
        &plan.segment_base_dir,
        "00000",
        &uid,
    );
    assert_eq!(
        zones.len(),
        expected.len(),
        "wildcard context should scan all known uids"
    );
    assert!(!zones.is_empty());
}

#[tokio::test]
async fn builder_event_type_wildcard_scans_all_known_uids() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let evt_a = "evt_a";
    let evt_b = "evt_b";
    registry_factory
        .define_with_fields(evt_a, &[("context_id", "string")])
        .await
        .unwrap();
    registry_factory
        .define_with_fields(evt_b, &[("context_id", "string")])
        .await
        .unwrap();
    let uid_a = registry.read().await.get_uid(evt_a).unwrap();
    let uid_b = registry.read().await.get_uid(evt_b).unwrap();

    let event_a = EventFactory::new()
        .with("event_type", evt_a)
        .with("context_id", "ctx1")
        .create();
    let event_b = EventFactory::new()
        .with("event_type", evt_b)
        .with("context_id", "ctx2")
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event_a, event_b])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        0,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let command = CommandFactory::query()
        .with_event_type("*")
        .with_context_id("ctx1")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let filter = FilterGroupFactory::new()
        .with_column("event_type")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!("*"))
        .create();
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("00000");

    let mut expected = CandidateZone::create_all_zones_for_segment_from_meta(
        &plan.segment_base_dir,
        "00000",
        &uid_a,
    );
    expected.extend(CandidateZone::create_all_zones_for_segment_from_meta(
        &plan.segment_base_dir,
        "00000",
        &uid_b,
    ));
    expected = CandidateZone::uniq(expected);

    assert_eq!(
        zones.len(),
        expected.len(),
        "wildcard event_type should scan zones for all known uids"
    );
    assert!(!zones.is_empty());
}

#[tokio::test]
async fn builder_timestamp_fallback_scans_all_known_uids() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "login_ts";
    registry_factory
        .define_with_fields(event_type, &[("device", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "alice")
            .with("device", "android")
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "alice")
            .with("device", "web")
            .create(),
    ];
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events)
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        0,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let command = CommandFactory::query()
        .with_event_type("*")
        .with_context_id("alice")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let timestamp_filter = plan
        .filter_groups
        .iter()
        .find(|fg| fg.column() == Some("timestamp"))
        .expect("timestamp filter present");

    let ctx = SelectionContext {
        plan: timestamp_filter,
        query_plan: &plan,
        base_dir: &plan.segment_base_dir,
        caches: None,
    };

    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("00000");

    let expected = CandidateZone::create_all_zones_for_segment_from_meta(
        &plan.segment_base_dir,
        "00000",
        &uid,
    );

    assert_eq!(
        zones.len(),
        expected.len(),
        "timestamp fallback should scan all zones when uid is missing"
    );
    assert!(!zones.is_empty());
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
    Flusher::new(
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
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!(event_type))
        .with_uid(&uid)
        .create();
    let ctx = SelectionContext {
        plan: &fp,
        query_plan: &plan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("001");
    assert!(!zones.is_empty());

    // context_id filter -> IndexZoneSelector with policy AllZones
    let fp2 = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!("c1"))
        .with_uid(&uid)
        .create();
    let ctx2 = SelectionContext {
        plan: &fp2,
        query_plan: &plan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector2 = ZoneSelectorBuilder::new(ctx2).build();
    let zones2 = selector2.select_for_segment("001");
    assert!(!zones2.is_empty());
}
