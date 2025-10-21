use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::QueryCaches;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::schema::FieldType;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterPlanFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

// Covers end-to-end RangePruner behavior (SuRF first, fallback handler) with id-like and non-id fields

#[tokio::test]
async fn surf_ge_and_le_inclusive_boundaries_id_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    let seg2 = shard_dir.join("002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "range_edges";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("order_id", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: order_id 100, 105
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"order_id": 100}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"order_id": 105}))
        .create();
    let mem1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a, b])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem1,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // seg2: order_id 95, 100
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c")
        .with("payload", json!({"order_id": 95}))
        .create();
    let d = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "d")
        .with("payload", json!({"order_id": 100}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![c, d])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // Gte 100 should include both segments
    let mut f_gte = FilterPlanFactory::new()
        .with_column("order_id")
        .with_operation(CompareOp::Gte)
        .with_uid(&uid)
        .with_value(json!(100))
        .create();
    f_gte.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "order_id".to_string(),
    });
    let ctx1 = SelectionContext {
        plan: &f_gte,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel1 = ZoneSelectorBuilder::new(ctx1).build();
    let g1 = sel1.select_for_segment("001");
    let g2 = sel1.select_for_segment("002");
    assert!(!g1.is_empty());
    assert!(!g2.is_empty());

    // Lte 100 should include both segments
    let mut f_lte = FilterPlanFactory::new()
        .with_column("order_id")
        .with_operation(CompareOp::Lte)
        .with_uid(&uid)
        .with_value(json!(100))
        .create();
    f_lte.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "order_id".to_string(),
    });
    let ctx2 = SelectionContext {
        plan: &f_lte,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel2 = ZoneSelectorBuilder::new(ctx2).build();
    let l1 = sel2.select_for_segment("001");
    let l2 = sel2.select_for_segment("002");
    assert!(!l1.is_empty());
    assert!(!l2.is_empty());
}

#[tokio::test]
async fn surf_gt_and_lt_exclusive_boundaries_id_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    let seg2 = shard_dir.join("002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "range_exclusive";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("user_id", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: user_id 11
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"user_id": 11}))
        .create();
    let mem1 = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![a])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem1,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // seg2: user_id 9
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"user_id": 9}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![b])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // Gt 10 -> seg1 only
    let mut f_gt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    f_gt.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "user_id".to_string(),
    });
    let ctx1 = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel1 = ZoneSelectorBuilder::new(ctx1).build();
    let z1 = sel1.select_for_segment("001");
    let z2 = sel1.select_for_segment("002");
    assert!(!z1.is_empty());
    assert!(z2.is_empty());

    // Lt 10 -> seg2 only
    let mut f_lt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    f_lt.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "user_id".to_string(),
    });
    let ctx2 = SelectionContext {
        plan: &f_lt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel2 = ZoneSelectorBuilder::new(ctx2).build();
    let y1 = sel2.select_for_segment("001");
    let y2 = sel2.select_for_segment("002");
    assert!(y1.is_empty());
    assert!(!y2.is_empty());
}

#[tokio::test]
async fn skips_surf_for_payload_temporal_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "range_temporal";
    reg_fac
        .define_with_field_types(
            event_type,
            &[
                ("context_id", FieldType::String),
                ("ts", FieldType::Timestamp),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: ts=100, 200
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"ts": 100u64}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"ts": 200u64}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a, b])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Provide caches so RangePruner can detect field calendar and skip SuRF
    let caches = QueryCaches::new(shard_dir.clone());
    // Sanity: calendar should exist for the temporal field
    let cal_path = seg1.join(format!("{}_{}.cal", uid, "ts"));
    assert!(
        cal_path.exists(),
        "expected calendar at {}",
        cal_path.display()
    );

    // RangePruner should skip temporal payload field 'ts' and return None
    let artifacts = ZoneArtifacts::new(&shard_dir, Some(&caches));
    let pruner = RangePruner { artifacts };
    let args = super::PruneArgs {
        segment_id: "001",
        uid: &uid,
        column: "ts",
        value: Some(&json!(150u64)),
        op: Some(&CompareOp::Gt),
    };
    let attempt = pruner.apply_surf_only(&args);
    assert!(
        attempt.is_none(),
        "RangePruner must skip for temporal fields"
    );
}
