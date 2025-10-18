use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
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
    let f_gte = FilterPlanFactory::new()
        .with_column("order_id")
        .with_operation(CompareOp::Gte)
        .with_uid(&uid)
        .with_value(json!(100))
        .create();
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
    let f_lte = FilterPlanFactory::new()
        .with_column("order_id")
        .with_operation(CompareOp::Lte)
        .with_uid(&uid)
        .with_value(json!(100))
        .create();
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
    let f_gt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
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
    let f_lt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
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
async fn falls_back_to_range_handler_on_non_id_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "range_fallback";
    reg_fac
        .define_with_field_types(
            event_type,
            &[
                ("context_id", FieldType::String),
                ("amount", FieldType::I64),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"amount": 5}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![a])
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

    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // Even though field is non-id-like, fallback handler returns all zones ⇒ not empty
    let f_gt = FilterPlanFactory::new()
        .with_column("amount")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(1))
        .create();
    let ctx = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let z = sel.select_for_segment("001");
    assert!(!z.is_empty());
}
