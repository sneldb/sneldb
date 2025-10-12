use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterPlanFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

#[tokio::test]
async fn xor_eq_uses_zxf_to_narrow_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    let seg2 = shard_dir.join("segment-002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "purchase";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1 zone 0 has key = "a", seg2 zone 0 has key = "b"
    {
        let e1 = EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c1")
            .with("payload", json!({"key": "a"}))
            .create();
        let mem = MemTableFactory::new()
            .with_capacity(1)
            .with_events(vec![e1])
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
    }
    {
        let e2 = EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c2")
            .with("payload", json!({"key": "b"}))
            .create();
        let mem = MemTableFactory::new()
            .with_capacity(1)
            .with_events(vec![e2])
            .create()
            .unwrap();
        crate::engine::core::Flusher::new(
            mem,
            2,
            &seg2,
            registry.clone(),
            Arc::new(tokio::sync::Mutex::new(())),
        )
        .flush()
        .await
        .unwrap();
    }

    // Query key == "b" should favor segment-002 via .zxf
    let filter = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("b"))
        .create();
    let command = CommandFactory::query().with_event_type(event_type).create();
    let qplan = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(command)
        .build()
        .await;

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();

    let out1 = selector.select_for_segment("segment-001");
    let out2 = selector.select_for_segment("segment-002");
    assert!(out1.is_empty());
    assert!(!out2.is_empty());
}

#[tokio::test]
async fn range_pruner_uses_zonesurf_for_gt_and_lte() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    let seg2 = shard_dir.join("segment-002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "range_evt_field";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: ids 1, 9
    let e1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"id": 1}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"id": 9}))
        .create();
    let mem1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e1, e2])
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

    // seg2: ids 15, 20
    let e3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c")
        .with("payload", json!({"id": 15}))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "d")
        .with("payload", json!({"id": 20}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e3, e4])
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

    // id > 10 should select zones from segment-002
    let filter_gt = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let qplan = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &filter_gt,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();
    let out1 = selector.select_for_segment("segment-001");
    let out2 = selector.select_for_segment("segment-002");
    assert!(out1.is_empty());
    assert!(!out2.is_empty());

    // id <= 10 should select zones from segment-001
    let filter_lte = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Lte)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let ctx2 = SelectionContext {
        plan: &filter_lte,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector2 = ZoneSelectorBuilder::new(ctx2).build();
    let out1b = selector2.select_for_segment("segment-001");
    let out2b = selector2.select_for_segment("segment-002");
    assert!(!out1b.is_empty());
    assert!(out2b.is_empty());
}

#[tokio::test]
async fn enum_pruner_respects_eq_and_neq() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    let seg2 = shard_dir.join("segment-002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "enum_evt";
    reg_fac
        .define_with_field_types(
            event_type,
            &[
                ("context_id", FieldType::String),
                (
                    "plan",
                    FieldType::Enum(EnumType {
                        variants: vec![
                            "free".to_string(),
                            "pro".to_string(),
                            "premium".to_string(),
                        ],
                    }),
                ),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: free, pro
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"plan": "free"}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c2")
        .with("payload", json!({"plan": "pro"}))
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

    // seg2: premium
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c3")
        .with("payload", json!({"plan": "premium"}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![c])
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

    // plan == pro -> segment-001 only
    let f_eq = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &f_eq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let out1 = sel.select_for_segment("segment-001");
    let out2 = sel.select_for_segment("segment-002");
    assert!(!out1.is_empty());
    assert!(out2.is_empty());

    // plan != pro -> both segments
    let f_neq = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Neq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();
    let ctx2 = SelectionContext {
        plan: &f_neq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel2 = ZoneSelectorBuilder::new(ctx2).build();
    let o1 = sel2.select_for_segment("segment-001");
    let o2 = sel2.select_for_segment("segment-002");
    assert!(!o1.is_empty());
    assert!(!o2.is_empty());
}

#[tokio::test]
async fn returns_all_zones_when_value_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "missing_value_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let e = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"key": "x"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![e])
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

    // No value provided in FilterPlan
    let filter = FilterPlanFactory::new()
        .with_column("key")
        .with_uid(&uid)
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("segment-001");

    let all =
        crate::engine::core::zone::candidate_zone::CandidateZone::create_all_zones_for_segment(
            "segment-001",
        );
    assert_eq!(zones.len(), all.len());
}

#[tokio::test]
async fn returns_empty_when_uid_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "missing_uid_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let e = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"key": "x"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![e])
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

    // Missing uid
    let filter = FilterPlanFactory::new()
        .with_column("key")
        .with_value(json!("x"))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("segment-001");
    assert!(zones.is_empty());
}

#[tokio::test]
async fn xor_pruner_skips_on_neq_operation() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "neq_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let e = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"key": "x"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![e])
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

    // Neq should not use XOR; not enum; not range -> empty
    let filter = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Neq)
        .with_uid(&uid)
        .with_value(json!("x"))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("segment-001");
    assert!(zones.is_empty());
}

#[tokio::test]
async fn xor_pruner_falls_back_to_full_field_filter_when_zxf_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "fallback_xf_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let e = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"key": "x"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![e])
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

    // Remove .zxf to force fallback to .xf
    let zxf_path = ZoneXorFilterIndex::file_path(&seg1, &uid, "key");
    let _ = std::fs::remove_file(&zxf_path);

    let filter = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("x"))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("segment-001");
    let all =
        crate::engine::core::zone::candidate_zone::CandidateZone::create_all_zones_for_segment(
            "segment-001",
        );
    assert_eq!(zones.len(), all.len());
}

#[tokio::test]
async fn zonesurf_ge_gt_le_lte_cover_boundaries_and_cross_segment() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("segment-001");
    let seg2 = shard_dir.join("segment-002");
    std::fs::create_dir_all(&seg1).unwrap();
    std::fs::create_dir_all(&seg2).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "zonesurf_edges";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("user_id", "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: user_id 10 (boundary), 12
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"user_id": 10}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"user_id": 12}))
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

    // seg2: user_id 8, 10 (boundary)
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c")
        .with("payload", json!({"user_id": 8}))
        .create();
    let d = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "d")
        .with("payload", json!({"user_id": 10}))
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

    // GE boundary (>= 10) should include both segments
    let f_ge = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gte)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let ctx_ge = SelectionContext {
        plan: &f_ge,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_ge = ZoneSelectorBuilder::new(ctx_ge).build();
    let ge1 = sel_ge.select_for_segment("segment-001");
    let ge2 = sel_ge.select_for_segment("segment-002");
    assert!(!ge1.is_empty());
    assert!(!ge2.is_empty());

    // GT boundary (> 10) should include seg1 only (12), exclude seg2 (10 not > 10)
    let f_gt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let ctx_gt = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_gt = ZoneSelectorBuilder::new(ctx_gt).build();
    let gt1 = sel_gt.select_for_segment("segment-001");
    let gt2 = sel_gt.select_for_segment("segment-002");
    assert!(!gt1.is_empty());
    assert!(gt2.is_empty());

    // LE boundary (<= 10) should include seg2 (8,10), include seg1 (10)
    let f_le = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lte)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let ctx_le = SelectionContext {
        plan: &f_le,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_le = ZoneSelectorBuilder::new(ctx_le).build();
    let le1 = sel_le.select_for_segment("segment-001");
    let le2 = sel_le.select_for_segment("segment-002");
    assert!(!le1.is_empty());
    assert!(!le2.is_empty());

    // LT boundary (< 10) should include seg2 only (8)
    let f_lt = FilterPlanFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    let ctx_lt = SelectionContext {
        plan: &f_lt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_lt = ZoneSelectorBuilder::new(ctx_lt).build();
    let lt1 = sel_lt.select_for_segment("segment-001");
    let lt2 = sel_lt.select_for_segment("segment-002");
    assert!(lt1.is_empty());
    assert!(!lt2.is_empty());
}
