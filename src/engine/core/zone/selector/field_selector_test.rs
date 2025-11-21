use std::collections::HashSet;
use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::{Command, CompareOp, Expr};
use crate::engine::core::ZoneMeta;
use crate::engine::core::read::cache::QueryCaches;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::candidate_zone::CandidateZone;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::{Flusher, InflightSegments};
use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterGroupFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

#[tokio::test]
async fn xor_eq_uses_zxf_to_narrow_zones() {
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
    let event_type = "purchase";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
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
        Flusher::new(
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
        Flusher::new(
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

    // Query key == "b" should favor 002 via .zxf
    let mut filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!("b"))
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneXorIndex {
            field: "key".to_string(),
        });
    }
    let command = CommandFactory::query().with_event_type(event_type).create();
    let qplan = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(command)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".into(), "002".into()])
        .create()
        .await;

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();

    let out1 = selector.select_for_segment("001");
    let out2 = selector.select_for_segment("002");
    assert!(out1.is_empty());
    assert!(!out2.is_empty());

    // For segment without the .zxf, fall back to FullScan to avoid probing
    let mut filter_fs = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!("b"))
        .create();
    if let Some(strategy) = filter_fs.index_strategy_mut() {
        *strategy = Some(IndexStrategy::FullScan);
    }
    let ctx_fs = SelectionContext {
        plan: &filter_fs,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector_fs = ZoneSelectorBuilder::new(ctx_fs).build();
    let out1_fs = selector_fs.select_for_segment("001");
    assert!(!out1_fs.is_empty());
}

#[tokio::test]
async fn timestamp_filter_falls_back_when_uid_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ts_fallback_evt";
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
    let qplan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let timestamp_filter = qplan
        .filter_groups
        .iter()
        .find(|fg| fg.column() == Some("timestamp"))
        .expect("timestamp filter present");

    let ctx = SelectionContext {
        plan: timestamp_filter,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("00000");

    let expected = CandidateZone::create_all_zones_for_segment_from_meta(&shard_dir, "00000", &uid);
    assert_eq!(
        zones.len(),
        expected.len(),
        "missing uid on timestamp filter should fall back to all zones"
    );
    assert!(!zones.is_empty());
}

#[tokio::test]
async fn falls_back_to_full_scan_when_strategy_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "login";
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
        .with_event_type(event_type)
        .with_context_id("alice")
        .create();
    let qplan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let filter = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("alice"))
        .create();

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();
    let zones = selector.select_for_segment("00000");

    let expected = CandidateZone::create_all_zones_for_segment_from_meta(&shard_dir, "00000", &uid);
    assert_eq!(
        zones.len(),
        expected.len(),
        "missing strategy should fall back to full scan"
    );
    assert!(!zones.is_empty());
}

#[tokio::test]
async fn range_pruner_uses_zonesurf_for_gt_and_lte() {
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
    let event_type = "range_evt_field";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

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
    Flusher::new(
        mem1,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Verify per-field timestamp artifacts exist in seg1
    let ts_cal = seg1.join(format!(
        "{}_{}.cal",
        registry.read().await.get_uid(event_type).unwrap(),
        "timestamp"
    ));
    assert!(ts_cal.exists(), "missing timestamp calendar in seg1");
    let ts_slab = seg1.join(format!(
        "{}_{}.tfi",
        registry.read().await.get_uid(event_type).unwrap(),
        "timestamp"
    ));
    assert!(ts_slab.exists(), "missing timestamp slab in seg1");

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
    Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Verify per-field timestamp artifacts exist in seg2
    let ts_cal2 = seg2.join(format!(
        "{}_{}.cal",
        registry.read().await.get_uid(event_type).unwrap(),
        "timestamp"
    ));
    assert!(ts_cal2.exists(), "missing timestamp calendar in seg2");
    let ts_slab2 = seg2.join(format!(
        "{}_{}.tfi",
        registry.read().await.get_uid(event_type).unwrap(),
        "timestamp"
    ));
    assert!(ts_slab2.exists(), "missing timestamp slab in seg2");

    // id > 10 should select zones from 002
    let mut filter_gt = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = filter_gt.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "id".to_string(),
        });
    }
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
    let out1 = selector.select_for_segment("001");
    let out2 = selector.select_for_segment("002");
    assert!(out1.is_empty());
    assert!(!out2.is_empty());

    // id <= 10 should select zones from 001
    let mut filter_lte = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Lte)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = filter_lte.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "id".to_string(),
        });
    }
    let ctx2 = SelectionContext {
        plan: &filter_lte,
        query_plan: &qplan,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector2 = ZoneSelectorBuilder::new(ctx2).build();
    let out1b = selector2.select_for_segment("001");
    let out2b = selector2.select_for_segment("002");
    assert!(!out1b.is_empty());
    assert!(out2b.is_empty());
}

#[tokio::test]
async fn enum_pruner_respects_eq_and_neq() {
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
    Flusher::new(
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
    Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // plan == pro -> 001 only
    let mut f_eq = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!("pro"))
        .create();
    if let Some(strategy) = f_eq.index_strategy_mut() {
        *strategy = Some(IndexStrategy::EnumBitmap {
            field: "plan".to_string(),
        });
    }
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".into()])
        .create()
        .await;
    let ctx = SelectionContext {
        plan: &f_eq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let out1 = sel.select_for_segment("001");
    let out2 = sel.select_for_segment("002");
    assert!(!out1.is_empty());
    assert!(out2.is_empty());

    // plan != pro -> both segments
    let mut f_neq = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Neq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!("pro"))
        .create();
    if let Some(strategy) = f_neq.index_strategy_mut() {
        *strategy = Some(IndexStrategy::EnumBitmap {
            field: "plan".to_string(),
        });
    }
    let ctx2 = SelectionContext {
        plan: &f_neq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel2 = ZoneSelectorBuilder::new(ctx2).build();
    let o1 = sel2.select_for_segment("001");
    let o2 = sel2.select_for_segment("002");
    assert!(!o1.is_empty());
    assert!(!o2.is_empty());
}

#[tokio::test]
async fn returns_all_zones_when_value_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "missing_value_evt";
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
    Flusher::new(
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
    let filter = FilterGroupFactory::new()
        .with_column("key")
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".into()])
        .create()
        .await;
    let uid = registry.read().await.get_uid(event_type).unwrap();
    assert!(
        q.segment_maybe_contains_uid("001", &uid),
        "test setup should register zones for uid"
    );
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("001");

    let all = CandidateZone::create_all_zones_for_segment_from_meta(&shard_dir, "001", &uid);
    assert_eq!(zones.len(), all.len());
}

#[tokio::test]
async fn falls_back_when_uid_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
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
    Flusher::new(
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
    let filter = FilterGroupFactory::new()
        .with_column("key")
        .with_value(json!("x"))
        .create();
    let uid = registry.read().await.get_uid(event_type).unwrap();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".into()])
        .create()
        .await;
    assert!(
        q.segment_maybe_contains_uid("001", &uid),
        "test setup should register zones for uid"
    );
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("001");
    let expected = CandidateZone::create_all_zones_for_segment_from_meta(&shard_dir, "001", &uid);
    let zone_keys: HashSet<(String, u32)> = zones
        .iter()
        .map(|z| (z.segment_id.clone(), z.zone_id))
        .collect();
    assert!(
        expected
            .iter()
            .all(|exp| zone_keys.contains(&(exp.segment_id.clone(), exp.zone_id))),
        "fallback should include all zones from the matching uid"
    );
    assert!(!zones.is_empty());
}

#[tokio::test]
async fn xor_pruner_skips_on_neq_operation() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "neq_evt";
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
    Flusher::new(
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
    let filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Neq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!("x"))
        .create();
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".into()])
        .create()
        .await;
    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("001");
    let uid = registry.read().await.get_uid(event_type).unwrap();
    let expected = CandidateZone::create_all_zones_for_segment_from_meta(&shard_dir, "001", &uid);
    assert_eq!(
        zones.len(),
        expected.len(),
        "neq fallback should return all zones for the matching uid"
    );
    assert!(zones.iter().all(|z| z.segment_id == "001"));
}

#[tokio::test]
async fn zone_xor_missing_falls_back_when_segment_inflight() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg = shard_dir.join("00000");
    std::fs::create_dir_all(&seg).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "xor_inflight";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let event = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx")
        .with("payload", json!({"key": "present"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![event])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        0,
        &seg,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Remove the .zxf file to emulate a reader racing with flush completion.
    let uid = registry.read().await.get_uid(event_type).unwrap();
    let zxf_path = seg.join(format!("{}_{}.zxf", uid, "key"));
    std::fs::remove_file(&zxf_path).expect("remove zxf");

    let inflight = InflightSegments::new();
    let mut cmd = CommandFactory::query().with_event_type(event_type).create();
    // Force WHERE key = "present" so the planner assigns ZoneXorIndex
    if let Command::Query { where_clause, .. } = &mut cmd {
        *where_clause = Some(Expr::Compare {
            field: "key".into(),
            op: CompareOp::Eq,
            value: json!("present"),
        });
    }

    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd.clone())
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".to_string()])
        .with_inflight_segments(inflight.clone())
        .create()
        .await;

    let mut filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("present"))
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneXorIndex {
            field: "key".to_string(),
        });
    }

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let selector = ZoneSelectorBuilder::new(ctx).build();

    let guard = inflight.guard("00000");
    let zones = selector.select_for_segment("00000");
    assert!(
        !zones.is_empty(),
        "in-flight segments should fall back to metadata when zxf is missing"
    );
    drop(guard);

    // Once the segment is no longer in-flight we should revert to the stricter behavior.
    let zones_after = selector.select_for_segment("00000");
    assert!(
        zones_after.is_empty(),
        "without inflight marker the missing zxf should result in no candidates"
    );
}

#[tokio::test]
async fn zonesurf_ge_gt_le_lte_cover_boundaries_and_cross_segment() {
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
    let event_type = "zonesurf_edges";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("user_id", "int")])
        .await
        .unwrap();

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
    Flusher::new(
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
    Flusher::new(
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
    let mut f_ge = FilterGroupFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gte)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = f_ge.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "user_id".to_string(),
        });
    }
    let ctx_ge = SelectionContext {
        plan: &f_ge,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_ge = ZoneSelectorBuilder::new(ctx_ge).build();
    let ge1 = sel_ge.select_for_segment("001");
    let ge2 = sel_ge.select_for_segment("002");
    assert!(!ge1.is_empty());
    assert!(!ge2.is_empty());

    // GT boundary (> 10) should include seg1 only (12), exclude seg2 (10 not > 10)
    let mut f_gt = FilterGroupFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Gt)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = f_gt.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "user_id".to_string(),
        });
    }
    let ctx_gt = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_gt = ZoneSelectorBuilder::new(ctx_gt).build();
    let gt1 = sel_gt.select_for_segment("001");
    let gt2 = sel_gt.select_for_segment("002");
    assert!(!gt1.is_empty());
    assert!(gt2.is_empty());

    // LE boundary (<= 10) should include seg2 (8,10), include seg1 (10)
    let mut f_le = FilterGroupFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lte)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = f_le.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "user_id".to_string(),
        });
    }
    let ctx_le = SelectionContext {
        plan: &f_le,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_le = ZoneSelectorBuilder::new(ctx_le).build();
    let le1 = sel_le.select_for_segment("001");
    let le2 = sel_le.select_for_segment("002");
    assert!(!le1.is_empty());
    assert!(!le2.is_empty());

    // LT boundary (< 10) should include seg2 only (8)
    let mut f_lt = FilterGroupFactory::new()
        .with_column("user_id")
        .with_operation(CompareOp::Lt)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(10))
        .create();
    if let Some(strategy) = f_lt.index_strategy_mut() {
        *strategy = Some(IndexStrategy::ZoneSuRF {
            field: "user_id".to_string(),
        });
    }
    let ctx_lt = SelectionContext {
        plan: &f_lt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_lt = ZoneSelectorBuilder::new(ctx_lt).build();
    let lt1 = sel_lt.select_for_segment("001");
    let lt2 = sel_lt.select_for_segment("002");
    assert!(lt1.is_empty());
    assert!(!lt2.is_empty());
}

#[tokio::test]
async fn temporal_pruner_routed_by_field_selector() {
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
    let event_type = "ts_evt";
    // No need to declare timestamp in schema; it's a fixed field
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("k", "string")])
        .await
        .unwrap();

    // seg1: timestamps 100, 150
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("timestamp", json!(100u64))
        .with("context_id", "a")
        .with("payload", json!({"k":"x"}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("timestamp", json!(150u64))
        .with("context_id", "b")
        .with("payload", json!({"k":"y"}))
        .create();
    let mem1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a, b])
        .create()
        .unwrap();
    Flusher::new(
        mem1,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // seg2: timestamp 200
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("timestamp", json!(200u64))
        .with("context_id", "c")
        .with("payload", json!({"k":"z"}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![c])
        .create()
        .unwrap();
    Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Build query plan
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // timestamp == 150 should select only seg1
    let mut f_eq = FilterGroupFactory::new()
        .with_column("timestamp")
        .with_operation(CompareOp::Eq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(150u64))
        .create();
    if let Some(strategy) = f_eq.index_strategy_mut() {
        *strategy = Some(IndexStrategy::TemporalEq {
            field: "timestamp".to_string(),
        });
    }
    let ctx_eq = SelectionContext {
        plan: &f_eq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_eq = ZoneSelectorBuilder::new(ctx_eq).build();
    let z1 = sel_eq.select_for_segment("001");
    let z2 = sel_eq.select_for_segment("002");
    assert!(!z1.is_empty(), "seg1 must be selected for ts==150");
    assert!(z2.is_empty(), "seg2 must not be selected for ts==150");

    // timestamp > 180 should select only seg2
    let mut f_gt = FilterGroupFactory::new()
        .with_column("timestamp")
        .with_operation(CompareOp::Gt)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(180u64))
        .create();
    if let Some(strategy) = f_gt.index_strategy_mut() {
        *strategy = Some(IndexStrategy::TemporalRange {
            field: "timestamp".to_string(),
        });
    }
    let ctx_gt = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_gt = ZoneSelectorBuilder::new(ctx_gt).build();
    let g1 = sel_gt.select_for_segment("001");
    let g2 = sel_gt.select_for_segment("002");
    assert!(g1.is_empty(), "seg1 must be excluded for ts>180");
    assert!(!g2.is_empty(), "seg2 must be selected for ts>180");
}

#[tokio::test]
async fn temporal_payload_field_routed_by_field_selector() {
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
    let event_type = "ts_payload_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("ts", "datetime")])
        .await
        .unwrap();

    // seg1: ts 100, 150
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"ts": 100u64}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "b")
        .with("payload", json!({"ts": 150u64}))
        .create();
    let mem1 = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a, b])
        .create()
        .unwrap();
    Flusher::new(
        mem1,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // seg2: ts 200
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c")
        .with("payload", json!({"ts": 200u64}))
        .create();
    let mem2 = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![c])
        .create()
        .unwrap();
    Flusher::new(
        mem2,
        2,
        &seg2,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Build query plan
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // ts == 150 should select only seg1
    let mut f_eq = FilterGroupFactory::new()
        .with_column("ts")
        .with_operation(CompareOp::Eq)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(150u64))
        .create();
    if let Some(strategy) = f_eq.index_strategy_mut() {
        *strategy = Some(IndexStrategy::TemporalEq {
            field: "ts".to_string(),
        });
    }
    let ctx_eq = SelectionContext {
        plan: &f_eq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_eq = ZoneSelectorBuilder::new(ctx_eq).build();
    let z1 = sel_eq.select_for_segment("001");
    let z2 = sel_eq.select_for_segment("002");
    assert!(!z1.is_empty(), "seg1 must be selected for ts==150");
    assert!(z2.is_empty(), "seg2 must not be selected for ts==150");

    // ts > 180 should select only seg2
    let mut f_gt = FilterGroupFactory::new()
        .with_column("ts")
        .with_operation(CompareOp::Gt)
        .with_uid(&registry.read().await.get_uid(event_type).unwrap())
        .with_value(json!(180u64))
        .create();
    if let Some(strategy) = f_gt.index_strategy_mut() {
        *strategy = Some(IndexStrategy::TemporalRange {
            field: "ts".to_string(),
        });
    }
    let ctx_gt = SelectionContext {
        plan: &f_gt,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_gt = ZoneSelectorBuilder::new(ctx_gt).build();
    let g1 = sel_gt.select_for_segment("001");
    let g2 = sel_gt.select_for_segment("002");
    assert!(g1.is_empty(), "seg1 must be excluded for ts>180");
    assert!(!g2.is_empty(), "seg2 must be selected for ts>180");
}

#[tokio::test]
async fn materialization_pruner_filters_zones_created_before_materialization() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "materialization_test";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();
    let materialization_created_at = 1500u64;

    // Create zones with different created_at timestamps
    // Zone 0: created_at = 1000 (before materialization)
    // Zone 1: created_at = 2000 (after materialization)
    let zone_0 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 1000u64)
        .create();

    let zone_1 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 2000u64)
        .create();

    ZoneMeta::save(&uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    // Create query plan with materialization metadata
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let mut q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".to_string()])
        .create()
        .await;
    q.set_metadata(
        "materialization_created_at".to_string(),
        materialization_created_at.to_string(),
    );

    let mut filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("test"))
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::FullScan);
    }

    let caches = QueryCaches::new(shard_dir.clone());

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: Some(&caches),
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let result = sel.select_for_segment("001");

    // Only zone 1 (created_at=2000 > 1500) should be retained
    assert_eq!(
        result.len(),
        1,
        "Only zone 1 should remain after materialization pruning"
    );
    assert_eq!(result[0].zone_id, 1);
    assert_eq!(result[0].segment_id, "001");
}

#[tokio::test]
async fn materialization_pruner_no_filter_when_metadata_missing() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "no_mat_test";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Create two zones
    let zone_0 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", 1000u64)
        .create();

    let zone_1 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", 2000u64)
        .create();

    ZoneMeta::save(&uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    // Create query plan WITHOUT materialization metadata
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".to_string()])
        .create()
        .await;

    let mut filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("test"))
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::FullScan);
    }

    let caches = QueryCaches::new(shard_dir.clone());

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: Some(&caches),
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let result = sel.select_for_segment("001");

    // Without materialization metadata, all zones should be retained
    assert_eq!(
        result.len(),
        2,
        "All zones should be retained when materialization metadata is missing"
    );
}

#[tokio::test]
async fn materialization_pruner_filters_zones_with_equal_created_at() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "equal_created_at_test";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Zone created_at exactly equals materialization_created_at
    let materialization_created_at = 1500u64;

    let zone_0 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 0)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 0)
        .with("end_row", 99)
        .with("timestamp_min", 1_000_000u64)
        .with("timestamp_max", 1_000_999u64)
        .with("created_at", materialization_created_at) // Equal to materialization time
        .create();

    let zone_1 = crate::test_helpers::factory::Factory::zone_meta()
        .with("zone_id", 1)
        .with("uid", uid.as_str())
        .with("segment_id", 1u64)
        .with("start_row", 100)
        .with("end_row", 199)
        .with("timestamp_min", 1_001_000u64)
        .with("timestamp_max", 1_001_999u64)
        .with("created_at", materialization_created_at + 1) // One millisecond after
        .create();

    ZoneMeta::save(&uid, &[zone_0.clone(), zone_1.clone()], &seg1).unwrap();

    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let mut q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["001".to_string()])
        .create()
        .await;
    q.set_metadata(
        "materialization_created_at".to_string(),
        materialization_created_at.to_string(),
    );

    let mut filter = FilterGroupFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("test"))
        .create();
    if let Some(strategy) = filter.index_strategy_mut() {
        *strategy = Some(IndexStrategy::FullScan);
    }

    let caches = QueryCaches::new(shard_dir.clone());

    let ctx = SelectionContext {
        plan: &filter,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: Some(&caches),
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let result = sel.select_for_segment("001");

    // Zone 0 (created_at == materialization_created_at) should be filtered out
    // Zone 1 (created_at > materialization_created_at) should be retained
    assert_eq!(
        result.len(),
        1,
        "Only zone 1 should remain (zone 0 has equal created_at)"
    );
    assert_eq!(result[0].zone_id, 1, "Zone 1 should be retained");
}
