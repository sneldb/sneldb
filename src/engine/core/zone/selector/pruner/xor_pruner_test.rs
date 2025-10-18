use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterPlanFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

#[tokio::test]
async fn xor_uses_zxf_primary_and_xf_fallback_and_skips_on_miss() {
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
    let event_type = "xor_evt";
    reg_fac
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // seg1: key values ["a", "b"]
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"key": "a"}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c2")
        .with("payload", json!({"key": "b"}))
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

    // seg2: key values ["c"]
    let c = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c3")
        .with("payload", json!({"key": "c"}))
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

    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;

    // 1) Primary: zxf should pick 002 for key == "c"
    let f_c = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("c"))
        .create();
    let ctx_c = SelectionContext {
        plan: &f_c,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_c = ZoneSelectorBuilder::new(ctx_c).build();
    let z1 = sel_c.select_for_segment("001");
    let z2 = sel_c.select_for_segment("002");
    assert!(z1.is_empty());
    assert!(!z2.is_empty());

    // 2) Fallback: remove .zxf for seg1 and query for existing value "a" -> should return all zones (xf presence only)
    let zxf_path = crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex::file_path(
        &seg1, &uid, "key",
    );
    let _ = std::fs::remove_file(&zxf_path);
    let f_a = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("a"))
        .create();
    let ctx_a = SelectionContext {
        plan: &f_a,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_a = ZoneSelectorBuilder::new(ctx_a).build();
    let xa1 = sel_a.select_for_segment("001");
    assert_eq!(
        xa1.len(),
        crate::engine::core::zone::candidate_zone::CandidateZone::create_all_zones_for_segment_from_meta(
            &shard_dir,
            "001",
            &uid
        )
        .len()
    );

    // 3) Negative: unknown value should return empty on both segments
    let f_unknown = FilterPlanFactory::new()
        .with_column("key")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("zzz"))
        .create();
    let ctx_u = SelectionContext {
        plan: &f_unknown,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel_u = ZoneSelectorBuilder::new(ctx_u).build();
    let u1 = sel_u.select_for_segment("001");
    let u2 = sel_u.select_for_segment("002");
    assert!(u1.is_empty());
    assert!(u2.is_empty());
}
