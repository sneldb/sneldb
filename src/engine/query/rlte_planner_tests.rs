use std::collections::HashMap;
use std::path::PathBuf;

use crate::command::types::{CompareOp, Expr};
use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::engine::core::{ZonePlanner, ZoneWriter};
use crate::engine::query::rlte_planner::plan_with_rlte;
use crate::engine::schema::registry::SchemaRegistry;
use crate::shared::config::CONFIG;
use crate::test_helpers::factories::{CommandFactory, EventFactory, SchemaRegistryFactory};
use std::sync::Arc;

/// Build N segments each with Z zones under one shard dir, and write RLTE via ZoneWriter path.
async fn seed_segments_with_field(
    shard_dir: &PathBuf,
    event_type: &str,
    field: &str,
    segments: usize,
    zones_per_segment: usize,
) -> (Arc<tokio::sync::RwLock<SchemaRegistry>>, String) {
    use std::sync::Arc;
    std::fs::create_dir_all(shard_dir).unwrap();

    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields(event_type, &[(field, "int")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let zone_size = CONFIG.engine.event_per_zone;

    // Each segment: write zones_per_segment * zone_size events, with increasing field to create separable ladders
    for s in 0..segments {
        let segment_dir = shard_dir.join(format!("{:05}", s));
        std::fs::create_dir_all(&segment_dir).unwrap();

        let rows = zones_per_segment * zone_size;
        let mut events = Vec::with_capacity(rows);
        for z in 0..zones_per_segment {
            // Make zone z have values in increasing ranges so planner must keep several
            let base = (z * 100_000) as i64;
            for i in 0..zone_size {
                let val = base + i as i64;
                let payload = serde_json::json!({ field: val });
                events.push(
                    EventFactory::new()
                        .with("event_type", event_type)
                        .with("context_id", format!("ctx-s{}-z{}-i{}", s, z, i))
                        .with("payload", payload)
                        .create(),
                );
            }
        }

        // Directly run ZonePlanner + ZoneWriter to create segment artifacts including RLTE
        let plans = ZonePlanner::new(&uid, s as u64)
            .plan(&events)
            .expect("plan zones");
        let writer = ZoneWriter::new(&uid, &segment_dir, Arc::clone(&registry));
        writer.write_all(&plans).await.expect("zone write");
    }

    (registry, uid)
}

#[tokio::test]
async fn test_planner_keeps_expected_zones_across_segments() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let event_type = "evt_rlte";
    let field = "score";

    // With event_per_zone=2 (from test config), create enough zones to test planning
    // k = (limit + offset) * 10, so we need at least k/2 zones to have enough events
    // 3 segments × 100 zones × 2 events = 600 events, which is enough for k=170
    let (registry, uid) = seed_segments_with_field(&shard_dir, event_type, field, 3, 100).await;

    // Build command with ORDER BY + LIMIT
    // Request a range that spans multiple zones to test the planner
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_limit(15)
        .with_offset(2)
        .with_order_by(field, false)
        .create();

    // Build a transient plan for uid
    let plan = QueryPlan::build(&cmd, Arc::clone(&registry)).await;

    // Shard maps
    let mut bases = HashMap::new();
    bases.insert(0usize, shard_dir.clone());
    let mut segs = HashMap::new();
    let mut seg_ids = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&shard_dir) {
        for e in rd.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if name.chars().all(|c| c.is_ascii_digit()) {
                    seg_ids.push(name.to_string());
                }
            }
        }
    }
    seg_ids.sort();
    segs.insert(0usize, seg_ids.clone());

    // Run planner
    let out = plan_with_rlte(&plan, &bases, &segs)
        .await
        .expect("planner output");
    let picked = out.per_shard.get(&0).expect("shard 0 plan");

    // Sanity: should keep multiple zones since K spans across several zones
    assert!(
        picked.zones.len() > 1,
        "expected more than one zone to be kept"
    );

    // Ensure zones are ordered by rank-1 ascending for ASC
    let mut rank1s = Vec::new();
    for (seg, zid) in &picked.zones {
        let rlte = RlteIndex::load(&uid, &shard_dir.join(seg)).expect("load RLTE");
        let ladder = rlte
            .ladders
            .get(field)
            .and_then(|m| m.get(zid))
            .expect("ladder present");
        rank1s.push(ladder[0].clone());
    }
    let mut sorted = rank1s.clone();
    sorted.sort();
    assert_eq!(rank1s, sorted, "expected zones ordered by rank-1 (ASC)");
}

#[tokio::test]
async fn test_planner_asc_frontier_uses_min() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let event_type = "evt_frontier";
    let field = "score";

    // Write enough zones so k = (limit+offset)*10 can be satisfied
    // With event_per_zone=1 in tests and LIMIT=1 → k=10, so create >=10 zones
    let (registry, _uid) = seed_segments_with_field(&shard_dir, event_type, field, 1, 20).await;

    // ORDER BY ASC LIMIT small value forces planner to anchor at min frontier
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_order_by(field, false)
        .with_limit(1)
        .create();
    let plan = QueryPlan::build(&cmd, Arc::clone(&registry)).await;

    // Shard maps
    let mut bases = HashMap::new();
    bases.insert(0usize, shard_dir.clone());
    let mut segs = HashMap::new();
    let mut seg_ids = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&shard_dir) {
        for e in rd.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if name.chars().all(|c| c.is_ascii_digit()) {
                    seg_ids.push(name.to_string());
                }
            }
        }
    }
    seg_ids.sort();
    segs.insert(0usize, seg_ids);

    let out = plan_with_rlte(&plan, &bases, &segs)
        .await
        .expect("planner output");
    let picked = out.per_shard.get(&0).expect("shard 0 plan");

    // With event_per_zone small and LIMIT 1, planner should keep at least one zone (the one containing the global min)
    assert!(picked.zones.len() >= 1);
}

#[tokio::test]
async fn test_planner_where_lt_small_skips_rlte() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let event_type = "evt_rlte_skip";
    let field = "score";

    // Seed segments with large score ranges so that WHERE score < 10 prunes all zones
    let (registry, _uid) = seed_segments_with_field(&shard_dir, event_type, field, 2, 5).await;

    // Build command: WHERE score < 10, ORDER BY score ASC LIMIT 2
    let where_expr = Expr::Compare {
        field: field.to_string(),
        op: CompareOp::Lt,
        value: serde_json::json!(10),
    };
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(where_expr)
        .with_order_by(field, false)
        .with_limit(2)
        .create();

    let plan = QueryPlan::build(&cmd, Arc::clone(&registry)).await;

    // Shard maps
    let mut bases = HashMap::new();
    bases.insert(0usize, shard_dir.clone());
    let mut segs = HashMap::new();
    let mut seg_ids = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&shard_dir) {
        for e in rd.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if name.chars().all(|c| c.is_ascii_digit()) {
                    seg_ids.push(name.to_string());
                }
            }
        }
    }
    seg_ids.sort();
    segs.insert(0usize, seg_ids);

    // RLTE should be skipped (None) because no zones satisfy score < 10
    let out = plan_with_rlte(&plan, &bases, &segs).await;
    assert!(
        out.is_none(),
        "Expected RLTE planner to skip when WHERE prunes all zones"
    );
}

#[tokio::test]
async fn test_planner_desc_where_gt_small_limit() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let event_type = "evt_desc_gt";
    let field = "score";

    // Seed with multiple zones; values increase per zone.
    let (registry, _uid) = seed_segments_with_field(&shard_dir, event_type, field, 1, 20).await;

    // WHERE score > modest threshold should keep many zones; with small LIMIT, RLTE should plan
    let where_expr = Expr::Compare {
        field: field.to_string(),
        op: CompareOp::Gt,
        value: serde_json::json!(100_000),
    };
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(where_expr)
        .with_order_by(field, true)
        .with_limit(1)
        .create();
    let plan = QueryPlan::build(&cmd, Arc::clone(&registry)).await;

    // Shard maps
    let mut bases = HashMap::new();
    bases.insert(0usize, shard_dir.clone());
    let mut segs = HashMap::new();
    let mut seg_ids = Vec::new();
    if let Ok(rd) = std::fs::read_dir(&shard_dir) {
        for e in rd.flatten() {
            if let Some(name) = e.file_name().to_str() {
                if name.chars().all(|c| c.is_ascii_digit()) {
                    seg_ids.push(name.to_string());
                }
            }
        }
    }
    seg_ids.sort();
    segs.insert(0usize, seg_ids);

    // RLTE should produce some zones for DESC case
    let out = plan_with_rlte(&plan, &bases, &segs)
        .await
        .expect("planner output");
    let picked = out.per_shard.get(&0).expect("shard 0 plan");
    assert!(picked.zones.len() >= 1, "expected at least one zone kept");
}

#[tokio::test]
async fn test_planner_where_on_other_field_does_not_refine() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let event_type = "evt_other_field";
    let order_field = "score";
    let other_field = "rank";

    // Define both fields; seed zones with increasing score and fixed rank
    let (registry, _uid) = {
        std::fs::create_dir_all(&shard_dir).unwrap();
        let factory = SchemaRegistryFactory::new();
        let registry = factory.registry();
        factory
            .define_with_fields(event_type, &[(order_field, "int"), (other_field, "int")])
            .await
            .unwrap();
        let uid = registry.read().await.get_uid(event_type).unwrap();

        let segment_dir = shard_dir.join("00000");
        std::fs::create_dir_all(&segment_dir).unwrap();
        let mut events = Vec::new();
        for z in 0..20 {
            for i in 0..CONFIG.engine.event_per_zone {
                let score = (z * 100_000 + i) as i64;
                let payload = serde_json::json!({ order_field: score, other_field: 1 });
                events.push(
                    EventFactory::new()
                        .with("event_type", event_type)
                        .with("context_id", format!("ctx-z{}-i{}", z, i))
                        .with("payload", payload)
                        .create(),
                );
            }
        }
        let plans = ZonePlanner::new(&uid, 0).plan(&events).expect("plan zones");
        let writer = ZoneWriter::new(&uid, &segment_dir, Arc::clone(&registry));
        writer.write_all(&plans).await.expect("zone write");
        (registry, uid)
    };

    // WHERE on different field should not prune RLTE candidates for score
    let where_expr = Expr::Compare {
        field: other_field.to_string(),
        op: CompareOp::Gt,
        value: serde_json::json!(0),
    };
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(where_expr)
        .with_order_by(order_field, false)
        .with_limit(1)
        .create();
    let plan = QueryPlan::build(&cmd, Arc::clone(&registry)).await;

    // Shard maps
    let mut bases = HashMap::new();
    bases.insert(0usize, shard_dir.clone());
    let mut segs = HashMap::new();
    segs.insert(0usize, vec!["00000".to_string()]);

    let out = plan_with_rlte(&plan, &bases, &segs)
        .await
        .expect("planner output");
    let picked = out.per_shard.get(&0).expect("shard 0 plan");
    assert!(
        picked.zones.len() >= 1,
        "expected zones not pruned by unrelated WHERE"
    );
}
