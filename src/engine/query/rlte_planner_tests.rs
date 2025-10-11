use std::collections::HashMap;
use std::path::PathBuf;

use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::engine::query::rlte_planner::plan_with_rlte;
use crate::test_helpers::factories::{CommandFactory, EventFactory, SchemaRegistryFactory};
use std::sync::Arc;

/// Build N segments each with Z zones under one shard dir, and write RLTE via ZoneWriter path.
async fn seed_segments_with_field(
    shard_dir: &PathBuf,
    event_type: &str,
    field: &str,
    segments: usize,
    zones_per_segment: usize,
) -> (
    Arc<tokio::sync::RwLock<crate::engine::schema::registry::SchemaRegistry>>,
    String,
) {
    use crate::engine::core::{ZonePlanner, ZoneWriter};
    use crate::shared::config::CONFIG;
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
        let segment_dir = shard_dir.join(format!("segment-{:05}", s));
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
                if name.starts_with("segment-") {
                    seg_ids.push(name.trim_start_matches("segment-").to_string());
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
        let rlte =
            RlteIndex::load(&uid, &shard_dir.join(format!("segment-{}", seg))).expect("load RLTE");
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
