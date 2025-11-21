use std::sync::{Arc, Once};

use tempfile::tempdir;

use crate::engine::core::zone::selector::index_selector::{IndexZoneSelector, MissingIndexPolicy};
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::zone::zone_index::ZoneIndex;
use crate::engine::core::zone::zone_meta::ZoneMeta;

use crate::test_helpers::factories::command_factory::CommandFactory;
use crate::test_helpers::factories::query_plan_factory::QueryPlanFactory;
use crate::test_helpers::factories::schema_factory::SchemaRegistryFactory;
use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;

fn ensure_test_config() {
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        std::env::set_var("SNELDB_CONFIG", "config/test.toml");
    });
}

#[tokio::test]
async fn index_selector_no_context_returns_all_zones_on_missing_index() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let seg1 = tmp.path().join("seg1");
    std::fs::create_dir_all(&seg1).unwrap();

    // Schema and uid
    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_no_ctx";
    registry_fac
        .define_with_fields(event_type, &[("id", "u64")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Ensure zone metadata exists so the selector knows this segment holds the UID.
    let metas = vec![ZoneMeta {
        zone_id: 0,
        uid: uid.clone(),
        segment_id: 0,
        start_row: 0,
        end_row: 0,
        timestamp_min: 0,
        timestamp_max: 0,
        created_at: 0,
    }];
    ZoneMeta::save(&uid, &metas, &seg1).unwrap();

    // Plan
    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    let artifacts = ZoneArtifacts::new(&plan.segment_base_dir, None);
    let selector = IndexZoneSelector {
        plan: &plan,
        caches: None,
        artifacts,
        policy: MissingIndexPolicy::AllZonesIfNoContext,
        uid: &uid,
        event_type,
        context_id: None,
    };

    let zones = selector.select_for_segment("seg1");
    assert_eq!(
        zones.len(),
        metas.len(),
        "fallback should return the real zone count from metadata"
    );
    assert!(zones.iter().all(|z| z.segment_id == "seg1"));
}

#[tokio::test]
async fn index_selector_skips_segments_without_uid() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let seg1 = tmp.path().join("seg1");
    std::fs::create_dir_all(&seg1).unwrap();

    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_missing_uid";
    registry_fac
        .define_with_fields(event_type, &[("id", "u64")])
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

    let artifacts = ZoneArtifacts::new(&plan.segment_base_dir, None);
    let selector = IndexZoneSelector {
        plan: &plan,
        caches: None,
        artifacts,
        policy: MissingIndexPolicy::AllZonesIfNoContext,
        uid: &uid,
        event_type,
        context_id: None,
    };

    let zones = selector.select_for_segment("seg1");
    assert!(
        zones.is_empty(),
        "Segments without the requested uid should be skipped entirely"
    );
}

#[tokio::test]
async fn index_selector_aggregates_zones_from_index() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let seg1 = tmp.path().join("seg1");
    std::fs::create_dir_all(&seg1).unwrap();

    // Schema and uid
    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_no_ctx_index";
    registry_fac
        .define_with_fields(event_type, &[("context_id", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Write ZoneIndex with multiple contexts and multiple zone ids
    {
        let mut fac = ZoneIndexFactory::new();
        fac = fac.with_entry(event_type, "ctxA", 0);
        fac = fac.with_entry(event_type, "ctxA", 2);
        fac = fac.with_entry(event_type, "ctxB", 1);
        let idx: ZoneIndex = fac.create();
        idx.write_to_path(seg1.join(format!("{}.idx", uid)))
            .unwrap();
    }

    // Plan
    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    let artifacts = ZoneArtifacts::new(&plan.segment_base_dir, None);
    let selector = IndexZoneSelector {
        plan: &plan,
        caches: None,
        artifacts,
        policy: MissingIndexPolicy::AllZonesIfNoContext,
        uid: &uid,
        event_type,
        context_id: None,
    };

    let zones = selector.select_for_segment("seg1");
    // Expect unique zone ids {0,1,2}
    assert_eq!(zones.len(), 3, "expected aggregation of all zone ids");
    let mut ids: Vec<u32> = zones.iter().map(|z| z.zone_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2]);
    assert!(zones.iter().all(|z| z.segment_id == "seg1"));
}

#[tokio::test]
async fn index_selector_context_uses_index_when_present() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let seg1 = tmp.path().join("seg1");
    std::fs::create_dir_all(&seg1).unwrap();

    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_ctx_present";
    registry_fac
        .define_with_fields(event_type, &[("context_id", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Write ZoneIndex mapping (event_type, ctxA) -> [0]
    {
        let mut fac = ZoneIndexFactory::new();
        fac = fac.with_entry(event_type, "ctxA", 0);
        let idx: ZoneIndex = fac.create();
        idx.write_to_path(seg1.join(format!("{}.idx", uid)))
            .unwrap();
    }

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctxA")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;

    let artifacts = ZoneArtifacts::new(&plan.segment_base_dir, None);
    let selector = IndexZoneSelector {
        plan: &plan,
        caches: None,
        artifacts,
        policy: MissingIndexPolicy::AllZones,
        uid: &uid,
        event_type,
        context_id: Some("ctxA"),
    };

    let zones = selector.select_for_segment("seg1");
    assert!(!zones.is_empty());
    assert!(zones.iter().all(|z| z.segment_id == "seg1"));
}

#[tokio::test]
async fn index_selector_keeps_segment_when_high_water_timestamp_present() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let seg1 = tmp.path().join("seg1");
    std::fs::create_dir_all(&seg1).unwrap();

    let registry_fac = SchemaRegistryFactory::new();
    let registry = registry_fac.registry();
    let event_type = "ev_high_water_guard";
    registry_fac
        .define_with_fields(event_type, &[("id", "u64")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let metas = vec![
        ZoneMeta {
            zone_id: 0,
            uid: uid.clone(),
            segment_id: 0,
            start_row: 0,
            end_row: 0,
            timestamp_min: 100,
            timestamp_max: 100,
            created_at: 100,
        },
        ZoneMeta {
            zone_id: 1,
            uid: uid.clone(),
            segment_id: 0,
            start_row: 1,
            end_row: 1,
            timestamp_min: 100,
            timestamp_max: 100,
            created_at: 100,
        },
    ];
    ZoneMeta::save(&uid, &metas, &seg1).unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let mut plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tmp.path())
        .with_segment_ids(vec!["seg1".into()])
        .create()
        .await;
    plan.metadata
        .insert("materialization_created_at".into(), "100".into());
    plan.metadata
        .insert("materialization_high_water_ts".into(), "100".into());

    let artifacts = ZoneArtifacts::new(&plan.segment_base_dir, None);
    let selector = IndexZoneSelector {
        plan: &plan,
        caches: None,
        artifacts,
        policy: MissingIndexPolicy::AllZones,
        uid: &uid,
        event_type,
        context_id: None,
    };

    let zones = selector.select_for_segment("seg1");
    assert_eq!(
        zones.len(),
        metas.len(),
        "high-water metadata should prevent early pruning"
    );
}
