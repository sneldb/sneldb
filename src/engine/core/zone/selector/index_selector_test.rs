use std::sync::Arc;

use tempfile::tempdir;

use crate::engine::core::zone::candidate_zone::CandidateZone;
use crate::engine::core::zone::selector::index_selector::{IndexZoneSelector, MissingIndexPolicy};
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::zone::zone_index::ZoneIndex;

use crate::test_helpers::factories::command_factory::CommandFactory;
use crate::test_helpers::factories::query_plan_factory::QueryPlanFactory;
use crate::test_helpers::factories::schema_factory::SchemaRegistryFactory;
use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;

#[tokio::test]
async fn index_selector_no_context_returns_all_zones_on_missing_index() {
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
    let all_zones = CandidateZone::create_all_zones_for_segment("seg1");
    assert_eq!(zones.len(), all_zones.len());
    assert!(zones.iter().all(|z| z.segment_id == "seg1"));
}

#[tokio::test]
async fn index_selector_aggregates_zones_from_index() {
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
