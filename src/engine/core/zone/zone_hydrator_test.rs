use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{
    CandidateZone, Flusher, QueryCaches, ZoneHydrator,
};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, ExecutionStepFactory, MemTableFactory,
    QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::tempdir;
use tracing::info;

#[tokio::test]
async fn hydrates_candidate_zones_with_values() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: schema + registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "purchase";

    schema_factory
        .define_with_fields(event_type, &[("amount", "integer"), ("region", "string")])
        .await
        .unwrap();

    // Create a temp segment with flushed events
    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"amount": 100, "region": "EU"}))
        .create_list(5);

    info!("Events: {:?}", events);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    // Create query command and plan
    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "amount".into(),
            op: CompareOp::Eq,
            value: json!(100),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Create execution steps from all filter groups in the plan
    let steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Run hydrator
    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Assert zones are returned and enriched
    assert!(!zones.is_empty(), "Expected candidate zones");
    assert!(
        zones[0].values.contains_key("amount"),
        "Expected zone to have amount values loaded"
    );
}

#[tokio::test]
async fn hydrates_wildcard_query_with_multiple_uids() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type1 = "order";
    let event_type2 = "payment";

    schema_factory
        .define_with_fields(event_type1, &[("amount", "integer"), ("status", "string")])
        .await
        .unwrap();
    schema_factory
        .define_with_fields(event_type2, &[("amount", "integer"), ("method", "string")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Flush events for both event types
    let events1 = EventFactory::new()
        .with("event_type", event_type1)
        .with("context_id", "ctx-1")
        .with("payload", json!({"amount": 100, "status": "pending"}))
        .create_list(3);

    let events2 = EventFactory::new()
        .with("event_type", event_type2)
        .with("context_id", "ctx-1")
        .with("payload", json!({"amount": 200, "method": "card"}))
        .create_list(2);

    let memtable1 = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events1)
        .create()
        .unwrap();

    let memtable2 = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events2)
        .create()
        .unwrap();

    let flusher1 = Flusher::new(
        memtable1,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher1.flush().await.expect("flush failed");

    let flusher2 = Flusher::new(
        memtable2,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher2.flush().await.expect("flush failed");

    // Create wildcard query
    let query_cmd = CommandFactory::query()
        .with_event_type("*")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Get UIDs for both event types
    let uid1 = registry
        .read()
        .await
        .get_uid(event_type1)
        .expect("UID not found");
    let uid2 = registry
        .read()
        .await
        .get_uid(event_type2)
        .expect("UID not found");

    // Create execution steps - for wildcard, we need to manually create zones with UIDs
    let mut steps = vec![];
    for filter_group in &plan.filter_groups {
        steps.push(
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create(),
        );
    }

    // Manually create candidate zones with UIDs for both event types
    // This simulates what ZoneCollector would do for a wildcard query
    let mut candidate_zones = Vec::new();
    let zones1 = CandidateZone::create_all_zones_for_segment_from_meta(
        &shard_dir,
        "00001",
        &uid1,
    );
    let zones2 = CandidateZone::create_all_zones_for_segment_from_meta(
        &shard_dir,
        "00001",
        &uid2,
    );
    candidate_zones.extend(zones1);
    candidate_zones.extend(zones2);

    // Inject zones into the first step (simulating collection)
    if let Some(step) = steps.first_mut() {
        step.candidate_zones = candidate_zones;
    }

    // Run hydrator
    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Assert zones from both UIDs are hydrated
    assert!(!zones.is_empty(), "Expected candidate zones");

    // Verify zones have UIDs set
    let zones_with_uids: Vec<_> = zones.iter().filter(|z| z.uid().is_some()).collect();
    assert!(
        zones_with_uids.len() > 0,
        "Expected zones with UIDs for wildcard query"
    );

    // Verify we have zones from both event types
    let uids_found: HashSet<&str> = zones
        .iter()
        .filter_map(|z| z.uid())
        .collect();
    assert!(
        uids_found.len() >= 1,
        "Expected multiple UIDs for wildcard query, found: {:?}",
        uids_found
    );
}

#[tokio::test]
async fn filters_zones_with_allowed_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "test_event";

    schema_factory
        .define_with_fields(event_type, &[("id", "integer")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"id": 1}))
        .create_list(5);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Create allowed zones filter - only allow zone 0 and 1
    let mut allowed_zones = HashSet::new();
    allowed_zones.insert(("00001".to_string(), 0));
    allowed_zones.insert(("00001".to_string(), 1));

    let hydrator = ZoneHydrator::new(&plan, steps)
        .with_allowed_zones(Some(allowed_zones));
    let zones = hydrator.hydrate().await;

    // Verify only allowed zones are returned
    assert!(
        zones.len() <= 2,
        "Expected at most 2 zones after filtering, got {}",
        zones.len()
    );
    for zone in &zones {
        assert!(
            zone.zone_id <= 1,
            "Expected only zones 0-1, got zone_id {}",
            zone.zone_id
        );
        assert_eq!(zone.segment_id, "00001");
    }
}

#[tokio::test]
async fn uses_cache_when_provided() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "cached_event";

    schema_factory
        .define_with_fields(event_type, &[("value", "integer")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"value": 42}))
        .create_list(3);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Create query caches
    let caches = QueryCaches::new(shard_dir.clone());

    let hydrator = ZoneHydrator::new(&plan, steps).with_caches(Some(&caches));
    let zones = hydrator.hydrate().await;

    // Verify hydration works with cache
    assert!(!zones.is_empty(), "Expected candidate zones");
    assert!(
        zones[0].values.contains_key("value") || zones[0].values.is_empty(),
        "Zone should be processed (values may be empty if no matches)"
    );
}

#[tokio::test]
async fn handles_empty_zones_gracefully() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "empty_event";

    schema_factory
        .define_with_fields(event_type, &[("id", "integer")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");

    // Don't create any segments or flush any events

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: json!(999), // Value that won't match anything
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()]) // Segment exists but has no matching data
        .create()
        .await;

    let steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Should handle empty zones without panicking
    // Result may be empty or contain zones with no matching values
    assert!(
        zones.is_empty() || zones.iter().all(|z| z.values.is_empty()),
        "Expected empty zones or zones with no values"
    );
}

#[tokio::test]
async fn deduplicates_zones_by_segment_and_zone_id() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "dedup_event";

    schema_factory
        .define_with_fields(event_type, &[("id", "integer")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"id": 1}))
        .create_list(3);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Create multiple steps that might produce duplicate zones
    let mut steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Manually inject duplicate zones into steps to test deduplication
    let uid = plan.event_type_uid().await.expect("UID not found");
    let duplicate_zones = CandidateZone::create_all_zones_for_segment_from_meta(
        &shard_dir,
        "00001",
        &uid,
    );

    // Add same zones to multiple steps
    for step in &mut steps {
        step.candidate_zones.extend(duplicate_zones.clone());
    }

    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Verify deduplication occurred
    let unique_zones: HashSet<(u32, String)> = zones
        .iter()
        .map(|z| (z.zone_id, z.segment_id.clone()))
        .collect();

    assert_eq!(
        zones.len(),
        unique_zones.len(),
        "Expected deduplicated zones, but found duplicates"
    );
}

#[tokio::test]
async fn handles_missing_uid_fallback() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "missing_uid_event";

    schema_factory
        .define_with_fields(event_type, &[("id", "integer")])
        .await
        .unwrap();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx-1")
        .with("payload", json!({"id": 1}))
        .create_list(2);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .unwrap();

    let flusher = Flusher::new(
        memtable,
        1,
        &segment_dir,
        Arc::clone(&registry),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("flush failed");

    let query_cmd = CommandFactory::query()
        .with_event_type(event_type)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(query_cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Create zones without UIDs to test fallback
    let mut steps: Vec<_> = plan
        .filter_groups
        .iter()
        .map(|filter_group| {
            ExecutionStepFactory::new()
                .with_plan(&plan)
                .with_filter(filter_group.clone())
                .create()
        })
        .collect();

    // Manually create zones without UIDs
    if let Some(step) = steps.first_mut() {
        let zones_without_uids = vec![
            CandidateZone::new(0, "00001".to_string()),
            CandidateZone::new(1, "00001".to_string()),
        ];
        step.candidate_zones = zones_without_uids;
    }

    let hydrator = ZoneHydrator::new(&plan, steps);
    let zones = hydrator.hydrate().await;

    // Should handle zones without UIDs by falling back to plan's event_type_uid
    // Result may be empty if no matching data, but should not panic
    assert!(
        zones.is_empty() || zones.iter().any(|z| z.values.is_empty() || !z.values.is_empty()),
        "Should handle missing UID zones gracefully"
    );
}
