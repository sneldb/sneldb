use super::scope::collect_zones_for_scope;
use crate::engine::core::Flusher;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use std::sync::{Arc, Once};
use tempfile::tempdir;

fn ensure_test_config() {
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        std::env::set_var("SNELDB_CONFIG", "config/test.toml");
    });
}

#[tokio::test]
async fn wildcard_scope_returns_zones_even_without_uid_override() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Define two event types and flush them into the same segment.
    for (event_type, device) in [("login", "android"), ("signup", "web")] {
        registry_factory
            .define_with_fields(event_type, &[("device", "string")])
            .await
            .unwrap();
        let event = EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "alice")
            .with("device", device)
            .create();
        let mem = MemTableFactory::new()
            .with_capacity(1)
            .with_events(vec![event])
            .create()
            .unwrap();
        Flusher::new(
            mem,
            0,
            &segment_dir,
            Arc::clone(&registry),
            Arc::new(tokio::sync::Mutex::new(())),
        )
        .flush()
        .await
        .unwrap();
    }

    let command = CommandFactory::query()
        .with_event_type("*")
        .with_context_id("alice")
        .create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00000".into()])
        .create()
        .await;

    let zones = collect_zones_for_scope(&plan, None, "00000", None);
    assert!(
        !zones.is_empty(),
        "wildcard scope should still return zones"
    );
}

#[tokio::test]
async fn uid_override_prefers_specific_event_type() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00001");
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
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(events)
        .create()
        .unwrap();
    Flusher::new(
        mem,
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
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let zones = collect_zones_for_scope(&plan, None, "00001", Some(&uid));
    assert_eq!(
        zones.len(),
        2,
        "uid override should honor per-event-type metadata"
    );
}

#[tokio::test]
async fn specific_scope_skips_segments_without_uid() {
    ensure_test_config();
    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let segment_dir = shard_dir.join("00002");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "login_missing";
    registry_factory
        .define_with_fields(event_type, &[("device", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&shard_dir)
        .with_segment_ids(vec!["00002".into()])
        .create()
        .await;

    let zones = collect_zones_for_scope(&plan, None, "00002", None);
    assert!(
        zones.is_empty(),
        "segments lacking the uid should be skipped instead of returning placeholder zones"
    );
}
