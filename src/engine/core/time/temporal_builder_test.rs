use super::temporal_builder::TemporalIndexBuilder;
use crate::engine::core::zone::zone_planner::ZonePlanner;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;

#[tokio::test]
async fn temporal_builder_writes_cal_and_tfi_for_datetime_field() {
    let tmp_dir = tempfile::tempdir().expect("tmpdir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt_tb";
    schema_factory
        .define_with_fields(
            event_type,
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("v", "int"),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Two zones worth of events with created_at
    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "a")
            .with("payload", json!({"created_at": 1_700_000_000u64, "v": 1}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "b")
            .with("payload", json!({"created_at": 1_700_000_500u64, "v": 2}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c")
            .with("payload", json!({"created_at": 1_700_001_000u64, "v": 3}))
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 2);
    let plans = planner.plan(&events).expect("plan");

    TemporalIndexBuilder::new(&uid, segment_dir, registry.clone())
        .build_for_zone_plans(&plans)
        .await
        .expect("build temporal");

    // Validate per-field calendar exists
    let cal_path = segment_dir.join(format!("{}_{}.cal", uid, "created_at"));
    assert!(
        cal_path.exists(),
        "missing calendar: {}",
        cal_path.display()
    );

    // Validate per-zone temporal index per field exists
    for zp in &plans {
        let tfi_path = segment_dir.join(format!("{}_{}_{}.tfi", uid, "created_at", zp.id));
        assert!(tfi_path.exists(), "missing tfi: {}", tfi_path.display());
    }
}

#[tokio::test]
async fn temporal_builder_skips_when_no_temporal_fields() {
    let tmp_dir = tempfile::tempdir().expect("tmpdir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt_no_time";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("name", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "x")
            .with("payload", json!({"name": "alice"}))
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 1);
    let plans = planner.plan(&events).expect("plan");

    TemporalIndexBuilder::new(&uid, segment_dir, registry.clone())
        .build_for_zone_plans(&plans)
        .await
        .expect("build temporal");

    // No calendars/tfi should be present
    let cal_glob = glob::glob(segment_dir.join(format!("{}_*.cal", uid)).to_str().unwrap())
        .unwrap()
        .count();
    assert_eq!(
        cal_glob, 0,
        "should not write calendars for non-temporal schema"
    );
}

#[tokio::test]
async fn temporal_builder_handles_multiple_temporal_fields() {
    let tmp_dir = tempfile::tempdir().expect("tmpdir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "evt_multi_time";
    schema_factory
        .define_with_fields(
            event_type,
            &[
                ("context_id", "string"),
                ("created_at", "datetime"),
                ("updated_at", "datetime"),
                ("due_date", "date"),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "m1")
            .with(
                "payload",
                json!({
                    "created_at": 1_700_000_000u64,
                    "updated_at": 1_700_000_100u64,
                    "due_date": 1_700_086_400u64
                }),
            )
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "m2")
            .with(
                "payload",
                json!({
                    "created_at": 1_700_000_200u64,
                    "updated_at": 1_700_000_300u64,
                    "due_date": 1_700_172_800u64
                }),
            )
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 2);
    let plans = planner.plan(&events).expect("plan");

    TemporalIndexBuilder::new(&uid, segment_dir, registry.clone())
        .build_for_zone_plans(&plans)
        .await
        .expect("build temporal");

    // Calendars for all temporal fields
    for field in ["created_at", "updated_at", "due_date"] {
        let cal = segment_dir.join(format!("{}_{}.cal", uid, field));
        assert!(
            cal.exists(),
            "missing calendar for {}: {}",
            field,
            cal.display()
        );
        for zp in &plans {
            let tfi = segment_dir.join(format!("{}_{}_{}.tfi", uid, field, zp.id));
            assert!(
                tfi.exists(),
                "missing tfi for {} zone {}: {}",
                field,
                zp.id,
                tfi.display()
            );
        }
    }
}
