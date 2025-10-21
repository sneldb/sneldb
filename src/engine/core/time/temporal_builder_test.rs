use super::temporal_builder::TemporalIndexBuilder;
use crate::engine::core::time::ZoneTemporalIndex;
use crate::engine::core::zone::zone_planner::ZonePlanner;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;

#[tokio::test]
async fn temporal_builder_writes_cal_and_slab_tfi_for_datetime_field() {
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
    // Timestamp (fixed field) calendar exists
    let ts_cal_path = segment_dir.join(format!("{}_{}.cal", uid, "timestamp"));
    assert!(
        ts_cal_path.exists(),
        "missing timestamp calendar: {}",
        ts_cal_path.display()
    );

    // Validate per-field slab temporal index exists once
    let slab_path = segment_dir.join(format!("{}_{}.tfi", uid, "created_at"));
    assert!(
        slab_path.exists(),
        "missing slab tfi: {}",
        slab_path.display()
    );
    // Timestamp (fixed field) slab exists
    let ts_slab_path = segment_dir.join(format!("{}_{}.tfi", uid, "timestamp"));
    assert!(
        ts_slab_path.exists(),
        "missing timestamp slab tfi: {}",
        ts_slab_path.display()
    );

    // Validate we can load per-zone index from the slab and it contains a known ts
    for zp in &plans {
        let zti = ZoneTemporalIndex::load_for_field(&uid, "created_at", zp.id, segment_dir)
            .expect("load zti from slab");
        // Pick a timestamp from this zone's events (if present) and assert it exists in zti
        if let Some(ev) = zp.events.get(0) {
            if let Some(ts) = ev
                .payload
                .as_object()
                .and_then(|o| o.get("created_at"))
                .and_then(|v| v.as_u64())
            {
                assert!(zti.contains_ts(ts as i64));
            }
        }
        // Also validate fixed timestamp slab loads and covers the event timestamp
        let zti_ts = ZoneTemporalIndex::load_for_field(&uid, "timestamp", zp.id, segment_dir)
            .expect("load zti for timestamp from slab");
        let ev_ts = zp.events[0].timestamp as i64;
        assert!(zti_ts.contains_ts(ev_ts));
    }
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

    // Calendars for all temporal fields and timestamp; slab per field
    for field in ["created_at", "updated_at", "due_date", "timestamp"] {
        let cal = segment_dir.join(format!("{}_{}.cal", uid, field));
        assert!(
            cal.exists(),
            "missing calendar for {}: {}",
            field,
            cal.display()
        );
        let slab = segment_dir.join(format!("{}_{}.tfi", uid, field));
        assert!(
            slab.exists(),
            "missing slab tfi for {}: {}",
            field,
            slab.display()
        );

        // Ensure each zone can be loaded from the slab and includes one of its timestamps
        for zp in &plans {
            let zti = ZoneTemporalIndex::load_for_field(&uid, field, zp.id, segment_dir)
                .expect("load zti from slab");
            // find a timestamp for this field within the zone, if present
            if field == "timestamp" {
                let ts = zp.events[0].timestamp;
                assert!(zti.contains_ts(ts as i64));
            } else if let Some(ts) = zp
                .events
                .iter()
                .filter_map(|ev| {
                    ev.payload
                        .as_object()
                        .and_then(|o| o.get(field))
                        .and_then(|v| v.as_u64())
                })
                .next()
            {
                assert!(
                    zti.contains_ts(ts as i64),
                    "loaded zti must contain known timestamp for field {} in zone {}",
                    field,
                    zp.id
                );
            }
        }
    }
}
