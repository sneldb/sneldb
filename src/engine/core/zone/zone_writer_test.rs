use crate::engine::core::zone::rlte_index::RlteIndex;
use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;
use crate::engine::core::{FieldXorFilter, ZoneIndex, ZoneMeta, ZonePlanner, ZoneWriter};
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory};
use serde_json::json;

#[tokio::test]
async fn test_zone_writer_creates_all_outputs_correctly() {
    // Setup
    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let event_type = "user_created";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("key", "string")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "foo")
            .with("key", "abc")
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "bar")
            .with("key", "xyz")
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "baz")
            .with("key", "abc")
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 42);
    let plans = planner.plan(&events).expect("Failed to plan zones");

    let writer = ZoneWriter::new(&uid, segment_dir, registry.clone());
    writer.write_all(&plans).await.expect("ZoneWriter failed");

    // Validate .zones file
    let zones_path = segment_dir.join(format!("{}.zones", uid));
    let zones = ZoneMeta::load(&zones_path).expect("Failed to load zone meta");
    assert_eq!(zones.len(), plans.len());

    // Validate .idx file
    let index_path = segment_dir.join(format!("{}.idx", uid));
    let index = ZoneIndex::load_from_path(&index_path).expect("Failed to load index");
    let context_map = index.get(event_type).expect("Missing event_type in index");
    for event in &events {
        assert!(
            context_map.contains_key(&event.context_id),
            "Missing context_id: {}",
            event.context_id
        );
    }

    // Validate .xf filter for "context_id"
    let context_filter_path = segment_dir.join(format!("{}_context_id.xf", uid));
    let filter = FieldXorFilter::load(&context_filter_path).expect("Failed to load XOR filter");

    for ctx_id in ["foo", "bar", "baz"] {
        assert!(
            filter.contains(ctx_id),
            "Expected XOR filter to contain context_id={}",
            ctx_id
        );
    }

    assert!(
        !filter.contains("not_in_data"),
        "XOR filter should not contain 'not_in_data'"
    );

    // Validate per-zone XOR index (.zxf) for payload field "key" if present
    let zxf_path = segment_dir.join(format!("{}_key.zxf", uid));

    let zxf = ZoneXorFilterIndex::load(&zxf_path).expect("Failed to load .zxf index for key");
    // Default payload in EventFactory has key="value" for all events
    for zone_id in 0..plans.len() {
        assert!(
            zxf.contains_in_zone(zone_id as u32, &json!("value")),
            "Zone {} should maybe contain payload key=value",
            zone_id
        );
    }

    // Validate RLTE file exists and is readable
    let rlte_path = segment_dir.join(format!("{}.rlte", uid));
    assert!(
        rlte_path.exists(),
        "Expected RLTE file to be written: {:?}",
        rlte_path
    );
    let rlte = RlteIndex::load(&uid, segment_dir).expect("Failed to load RLTE index");
    // RLTE should exclude context_id (sorted by LSM), and include payload 'key'
    assert!(
        !rlte.ladders.contains_key("context_id"),
        "RLTE must exclude context_id ladder"
    );
    assert!(
        rlte.ladders.contains_key("key"),
        "RLTE must include payload 'key' ladder"
    );
    // Each recorded zone should have a non-empty ladder for 'key'
    if let Some(per_zone) = rlte.ladders.get("key") {
        assert!(!per_zone.is_empty(), "RLTE key should have zone entries");
        for (zone_id, ladder) in per_zone {
            assert!(
                !ladder.is_empty(),
                "RLTE ladder for zone {} should not be empty",
                zone_id
            );
        }
    }
}

#[tokio::test]
async fn test_zone_writer_skips_surf_for_datetime_and_builds_for_amount() {
    // Setup
    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let event_type = "orders";
    schema_factory
        .define_with_fields(
            event_type,
            &[
                ("context_id", "string"),
                ("ts", "datetime"),
                ("amount", "int"),
            ],
        )
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Create events with datetime payload and numeric amount
    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c1")
            .with("payload", json!({"ts": 1_700_000_000u64, "amount": 10}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c2")
            .with("payload", json!({"ts": 1_700_000_100u64, "amount": 20}))
            .create(),
    ];

    // Write via ZoneWriter
    let planner = ZonePlanner::new(&uid, 99);
    let plans = planner.plan(&events).expect("Failed to plan zones");
    let writer = ZoneWriter::new(&uid, segment_dir, registry.clone());
    writer.write_all(&plans).await.expect("ZoneWriter failed");

    // Validate SuRF behavior: amount exists; ts (datetime) skipped
    let amount_surf = segment_dir.join(format!("{}_{}.zsrf", uid, "amount"));
    assert!(
        amount_surf.exists(),
        "amount SuRF missing: {}",
        amount_surf.display()
    );
    let ts_surf = segment_dir.join(format!("{}_{}.zsrf", uid, "ts"));
    assert!(
        std::fs::metadata(&ts_surf).is_err(),
        "datetime field ts should not have SuRF"
    );

    // Validate per-field temporal artifacts exist: {uid}_{field}.cal and {uid}_{field}_{zone}.tfi
    let cal_path = segment_dir.join(format!("{}_{}.cal", uid, "ts"));
    assert!(
        cal_path.exists(),
        "per-field calendar missing for ts: {}",
        cal_path.display()
    );
    for zp in &plans {
        let tfi_path = segment_dir.join(format!("{}_{}_{}.tfi", uid, "ts", zp.id));
        assert!(
            tfi_path.exists(),
            "per-zone temporal index missing for ts zone {}: {}",
            zp.id,
            tfi_path.display()
        );
    }
}
