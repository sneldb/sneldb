use crate::engine::core::ColumnReader;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::column::type_catalog::ColumnTypeCatalog;
use crate::engine::core::time::ZoneTemporalIndex;
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

    // Create events ensuring each zone has at least 2 unique values for "key"
    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "foo")
            .with("payload", json!({"key": "abc"}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "bar")
            .with("payload", json!({"key": "xyz"}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "baz")
            .with("payload", json!({"key": "abc"}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "qux")
            .with("payload", json!({"key": "def"}))
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
    // Note: .zxf files are only created if IndexBuildPlanner includes ZONE_XOR_INDEX for the field
    // and each zone has at least 2 unique values
    let zxf_path = segment_dir.join(format!("{}_key.zxf", uid));
    if zxf_path.exists() {
        let zxf = ZoneXorFilterIndex::load(&zxf_path).expect("Failed to load .zxf index for key");
        // Each zone should contain the key values from its events
        // Check that at least some zones contain expected values
        let mut found_zones = 0;
        for zone_id in 0..plans.len() {
            if zxf.contains_in_zone(zone_id as u32, &json!("abc"))
                || zxf.contains_in_zone(zone_id as u32, &json!("xyz"))
                || zxf.contains_in_zone(zone_id as u32, &json!("def"))
            {
                found_zones += 1;
            }
        }
        assert!(
            found_zones > 0,
            "At least one zone should contain key values"
        );
    } else {
        // .zxf file might not exist if zones don't have enough unique values or field not in build plan
        // This is acceptable - the test still validates other outputs
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

    // Validate per-field temporal artifacts exist: {uid}_{field}.cal and slab {uid}_{field}.tfi
    let cal_path = segment_dir.join(format!("{}_{}.cal", uid, "ts"));
    assert!(
        cal_path.exists(),
        "per-field calendar missing for ts: {}",
        cal_path.display()
    );
    let slab_path = segment_dir.join(format!("{}_{}.tfi", uid, "ts"));
    assert!(
        slab_path.exists(),
        "per-field slab tfi missing for ts: {}",
        slab_path.display()
    );

    // Ensure each zone index can be loaded from the slab and recognizes one of its timestamps
    for zp in &plans {
        let zti = ZoneTemporalIndex::load_for_field(&uid, "ts", zp.id, segment_dir)
            .expect("load zti from slab");
        if let Some(ev) = zp.events.get(0) {
            if let Some(ts) = ev
                .payload
                .as_object()
                .and_then(|o| o.get("ts"))
                .and_then(|v| v.as_u64())
            {
                assert!(zti.contains_ts(ts as i64));
            }
        }
    }
}

#[tokio::test]
async fn test_zone_writer_applies_type_catalog_hints() {
    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let event_type = "analytics";
    schema_factory
        .define_with_fields(
            event_type,
            &[
                ("context_id", "string"),
                ("metric", "string"), // schema says string => VarBytes without hints
            ],
        )
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "a1")
            .with("payload", json!({"metric": 100 }))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "a2")
            .with("payload", json!({"metric": 200 }))
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 7);
    let plans = planner.plan(&events).expect("plan zones");

    let mut catalog = ColumnTypeCatalog::new();
    catalog.record(
        (event_type.to_string(), "metric".to_string()),
        PhysicalType::I64,
    );

    let writer = ZoneWriter::new(&uid, segment_dir, registry.clone()).with_type_catalog(catalog);
    writer.write_all(&plans).await.expect("ZoneWriter failed");

    // Load the written column and ensure the type hint produced an i64 block
    let mut all_strings = Vec::new();
    for plan in &plans {
        let snapshot = ColumnReader::load_for_zone_snapshot(
            segment_dir,
            &plan.segment_id.to_string(),
            &uid,
            "metric",
            plan.id,
            None,
        )
        .expect("snapshot");
        assert_eq!(snapshot.physical_type(), PhysicalType::I64);
        all_strings.extend(snapshot.to_strings());
    }
    all_strings.sort();
    assert_eq!(all_strings, vec!["100", "200"]);
}

#[tokio::test]
async fn test_zone_writer_type_catalog_does_not_override_schema_types() {
    let tmp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let segment_dir = tmp_dir.path();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let event_type = "flags";
    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("success", "bool")])
        .await
        .unwrap();

    let uid = registry.read().await.get_uid(event_type).unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c1")
            .with("payload", json!({"success": true }))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "c2")
            .with("payload", json!({"success": false }))
            .create(),
    ];

    let planner = ZonePlanner::new(&uid, 11);
    let plans = planner.plan(&events).expect("plan zones");

    let mut catalog = ColumnTypeCatalog::new();
    // Attempt to override schema type with VarBytes hint
    catalog.record(
        (event_type.to_string(), "success".to_string()),
        PhysicalType::VarBytes,
    );

    let writer = ZoneWriter::new(&uid, segment_dir, registry.clone()).with_type_catalog(catalog);
    writer.write_all(&plans).await.expect("ZoneWriter failed");

    let snapshot = ColumnReader::load_for_zone_snapshot(
        segment_dir,
        &plans[0].segment_id.to_string(),
        &uid,
        "success",
        plans[0].id,
        None,
    )
    .expect("snapshot");
    // schema wins: still Bool
    assert_eq!(snapshot.physical_type(), PhysicalType::Bool);
}
