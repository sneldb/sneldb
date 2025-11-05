use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::{ColumnReader, Compactor, ZoneMeta};
use crate::engine::core::{Event, Flusher};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

/// Test data structure to track expected values
#[derive(Debug)]
struct ExpectedEvent {
    context_id: String,
    email: String,
    timestamp: u64,
    purchase_total: f64,
    success: Option<bool>, // None means null
}

/// Helper function to create events for a segment
fn create_segment_events(segment_id: u32, base_timestamp: u64) -> (Vec<Event>, Vec<ExpectedEvent>) {
    let mut events = Vec::new();
    let mut expected = Vec::new();

    // Event 1: success=true
    let ctx_id_1 = format!("ctx{}", segment_id * 3 + 1);
    let email_1 = format!("{}@example.com", ctx_id_1);
    events.push(
        EventFactory::new()
            .with("event_type", "user_created")
            .with("context_id", ctx_id_1.clone())
            .with("email", email_1.clone())
            .with("timestamp", base_timestamp)
            .with(
                "payload",
                json!({
                    "email": email_1.clone(),
                    "purchase_total": 10.5,
                    "success": true
                }),
            )
            .create(),
    );
    expected.push(ExpectedEvent {
        context_id: ctx_id_1,
        email: email_1,
        timestamp: base_timestamp,
        purchase_total: 10.5,
        success: Some(true),
    });

    // Event 2: success=null
    let ctx_id_2 = format!("ctx{}", segment_id * 3 + 2);
    let email_2 = format!("{}@example.com", ctx_id_2);
    events.push(
        EventFactory::new()
            .with("event_type", "user_created")
            .with("context_id", ctx_id_2.clone())
            .with("email", email_2.clone())
            .with("timestamp", base_timestamp + 100)
            .with(
                "payload",
                json!({
                    "email": email_2.clone(),
                    "purchase_total": 42.25,
                    "success": null
                }),
            )
            .create(),
    );
    expected.push(ExpectedEvent {
        context_id: ctx_id_2,
        email: email_2,
        timestamp: base_timestamp + 100,
        purchase_total: 42.25,
        success: None, // null
    });

    // Event 3: success=true
    let ctx_id_3 = format!("ctx{}", segment_id * 3 + 3);
    let email_3 = format!("{}@example.com", ctx_id_3);
    events.push(
        EventFactory::new()
            .with("event_type", "user_created")
            .with("context_id", ctx_id_3.clone())
            .with("email", email_3.clone())
            .with("timestamp", base_timestamp + 200)
            .with(
                "payload",
                json!({
                    "email": email_3.clone(),
                    "purchase_total": 0.0,
                    "success": true
                }),
            )
            .create(),
    );
    expected.push(ExpectedEvent {
        context_id: ctx_id_3,
        email: email_3,
        timestamp: base_timestamp + 200,
        purchase_total: 0.0,
        success: Some(true),
    });

    (events, expected)
}

#[tokio::test]
async fn test_compactor_merges_segments_successfully() {
    // --- Setup test environment ---
    let base_dir = tempdir().unwrap();
    let input_dir = base_dir.path().join("input");
    let output_dir = base_dir.path().join("output");
    std::fs::create_dir_all(&input_dir).unwrap();
    std::fs::create_dir_all(&output_dir).unwrap();

    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    registry_factory
        .define_with_fields(
            "user_created",
            &[
                ("context_id", "string"),
                ("email", "string"),
                ("timestamp", "i64"),
                ("purchase_total", "f64"),
                ("success", "bool"),
            ],
        )
        .await
        .unwrap();

    let uid = registry.read().await.get_uid("user_created").unwrap();

    // --- Create and flush input segments ---
    const NUM_SEGMENTS: u32 = 2;
    const BASE_TIMESTAMP: u64 = 1_694_000_000;
    let mut all_expected_events = Vec::new();

    for segment_id in 0..NUM_SEGMENTS {
        let (events, expected) =
            create_segment_events(segment_id, BASE_TIMESTAMP + segment_id as u64);
        all_expected_events.extend(expected);

        let memtable = MemTableFactory::new().with_events(events).create().unwrap();
        let segment_dir = input_dir.join(format!("{:05}", segment_id));
        let flusher = Flusher::new(
            memtable,
            segment_id as u64,
            &segment_dir,
            Arc::clone(&registry),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        flusher.flush().await.expect("Flush failed");
    }

    const EXPECTED_TOTAL_EVENTS: usize = (NUM_SEGMENTS as usize) * 3;
    assert_eq!(
        all_expected_events.len(),
        EXPECTED_TOTAL_EVENTS,
        "Should have {} expected events",
        EXPECTED_TOTAL_EVENTS
    );

    // --- Run Compactor ---
    const L1_SEGMENT_ID: u64 = 10_000; // L1 level to exercise per-level zone sizing
    let compactor = Compactor::new(
        uid.clone(),
        vec!["00000".to_string(), "00001".to_string()],
        L1_SEGMENT_ID,
        input_dir.clone(),
        output_dir.clone(),
        Arc::clone(&registry),
    );

    compactor.run().await.expect("Compactor run failed");

    // --- Validate zone metadata ---
    let zones_path = output_dir.join(format!("{}.zones", uid));
    let zones = ZoneMeta::load(&zones_path).expect("Load zone meta failed");
    assert!(!zones.is_empty(), "Zone meta should not be empty");
    // With test config: event_per_zone=1 and level-aware sizing (target = base * (level+1)),
    // L1 compaction targets 2 rows per zone. Six merged events should produce three zones.
    const EXPECTED_ZONES: usize = 3;
    assert_eq!(
        zones.len(),
        EXPECTED_ZONES,
        "Expected {} zones at L1 target size (2 rows per zone)",
        EXPECTED_ZONES
    );

    // Verify zone metadata structure
    let mut total_rows = 0;
    for (idx, zone) in zones.iter().enumerate() {
        assert_eq!(zone.uid, uid, "Zone {} should have correct uid", idx);
        assert_eq!(
            zone.segment_id, L1_SEGMENT_ID,
            "Zone {} should have L1 segment_id",
            idx
        );
        assert_eq!(
            zone.zone_id, idx as u32,
            "Zone {} should have correct zone_id",
            idx
        );
        assert!(
            zone.start_row <= zone.end_row,
            "Zone {}: start_row ({}) should be <= end_row ({})",
            idx,
            zone.start_row,
            zone.end_row
        );
        assert!(
            zone.timestamp_min <= zone.timestamp_max,
            "Zone {}: timestamp_min ({}) should be <= timestamp_max ({})",
            idx,
            zone.timestamp_min,
            zone.timestamp_max
        );
        let zone_row_count = (zone.end_row - zone.start_row + 1) as usize;
        total_rows += zone_row_count;
    }
    assert_eq!(
        total_rows, EXPECTED_TOTAL_EVENTS,
        "Total rows across all zones should match input event count"
    );

    // --- Validate file existence ---
    let required_fields = [
        "context_id",
        "email",
        "timestamp",
        "purchase_total",
        "success",
    ];
    for field in &required_fields {
        let col_path = output_dir.join(format!("{}_{}.col", uid, field));
        assert!(
            col_path.exists(),
            ".col file should exist for field: {}",
            field
        );
    }

    // XOR filters should exist for indexed fields (context_id, email)
    for field in &["context_id", "email"] {
        let xf_path = output_dir.join(format!("{}_{}.xf", uid, field));
        assert!(
            xf_path.exists(),
            ".xf file should exist for field: {}",
            field
        );
    }

    // --- Validate data integrity ---
    let out_seg_label = format!("{:05}", L1_SEGMENT_ID);
    let mut all_context_ids = Vec::new();
    let mut all_timestamps = Vec::new();
    let mut all_purchase_totals = Vec::new();
    let mut all_success_values = Vec::new();

    for zone in &zones {
        // Load and verify context_id
        let ctx_ids = ColumnReader::load_for_zone(
            &output_dir,
            &out_seg_label,
            &uid,
            "context_id",
            zone.zone_id,
        )
        .expect("Failed to load context_id column");
        all_context_ids.extend(ctx_ids.clone());
        assert_eq!(
            ctx_ids.len(),
            (zone.end_row - zone.start_row + 1) as usize,
            "Zone {}: context_id count should match zone row count",
            zone.zone_id
        );

        // Load and verify timestamp (I64)
        let timestamp_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "timestamp",
            zone.zone_id,
            None,
        )
        .expect("Failed to load timestamp column");
        assert_eq!(
            timestamp_snapshot.physical_type(),
            PhysicalType::I64,
            "Zone {}: timestamp should have I64 physical type",
            zone.zone_id
        );
        let timestamps: Vec<i64> = timestamp_snapshot
            .into_scalar_values()
            .iter()
            .filter_map(|v| v.as_i64())
            .collect();
        all_timestamps.extend(timestamps);

        // Load and verify purchase_total (F64)
        let purchase_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "purchase_total",
            zone.zone_id,
            None,
        )
        .expect("Failed to load purchase_total column");
        assert_eq!(
            purchase_snapshot.physical_type(),
            PhysicalType::F64,
            "Zone {}: purchase_total should have F64 physical type",
            zone.zone_id
        );
        let purchases: Vec<f64> = purchase_snapshot
            .into_scalar_values()
            .iter()
            .filter_map(|v| v.as_f64())
            .collect();
        all_purchase_totals.extend(purchases);

        // Load and verify success (Bool with nulls)
        let success_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "success",
            zone.zone_id,
            None,
        )
        .expect("Failed to load success column");
        assert_eq!(
            success_snapshot.physical_type(),
            PhysicalType::Bool,
            "Zone {}: success should have Bool physical type",
            zone.zone_id
        );
        let success_strings = success_snapshot.to_strings();
        all_success_values.extend(success_strings);
    }

    // --- Verify data correctness ---
    // Check that all events are preserved
    assert_eq!(
        all_context_ids.len(),
        EXPECTED_TOTAL_EVENTS,
        "Total context_ids should match input event count"
    );

    // Verify global sort order (context_ids should be sorted)
    let mut sorted_context_ids = all_context_ids.clone();
    sorted_context_ids.sort();
    assert_eq!(
        all_context_ids, sorted_context_ids,
        "context_id values must be globally sorted across all zones"
    );

    // Verify null bool values map to empty strings
    let null_count = all_success_values.iter().filter(|v| v.is_empty()).count();
    let expected_null_count = all_expected_events
        .iter()
        .filter(|e| e.success.is_none())
        .count();
    assert_eq!(
        null_count, expected_null_count,
        "Should have {} null bool values (as empty strings). Found: {}, All values: {:?}",
        expected_null_count, null_count, all_success_values
    );

    // Verify bool values are correct (true -> "true", false -> "false", null -> "")
    for (idx, expected) in all_expected_events.iter().enumerate() {
        if idx < all_success_values.len() {
            let actual = &all_success_values[idx];
            match expected.success {
                Some(true) => assert_eq!(
                    actual, "true",
                    "Event {}: expected success=true, got '{}'",
                    idx, actual
                ),
                Some(false) => assert_eq!(
                    actual, "false",
                    "Event {}: expected success=false, got '{}'",
                    idx, actual
                ),
                None => assert_eq!(
                    actual, "",
                    "Event {}: expected success=null (empty string), got '{}'",
                    idx, actual
                ),
            }
        }
    }

    // Verify timestamps are within expected range
    let expected_min_ts = BASE_TIMESTAMP;
    let expected_max_ts = BASE_TIMESTAMP + (NUM_SEGMENTS as u64) + 200;
    if let Some(&min_ts) = all_timestamps.iter().min() {
        assert!(
            min_ts >= expected_min_ts as i64,
            "Minimum timestamp {} should be >= {}",
            min_ts,
            expected_min_ts
        );
    }
    if let Some(&max_ts) = all_timestamps.iter().max() {
        assert!(
            max_ts <= expected_max_ts as i64,
            "Maximum timestamp {} should be <= {}",
            max_ts,
            expected_max_ts
        );
    }

    // Verify purchase totals match expected values
    assert_eq!(
        all_purchase_totals.len(),
        EXPECTED_TOTAL_EVENTS,
        "All purchase_total values should be loaded"
    );
    for (idx, expected) in all_expected_events.iter().enumerate() {
        if idx < all_purchase_totals.len() {
            let actual = all_purchase_totals[idx];
            assert!(
                (actual - expected.purchase_total).abs() < f64::EPSILON,
                "Event {}: expected purchase_total={}, got {}",
                idx,
                expected.purchase_total,
                actual
            );
        }
    }
}
