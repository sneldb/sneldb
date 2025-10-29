use crate::engine::core::Flusher;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::{ColumnReader, Compactor, SegmentIndex, ZoneMeta};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_compactor_merges_segments_successfully() {
    // --- Setup input segments ---
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

    // Flush 2 input segments
    for segment_id in 0..=1 {
        let events = vec![
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 1))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 1))
                .with("timestamp", 1_694_000_000 + segment_id as u64)
                .with(
                    "payload",
                    json!({
                        "email": format!("ctx{}@example.com", segment_id * 3 + 1),
                        "purchase_total": 10.5,
                        "success": true
                    }),
                )
                .create(),
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 2))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 2))
                .with("timestamp", 1_694_000_100 + segment_id as u64)
                .with(
                    "payload",
                    json!({
                        "email": format!("ctx{}@example.com", segment_id * 3 + 2),
                        "purchase_total": 42.25,
                        "success": null
                    }),
                )
                .create(),
            EventFactory::new()
                .with("event_type", "user_created")
                .with("context_id", format!("ctx{}", segment_id * 3 + 3))
                .with("email", format!("ctx{}@example.com", segment_id * 3 + 3))
                .with("timestamp", 1_694_000_200 + segment_id as u64)
                .with(
                    "payload",
                    json!({
                        "email": format!("ctx{}@example.com", segment_id * 3 + 3),
                        "purchase_total": 0.0,
                        "success": true
                    }),
                )
                .create(),
        ];

        let memtable = MemTableFactory::new().with_events(events).create().unwrap();

        let segment_dir = input_dir.join(format!("{:05}", segment_id));
        let flusher = Flusher::new(
            memtable,
            segment_id,
            &segment_dir,
            Arc::clone(&registry),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        flusher.flush().await.expect("Flush failed");
    }

    // --- Run Compactor ---
    // Use an L1 output id to exercise per-level zone sizing
    let compactor = Compactor::new(
        uid.clone(),
        vec!["00000".to_string(), "00001".to_string()],
        10_000, // L1 id
        input_dir.clone(),
        output_dir.clone(),
        Arc::clone(&registry),
    );

    compactor.run().await.expect("Compactor run failed");

    // --- Validate outputs ---
    let zones_path = output_dir.join(format!("{}.zones", uid));
    let zone_meta = ZoneMeta::load(&zones_path).expect("Load zone meta failed");
    assert!(!zone_meta.is_empty(), "Zone meta should not be empty");
    // With test config: fill_factor=3, event_per_zone=1, level=1 => target_rows=6
    // We wrote 6 merged events, so expect exactly 1 zone
    assert_eq!(
        zone_meta.len(),
        1,
        "Expected a single zone at L1 target size"
    );

    let col_path = output_dir.join(format!("{}_{}.col", uid, "context_id"));
    assert!(col_path.exists(), ".col file for uid should exist");

    let col_path = output_dir.join(format!("{}_{}.col", uid, "email"));
    assert!(col_path.exists(), ".col file for uid should exist");

    let xf_path = output_dir.join(format!("{}_{}.xf", uid, "context_id"));
    assert!(xf_path.exists(), ".xf file should exist");

    let xf_path = output_dir.join(format!("{}_{}.xf", uid, "email"));
    assert!(xf_path.exists(), ".xf file should exist");

    let index_path = output_dir.join(format!("{}.idx", uid));
    assert!(index_path.exists(), ".idx file should exist");

    let index = SegmentIndex::load(&output_dir)
        .await
        .expect("Load index failed");
    let matching = index.list_for_uid(&uid);
    assert!(
        !matching.is_empty(),
        "Segment index should contain entry for uid"
    );

    // Read back context_id values and verify global sort
    let mut all_ctx = Vec::new();
    let zones = ZoneMeta::load(&zones_path).unwrap();
    let out_seg_label = format!("{:05}", 10_000u32);

    for z in &zones {
        let ctx_ids =
            ColumnReader::load_for_zone(&output_dir, &out_seg_label, &uid, "context_id", z.zone_id)
                .unwrap();
        all_ctx.extend(ctx_ids);

        let timestamp_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "timestamp",
            z.zone_id,
            None,
        )
        .unwrap();
        assert_eq!(timestamp_snapshot.physical_type(), PhysicalType::I64);

        let purchase_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "purchase_total",
            z.zone_id,
            None,
        )
        .unwrap();
        assert_eq!(purchase_snapshot.physical_type(), PhysicalType::F64);

        let success_snapshot = ColumnReader::load_for_zone_snapshot(
            &output_dir,
            &out_seg_label,
            &uid,
            "success",
            z.zone_id,
            None,
        )
        .unwrap();
        assert_eq!(success_snapshot.physical_type(), PhysicalType::Bool);
        let success_strings = success_snapshot.to_strings();
        assert!(
            success_strings.iter().any(|v| v == ""),
            "null bool should map to empty string"
        );
    }

    let mut sorted = all_ctx.clone();
    sorted.sort();
    assert_eq!(all_ctx, sorted, "context_id values must be globally sorted");
}
