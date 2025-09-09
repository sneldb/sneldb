use crate::engine::core::zone::enum_bitmap_index::{EnumBitmapBuilder, EnumBitmapIndex};
use crate::engine::schema::FieldType;
use crate::engine::schema::types::EnumType;
use crate::test_helpers::factories::{
    EnumBitmapIndexFactory, EventFactory, SchemaRegistryFactory, ZonePlanFactory,
};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn enum_bitmap_index_save_and_load_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("user_status.ebm");

    let index = EnumBitmapIndexFactory::new()
        .with_variants(&["pro", "basic", "trial"])
        .with_rows_per_zone(10)
        .with_zone_variant_bits(0, 0, &[0, 3, 9]) // pro at rows 0,3,9
        .with_zone_variant_bits(0, 1, &[1, 2, 5]) // basic
        .build();

    index.save(&path).unwrap();
    let loaded = EnumBitmapIndex::load(&path).unwrap();

    assert_eq!(loaded.variants, vec!["pro", "basic", "trial"]);
    assert_eq!(loaded.rows_per_zone, 10);
    assert!(loaded.has_any(0, 0));
    assert!(loaded.has_any(0, 1));
    assert!(!loaded.has_any(1, 0));
}

#[tokio::test]
async fn enum_bitmap_builder_builds_from_zone_values() {
    let events = vec![
        EventFactory::new()
            .with("payload", json!({"plan":"pro"}))
            .create(),
        EventFactory::new()
            .with("payload", json!({"plan":"basic"}))
            .create(),
        EventFactory::new()
            .with("payload", json!({"plan":"pro"}))
            .create(),
        EventFactory::new()
            .with("payload", json!({"plan":"trial"}))
            .create(),
    ];

    let plan = ZonePlanFactory::new()
        .with("id", 0)
        .with("start_index", 0)
        .with("end_index", events.len() - 1)
        .with("uid", "users")
        .with("event_type", "user_created")
        .with("segment_id", 1)
        .with("events", serde_json::to_value(events).unwrap())
        .create();

    let mut builder = EnumBitmapBuilder::new(
        "users",
        "plan",
        vec!["pro".into(), "basic".into(), "trial".into()],
        4,
    );
    let values: Vec<String> = plan
        .events
        .iter()
        .map(|e| e.get_field_value("plan"))
        .collect();
    builder.add_zone_values(plan.id, &values);
    let index = builder.build();

    assert_eq!(index.variants, vec!["pro", "basic", "trial"]);
    assert!(index.has_any(0, 0));
    assert!(index.has_any(0, 1));
    assert!(index.has_any(0, 2));
}

#[tokio::test]
async fn enum_bitmap_build_all_writes_files_for_enum_fields() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_field_types(
            "user_created",
            &[
                ("context_id", FieldType::String),
                (
                    "plan",
                    FieldType::Enum(EnumType {
                        variants: vec!["pro".to_string(), "basic".to_string(), "trial".to_string()],
                    }),
                ),
            ],
        )
        .await
        .unwrap();

    let events = vec![
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"pro"}))
            .create(),
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"basic"}))
            .create(),
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"trial"}))
            .create(),
    ];

    let zone = ZonePlanFactory::new()
        .with("id", 0)
        .with("start_index", 0)
        .with("end_index", events.len() - 1)
        .with("uid", "users")
        .with("event_type", "user_created")
        .with("segment_id", 1)
        .with("events", serde_json::to_value(events).unwrap())
        .create();

    let dir = tempdir().unwrap();
    EnumBitmapBuilder::build_all(&[zone], dir.path(), &registry)
        .await
        .expect("build_all should succeed");

    let ebm_path = dir.path().join("users_plan.ebm");
    assert!(ebm_path.exists(), "EBM file should exist");

    let loaded = EnumBitmapIndex::load(&ebm_path).unwrap();
    assert_eq!(loaded.variants, vec!["pro", "basic", "trial"]);
    assert!(loaded.has_any(0, 0));
}

#[tokio::test]
async fn enum_bitmap_build_all_handles_multiple_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_field_types(
            "user_created",
            &[
                ("context_id", FieldType::String),
                (
                    "plan",
                    FieldType::Enum(EnumType {
                        variants: vec![
                            "free".to_string(),
                            "pro".to_string(),
                            "premium".to_string(),
                            "enterprise".to_string(),
                        ],
                    }),
                ),
            ],
        )
        .await
        .unwrap();

    // Two zones with 2 rows each: zone 0 -> [free, pro], zone 1 -> [premium, enterprise]
    let events_zone0 = vec![
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"free"}))
            .create(),
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"pro"}))
            .create(),
    ];
    let events_zone1 = vec![
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"premium"}))
            .create(),
        EventFactory::new()
            .with("event_type", "user_created")
            .with("payload", json!({"plan":"enterprise"}))
            .create(),
    ];

    let zone0 = ZonePlanFactory::new()
        .with("id", 0)
        .with("start_index", 0)
        .with("end_index", events_zone0.len() - 1)
        .with("uid", "users")
        .with("event_type", "user_created")
        .with("segment_id", 1)
        .with("events", serde_json::to_value(events_zone0).unwrap())
        .create();

    let zone1 = ZonePlanFactory::new()
        .with("id", 1)
        .with("start_index", 0)
        .with("end_index", events_zone1.len() - 1)
        .with("uid", "users")
        .with("event_type", "user_created")
        .with("segment_id", 1)
        .with("events", serde_json::to_value(events_zone1).unwrap())
        .create();

    let dir = tempdir().unwrap();
    EnumBitmapBuilder::build_all(&[zone0, zone1], dir.path(), &registry)
        .await
        .expect("build_all should succeed");

    let ebm_path = dir.path().join("users_plan.ebm");
    assert!(ebm_path.exists(), "EBM file should exist");

    let loaded = EnumBitmapIndex::load(&ebm_path).unwrap();
    assert_eq!(
        loaded.variants,
        vec!["free", "pro", "premium", "enterprise"]
    );
    assert_eq!(loaded.rows_per_zone, 2);

    // Helper to decode row positions from bytes
    fn decode(bytes: &[u8], rows_per_zone: u16) -> Vec<usize> {
        let mut rows = Vec::new();
        let max = rows_per_zone as usize;
        for i in 0..max {
            let byte = i / 8;
            let bit = i % 8;
            if byte < bytes.len() && (bytes[byte] & (1u8 << bit)) != 0 {
                rows.push(i);
            }
        }
        rows
    }

    let z0 = loaded.zone_bitmaps.get(&0).expect("zone 0 missing");
    // variants order: [free, pro, premium, enterprise]
    assert_eq!(decode(&z0[0], loaded.rows_per_zone), vec![0]); // free at row 0
    assert_eq!(decode(&z0[1], loaded.rows_per_zone), vec![1]); // pro at row 1
    assert_eq!(decode(&z0[2], loaded.rows_per_zone), Vec::<usize>::new());
    assert_eq!(decode(&z0[3], loaded.rows_per_zone), Vec::<usize>::new());

    let z1 = loaded.zone_bitmaps.get(&1).expect("zone 1 missing");
    assert_eq!(decode(&z1[0], loaded.rows_per_zone), Vec::<usize>::new());
    assert_eq!(decode(&z1[1], loaded.rows_per_zone), Vec::<usize>::new());
    assert_eq!(decode(&z1[2], loaded.rows_per_zone), vec![0]); // premium at row 0
    assert_eq!(decode(&z1[3], loaded.rows_per_zone), vec![1]); // enterprise at row 1
}
