use crate::command::types::CompareOp;
use crate::engine::core::{Flusher, ZoneFinder};
use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterPlanFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn finds_event_type_zones_with_mock_index() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("segment-001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type1 = "user_created";
    let event_type2 = "user_updated";

    schema_factory
        .define_with_fields(event_type1, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    schema_factory
        .define_with_fields(event_type2, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

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

    let event1 = EventFactory::new()
        .with("event_type", event_type1)
        .with("context_id", "ctx1")
        .with("payload", json!({ "id": 1 }))
        .create();

    let event2 = EventFactory::new()
        .with("event_type", event_type2)
        .with("context_id", "ctx2")
        .with("payload", json!({ "id": 2 }))
        .create();

    let event3 = EventFactory::new()
        .with("event_type", event_type1)
        .with("context_id", "ctx3")
        .with("payload", json!({ "id": 3 }))
        .create();

    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event1, event2])
        .create()
        .expect("Failed to create memtable");

    let flusher = Flusher::new(memtable, segment1_id, &segment1_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event3])
        .create()
        .expect("Failed to create memtable");

    let flusher = Flusher::new(memtable, segment2_id, &segment2_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // when event_type is user_created
    let filter_plan = FilterPlanFactory::new()
        .with_column("event_type")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!(event_type1))
        .create();

    let command = CommandFactory::query()
        .with_event_type(event_type1)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["segment-001".to_string(), "segment-002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].segment_id, "segment-001");
    assert_eq!(found[1].segment_id, "segment-002");

    // when id is 3 and event_type is user_updated
    let filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid2)
        .with_value(json!(3))
        .create();

    let command = CommandFactory::query()
        .with_event_type(event_type2)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 0);

    // when id is 3 and event_type is user_created
    let filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!(3))
        .create();

    let command = CommandFactory::query()
        .with_event_type(event_type1)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();
    // With per-zone XOR (.zxf), equality pruning should narrow to zones in segment-002 only
    assert!(!found.is_empty());
    assert!(found.iter().all(|z| z.segment_id == "segment-002"));

    // when range query is used
    let filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid1)
        .with_value(json!(0))
        .create();

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    // Range query currently returns all zones per segment
    use crate::shared::config::CONFIG;
    let per_segment = CONFIG.engine.fill_factor();
    assert_eq!(found.len(), per_segment * 2);
    // Ensure each segment contributes [0..per_segment) zone ids
    for seg in ["segment-001", "segment-002"] {
        for zid in 0..per_segment {
            assert!(
                found
                    .iter()
                    .any(|z| z.segment_id == seg && z.zone_id == zid as u32),
                "missing zone {} in {}",
                zid,
                seg
            );
        }
    }

    // when just context_id is used
    let filter_plan = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!("ctx3"))
        .create();

    let command = CommandFactory::query()
        .with_context_id("ctx3")
        .with_event_type(event_type1)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].segment_id, "segment-002");

    // when just context_id is used
    let filter_plan = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!("ctx3"))
        .create();

    let command = CommandFactory::query()
        .with_context_id("ctx3")
        .with_event_type(event_type1)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].segment_id, "segment-002");
}

#[tokio::test]
async fn ebm_eq_prunes_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("segment-001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "subscription";

    schema_factory
        .define_with_field_types(
            event_type,
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

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // segment-001: [free, pro]
    let e1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({ "plan": "free" }))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({ "plan": "pro" }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e1, e2])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(memtable, segment1_id, &segment1_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // segment-002: [premium, enterprise]
    let e3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx3")
        .with("payload", json!({ "plan": "premium" }))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx4")
        .with("payload", json!({ "plan": "enterprise" }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e3, e4])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(memtable, segment2_id, &segment2_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // Query: plan == "pro" -> only segment-001 zone 0
    let filter_plan = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["segment-001".to_string(), "segment-002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].segment_id, "segment-001");
    assert_eq!(found[0].zone_id, 0);
}

#[tokio::test]
async fn ebm_neq_prunes_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("segment-001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("segment-002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "subscription";

    schema_factory
        .define_with_field_types(
            event_type,
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

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // segment-001: [free, pro]
    let e1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({ "plan": "free" }))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({ "plan": "pro" }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e1, e2])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(memtable, segment1_id, &segment1_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // segment-002: [premium, enterprise]
    let e3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx3")
        .with("payload", json!({ "plan": "premium" }))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx4")
        .with("payload", json!({ "plan": "enterprise" }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e3, e4])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(memtable, segment2_id, &segment2_dir, registry.clone());
    flusher.flush().await.expect("Flush failed");

    // Query: plan != "pro" -> both segment zones included
    let filter_plan = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Neq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["segment-001".to_string(), "segment-002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].zone_id, 0);
    assert_eq!(found[1].zone_id, 0);
}
