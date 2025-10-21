use crate::command::types::CompareOp;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::{Flusher, ZoneFinder};
use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterPlanFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn finds_event_type_zones_with_mock_index() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
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
    eprintln!("uid1: {}", uid1);
    eprintln!("uid2: {}", uid2);

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

    let flusher = Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![event3])
        .create()
        .expect("Failed to create memtable");

    let flusher = Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // when event_type is user_created
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("event_type")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!(event_type1))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneXorIndex {
        field: "event_type".to_string(),
    });

    let command = CommandFactory::query()
        .with_event_type(event_type1)
        .create();

    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].segment_id, "001");
    assert_eq!(found[1].segment_id, "002");

    // when id is 3 and event_type is user_updated
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid2)
        .with_value(json!(3))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneXorIndex {
        field: "id".to_string(),
    });

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
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!(3))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneXorIndex {
        field: "id".to_string(),
    });

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
    // With per-zone XOR (.zxf), equality pruning should narrow to zones in 002 only
    assert!(!found.is_empty());
    assert!(found.iter().all(|z| z.segment_id == "002"));

    // when range query is used
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid1)
        .with_value(json!(0))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });

    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert_eq!(found[0].zone_id, 0);
    assert_eq!(found[0].segment_id, "001");
    assert_eq!(found[1].zone_id, 0);
    assert_eq!(found[1].segment_id, "002");

    // when just context_id is used
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!("ctx3"))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneXorIndex {
        field: "context_id".to_string(),
    });

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
    assert_eq!(found[0].segment_id, "002");

    // when just context_id is used
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid1)
        .with_value(json!("ctx3"))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneXorIndex {
        field: "context_id".to_string(),
    });

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
    assert_eq!(found[0].segment_id, "002");
}

#[tokio::test]
async fn ebm_eq_prunes_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
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

    // 001: [free, pro]
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
    let flusher = Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // 002: [premium, enterprise]
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
    let flusher = Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Query: plan == "pro" -> only 001 zone 0
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::EnumBitmap {
        field: "plan".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].segment_id, "001");
    let expected_zone_id = (1usize / crate::shared::config::CONFIG.engine.event_per_zone) as u32;
    assert_eq!(found[0].zone_id, expected_zone_id);
}

#[tokio::test]
async fn ebm_neq_prunes_zones() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
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

    // 001: [free, pro]
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
    let flusher = Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // 002: [premium, enterprise]
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
    let flusher = Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // Query: plan != "pro" -> both segment zones included
    let mut filter_plan = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Neq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::EnumBitmap {
        field: "plan".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    // With dynamic zone size, the NEQ("pro") matches:
    // 001: index 0 ("free") => one zone id = 0 / zone_size
    // 002: indices 0 ("premium"), 1 ("enterprise") => zone ids = {0/zone_size, 1/zone_size}
    let zone_size = crate::shared::config::CONFIG.engine.event_per_zone;
    let seg1_zone_id = (0usize / zone_size) as u32;
    let mut seg2_zone_ids = vec![(0usize / zone_size) as u32, (1usize / zone_size) as u32];
    seg2_zone_ids.sort_unstable();
    seg2_zone_ids.dedup();

    let mut expected: Vec<(String, u32)> = vec![("001".to_string(), seg1_zone_id)];
    for zid in &seg2_zone_ids {
        expected.push(("002".to_string(), *zid));
    }

    // Compare sets ignoring order
    let mut actual: Vec<(String, u32)> = found
        .iter()
        .map(|z| (z.segment_id.clone(), z.zone_id))
        .collect();
    expected.sort();
    actual.sort();

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn zone_surf_prunes_segments_for_gt() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "range_evt";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // 001: ids < 10
    let e1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({ "id": 1 }))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({ "id": 9 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e1, e2])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // 002: ids > 10
    let e3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx3")
        .with("payload", json!({ "id": 15 }))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx4")
        .with("payload", json!({ "id": 20 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e3, e4])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert!(found.iter().all(|z| z.segment_id == "002"));
    assert!(!found.is_empty());
}

#[tokio::test]
async fn zone_surf_prunes_segments_for_lt() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "range_evt2";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // 001: ids < 10
    let a1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({ "id": 1 }))
        .create();
    let a2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({ "id": 9 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a1, a2])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    // 002: ids > 10
    let b1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx3")
        .with("payload", json!({ "id": 15 }))
        .create();
    let b2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx4")
        .with("payload", json!({ "id": 20 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![b1, b2])
        .create()
        .expect("Failed to create memtable");
    let flusher = Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Lt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert!(found.iter().all(|z| z.segment_id == "001"));
    assert!(!found.is_empty());
}

#[tokio::test]
async fn zone_surf_gte_includes_boundary() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "range_evt_gte";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // 001: boundary value 10
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctxA")
        .with("payload", json!({ "id": 10 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![a])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // 002: value 11
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctxB")
        .with("payload", json!({ "id": 11 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![b])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gte)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert!(found.iter().any(|z| z.segment_id == "001"));
    assert!(found.iter().any(|z| z.segment_id == "002"));
}

#[tokio::test]
async fn zone_surf_lte_includes_boundary() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let segment1_id = 1;
    let segment1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&segment1_dir).unwrap();

    let segment2_id = 2;
    let segment2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&segment2_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "range_evt_lte";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // 001: value 19
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctxA")
        .with("payload", json!({ "id": 19 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![a])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        segment1_id,
        &segment1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // 002: boundary value 20
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctxB")
        .with("payload", json!({ "id": 20 }))
        .create();
    let memtable = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![b])
        .create()
        .unwrap();
    Flusher::new(
        memtable,
        segment2_id,
        &segment2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let mut filter_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Lte)
        .with_uid(&uid)
        .with_value(json!(20))
        .create();
    filter_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });

    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;

    let binding = vec!["001".to_string(), "002".to_string()];
    let finder = ZoneFinder::new(&filter_plan, &query_plan, &binding, &shard_dir);
    let found = finder.find();

    assert_eq!(found.len(), 2);
    assert!(found.iter().any(|z| z.segment_id == "001"));
    assert!(found.iter().any(|z| z.segment_id == "002"));
}

#[tokio::test]
async fn zone_surf_between_three_segments_and() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");

    let s1_id = 1;
    let s1_dir = shard_dir.join("001");
    std::fs::create_dir_all(&s1_dir).unwrap();

    let s2_id = 2;
    let s2_dir = shard_dir.join("002");
    std::fs::create_dir_all(&s2_dir).unwrap();

    let s3_id = 3;
    let s3_dir = shard_dir.join("003");
    std::fs::create_dir_all(&s3_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "range_evt3";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    // 001: [5, 12]
    let e1 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .with("payload", json!({"id":5}))
        .create();
    let e2 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx2")
        .with("payload", json!({"id":12}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e1, e2])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        s1_id,
        &s1_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // 002: [25, 31]
    let e3 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx3")
        .with("payload", json!({"id":25}))
        .create();
    let e4 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx4")
        .with("payload", json!({"id":31}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e3, e4])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        s2_id,
        &s2_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // 003: [15, 29]
    let e5 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx5")
        .with("payload", json!({"id":15}))
        .create();
    let e6 = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx6")
        .with("payload", json!({"id":29}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![e5, e6])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        s3_id,
        &s3_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Build query plan
    let command = CommandFactory::query().with_event_type(event_type).create();
    let query_plan = QueryPlanFactory::new()
        .with_registry(registry.clone())
        .with_command(command)
        .build()
        .await;
    let binding = vec!["001".to_string(), "002".to_string(), "003".to_string()];

    // id > 10
    let mut gt_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Gt)
        .with_uid(&uid)
        .with_value(json!(10))
        .create();
    gt_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });
    let gt_finder = ZoneFinder::new(&gt_plan, &query_plan, &binding, &shard_dir);
    let zones_gt = gt_finder.find();

    // id < 30
    let mut lt_plan = FilterPlanFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Lt)
        .with_uid(&uid)
        .with_value(json!(30))
        .create();
    lt_plan.index_strategy = Some(IndexStrategy::ZoneSuRF {
        field: "id".to_string(),
    });
    let lt_finder = ZoneFinder::new(&lt_plan, &query_plan, &binding, &shard_dir);
    let zones_lt = lt_finder.find();

    // AND combine
    let combined = crate::engine::core::ZoneCombiner::new(
        vec![zones_gt, zones_lt],
        crate::engine::core::LogicalOp::And,
    )
    .combine();

    // Expect segments: 001 (12), 003 (15,29), 002 (25); exclude 5 and 31 via intersection logic
    let segs: std::collections::HashSet<_> =
        combined.iter().map(|z| z.segment_id.as_str()).collect();
    assert!(segs.contains("001"));
    assert!(segs.contains("002"));
    assert!(segs.contains("003"));
    assert!(!combined.is_empty());
}
