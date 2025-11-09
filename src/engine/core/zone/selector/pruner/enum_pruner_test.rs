use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::schema::{EnumType, FieldType};
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, FilterGroupFactory, MemTableFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

#[tokio::test]
async fn enum_pruner_handles_unknown_variant_and_empty_index() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "enum_unknown";
    reg_fac
        .define_with_field_types(
            event_type,
            &[
                ("context_id", FieldType::String),
                (
                    "plan",
                    FieldType::Enum(EnumType {
                        variants: vec!["free".to_string(), "pro".to_string()],
                    }),
                ),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // write one event with variant "free"
    let ev = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"plan": "free"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![ev])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Unknown variant should return None -> FieldSelector drops to xor/range (none) -> empty
    let mut f_unknown = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("enterprise"))
        .create();
    if let Some(strategy) = f_unknown.index_strategy_mut() {
        *strategy = Some(IndexStrategy::EnumBitmap {
            field: "plan".to_string(),
        });
    }
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &f_unknown,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("001");
    assert!(zones.is_empty());

    // Neq with unknown currently returns None (unknown variant short-circuits) -> empty
    let mut f_neq_unknown = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Neq)
        .with_uid(&uid)
        .with_value(json!("enterprise"))
        .create();
    if let Some(strategy) = f_neq_unknown.index_strategy_mut() {
        *strategy = Some(IndexStrategy::EnumBitmap {
            field: "plan".to_string(),
        });
    }
    let ctx2 = SelectionContext {
        plan: &f_neq_unknown,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel2 = ZoneSelectorBuilder::new(ctx2).build();
    let zones2 = sel2.select_for_segment("001");
    assert!(zones2.is_empty());
}

#[tokio::test]
async fn enum_pruner_multiple_variants_same_zone_dedupes() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "enum_dedupe";
    reg_fac
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
                        ],
                    }),
                ),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    // Same zone contains two variants: free and pro
    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c1")
        .with("payload", json!({"plan": "free"}))
        .create();
    let b = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "c2")
        .with("payload", json!({"plan": "pro"}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(2)
        .with_events(vec![a, b])
        .create()
        .unwrap();
    crate::engine::core::Flusher::new(
        mem,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    // Eq pro -> exactly one zone should be returned
    let mut f_eq = FilterGroupFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_uid(&uid)
        .with_value(json!("pro"))
        .create();
    if let Some(strategy) = f_eq.index_strategy_mut() {
        *strategy = Some(IndexStrategy::EnumBitmap {
            field: "plan".to_string(),
        });
    }
    let cmd = CommandFactory::query().with_event_type(event_type).create();
    let q = QueryPlanFactory::new()
        .with_registry(Arc::clone(&registry))
        .with_command(cmd)
        .build()
        .await;
    let ctx = SelectionContext {
        plan: &f_eq,
        query_plan: &q,
        base_dir: &shard_dir,
        caches: None,
    };
    let sel = ZoneSelectorBuilder::new(ctx).build();
    let zones = sel.select_for_segment("001");
    assert_eq!(zones.len(), 1);
}
