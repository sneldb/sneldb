use std::sync::Arc;

use serde_json::json;
use tempfile::tempdir;

use crate::command::types::CompareOp;
use crate::engine::core::Flusher;
use crate::engine::core::QueryCaches;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::schema::FieldType;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};

#[tokio::test]
async fn skips_xor_for_payload_temporal_field() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp = tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    let seg1 = shard_dir.join("001");
    std::fs::create_dir_all(&seg1).unwrap();

    let reg_fac = SchemaRegistryFactory::new();
    let registry = reg_fac.registry();
    let event_type = "xor_temporal";
    reg_fac
        .define_with_field_types(
            event_type,
            &[
                ("context_id", FieldType::String),
                ("ts", FieldType::Timestamp),
            ],
        )
        .await
        .unwrap();
    let uid = registry.read().await.get_uid(event_type).unwrap();

    let a = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "a")
        .with("payload", json!({"ts": 123u64}))
        .create();
    let mem = MemTableFactory::new()
        .with_capacity(1)
        .with_events(vec![a])
        .create()
        .unwrap();
    Flusher::new(
        mem,
        1,
        &seg1,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .flush()
    .await
    .unwrap();

    let caches = QueryCaches::new(shard_dir.clone());
    // ensure calendar exists
    let cal = seg1.join(format!("{}_{}.cal", uid, "ts"));
    assert!(cal.exists());

    let artifacts = ZoneArtifacts::new(&shard_dir, Some(&caches));
    let pruner = XorPruner { artifacts };
    let val = ScalarValue::from(json!(123u64));
    let args = super::PruneArgs {
        segment_id: "001",
        uid: &uid,
        column: "ts",
        value: Some(&val),
        op: Some(&CompareOp::Eq),
    };
    let out = pruner
        .apply_zone_index_only(&args)
        .or_else(|| pruner.apply_presence_only(&args));
    assert!(out.is_none(), "XorPruner must skip for temporal fields");
}
