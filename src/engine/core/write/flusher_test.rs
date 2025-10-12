use crate::engine::core::{Flusher, SegmentIndex, ZoneMeta};
use crate::test_helpers::factories::{EventFactory, MemTableFactory, SchemaRegistryFactory};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_flusher_flushes_memtable_to_segment_dir() {
    let tmp_dir = tempdir().expect("Temp dir failed");
    let shard_dir = tmp_dir.path().join("shard-0");
    let segment_dir = shard_dir.join("segment-007");
    std::fs::create_dir_all(&segment_dir).unwrap();

    let segment_id = 7;

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let event_type = "user_created";

    schema_factory
        .define_with_fields(event_type, &[("context_id", "string"), ("email", "string")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found");

    let events = EventFactory::new()
        .with("event_type", event_type)
        .with("context_id", "ctx1")
        .create_list(5);

    let memtable = MemTableFactory::new()
        .with_capacity(10)
        .with_events(events)
        .create()
        .expect("Failed to create memtable");

    let flusher = Flusher::new(
        memtable,
        segment_id,
        &segment_dir,
        registry.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );
    flusher.flush().await.expect("Flush failed");

    let zones_path = segment_dir.join(format!("{}.zones", uid));
    let zone_meta = ZoneMeta::load(&zones_path).expect("Failed to load zones");
    assert!(!zone_meta.is_empty(), "ZoneMeta should not be empty");

    let segment_index = SegmentIndex::load(&shard_dir)
        .await
        .expect("Failed to load segment index");
    assert_eq!(segment_index.entries.len(), 1, "Expected one segment entry");

    let entry = &segment_index.entries[0];
    assert_eq!(entry.label, format!("{:05}", segment_id));
    assert_eq!(entry.counter, segment_id as u32);
    assert_eq!(entry.uids, vec![uid]);
}
