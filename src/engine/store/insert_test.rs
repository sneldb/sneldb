use crate::engine::query::scan::scan;
use crate::engine::store::insert::insert_and_maybe_flush;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, SchemaRegistryFactory, ShardContextFactory,
};
use serde_json::json;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::{Instant, timeout};

#[tokio::test]
async fn test_insert_and_maybe_flush_e2e() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let tmp_dir = tempdir().unwrap();
    let shard_dir = tmp_dir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();

    let event_type = "test_insert_and_maybe_flush_event";
    schema_factory
        .define_with_fields(event_type, &[("key", "string"), ("value", "int")])
        .await
        .unwrap();

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found for 'test_event'");

    let mut ctx = ShardContextFactory::new()
        .with_capacity(2) // small capacity to trigger flush quickly
        .with_base_dir(shard_dir)
        .create();

    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx1")
            .with("payload", json!({"key": "a", "value": 1}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx2")
            .with("payload", json!({"key": "b", "value": 2}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx1")
            .with("payload", json!({"key": "c", "value": 3}))
            .create(),
    ];

    for event in events.into_iter() {
        insert_and_maybe_flush(event, &mut ctx, &registry)
            .await
            .expect("Insert failed");
    }

    // Wait for flush to publish segment ids (flush happens asynchronously)
    let wait_deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if ctx.segment_ids.read().unwrap().len() == 1 {
            break;
        }
        if Instant::now() >= wait_deadline {
            panic!("Timed out waiting for segment_ids to be populated");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify one flush happened (after 2 inserts, due to capacity = 2)
    let flushed_segments = ctx.segment_ids.read().unwrap();
    assert_eq!(flushed_segments.len(), 1);

    // Wait for flush and verification to complete
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify segment directory created
    let segment_path = ctx.base_dir.join("00000");
    assert!(segment_path.exists());

    // Verify zones file exists and is valid
    let zones_path = segment_path.join(format!("{}.zones", uid));
    assert!(
        zones_path.exists(),
        ".zones file should exist at {:?}",
        zones_path
    );

    // Verify .col file exists and is valid
    let col_path = segment_path.join(format!("{}_{}.col", uid, "event_type"));
    assert!(
        col_path.exists(),
        ".col file should exist at {:?}",
        col_path
    );

    let col_path = segment_path.join(format!("{}_{}.col", uid, "timestamp"));
    assert!(
        col_path.exists(),
        ".col file should exist at {:?}",
        col_path
    );

    let col_path = segment_path.join(format!("{}_{}.col", uid, "context_id"));
    assert!(
        col_path.exists(),
        ".col file should exist at {:?}",
        col_path
    );

    let col_path = segment_path.join(format!("{}_{}.col", uid, "key"));
    assert!(
        col_path.exists(),
        ".col file should exist at {:?}",
        col_path
    );

    let col_path = segment_path.join(format!("{}_{}.col", uid, "value"));
    assert!(
        col_path.exists(),
        ".col file should exist at {:?}",
        col_path
    );

    // verify offset file exists (.zfc only)
    for field in ["event_type", "timestamp", "context_id", "key", "value"] {
        let zfc = segment_path.join(format!("{}_{}.zfc", uid, field));
        assert!(zfc.exists(), ".zfc should exist for {}", field);
    }

    // verify .xf file exists and is valid
    let xf_path = segment_path.join(format!("{}_{}.xf", uid, "event_type"));
    assert!(xf_path.exists(), ".xf file should exist at {:?}", xf_path);

    let xf_path = segment_path.join(format!("{}_{}.xf", uid, "context_id"));
    assert!(xf_path.exists(), ".xf file should exist at {:?}", xf_path);

    let xf_path = segment_path.join(format!("{}_{}.xf", uid, "key"));
    assert!(xf_path.exists(), ".xf file should exist at {:?}", xf_path);

    let xf_path = segment_path.join(format!("{}_{}.xf", uid, "value"));
    assert!(xf_path.exists(), ".xf file should exist at {:?}", xf_path);

    // verify .idx file is valid
    let idx_path = segment_path.join(format!("{}.idx", uid));
    assert!(
        idx_path.exists(),
        ".idx file should exist at {:?}",
        idx_path
    );

    // verify .segment file is valid
    let segment_path = ctx.base_dir.join(format!("segments.idx"));
    assert!(
        segment_path.exists(),
        ".segment file should exist at {:?}",
        segment_path
    );

    // verify scan returns all events with ctx1 (2 events, since we changed one to ctx2 for filter creation)
    let cmd = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .create();
    let handle = scan(
        &cmd,
        None,
        &registry,
        &ctx.base_dir,
        &ctx.segment_ids,
        &ctx.memtable,
        &ctx.passive_buffers,
    )
    .await
    .expect("scan should succeed");

    // Count rows from streaming response
    let mut receiver = handle.receiver;
    let mut row_count = 0;
    while let Some(batch) = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .ok()
        .flatten()
    {
        row_count += batch.len();
    }
    assert_eq!(row_count, 2, "Should find 2 events for ctx1");
}
