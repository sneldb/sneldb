use std::collections::HashSet;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::engine::core::MemTable;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::query::streaming::scan::StreamingScan;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, SchemaRegistryFactory,
};

fn build_memtable(event_type: &str) -> MemTable {
    let events = vec![
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx-1")
            .with("payload", json!({"value": 10}))
            .create(),
        EventFactory::new()
            .with("event_type", event_type)
            .with("context_id", "ctx-2")
            .with("payload", json!({"value": 11}))
            .create(),
    ];

    MemTableFactory::new()
        .with_events(events)
        .create()
        .expect("memtable")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_scan_emits_memtable_rows() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("payload", "object"),
                ("value", "int"),
            ],
        )
        .await
        .expect("schema defined");

    let registry = registry_factory.registry();
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();

    let memtable = build_memtable("stream_event");
    let passive_buffers = Arc::new(PassiveBufferSet::new(4));

    let tmp_dir = TempDir::new().expect("temp dir");
    let base_dir = tmp_dir.path().join("segments");
    std::fs::create_dir_all(&base_dir).expect("segment dir");

    let segment_ids = Arc::new(StdRwLock::new(vec!["seg-1".to_string()]));

    let scan = StreamingScan::new(
        &command,
        None,
        &registry,
        &base_dir,
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await
    .expect("streaming scan init");

    let handle = scan.execute().await.expect("streaming scan run");

    let context_idx = handle
        .schema
        .columns()
        .iter()
        .position(|col| col.name == "context_id")
        .expect("context column");

    let mut receiver = handle.receiver;
    let mut contexts = HashSet::new();
    while let Some(batch) = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout")
    {
        let column = batch.column(context_idx).expect("context data");
        for value in column {
            if let Some(ctx) = value.as_str() {
                contexts.insert(ctx.to_string());
            }
        }
    }

    assert!(contexts.contains("ctx-1"));
    assert!(contexts.contains("ctx-2"));
}

#[tokio::test]
async fn streaming_scan_supports_aggregate_queries() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("value", "int"),
            ],
        )
        .await
        .expect("schema defined");

    let registry = registry_factory.registry();
    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .add_count()
        .create();

    let memtable = MemTableFactory::new().create().expect("memtable");
    let passive_buffers = Arc::new(PassiveBufferSet::new(2));

    let tmp_dir = TempDir::new().expect("temp dir");
    let base_dir = tmp_dir.path().join("segments");
    std::fs::create_dir_all(&base_dir).expect("segment dir");

    let segment_ids = Arc::new(StdRwLock::new(vec!["seg-agg".to_string()]));

    // Aggregate queries now support streaming via AggregateOp
    let result = StreamingScan::new(
        &command,
        None,
        &registry,
        &base_dir,
        &segment_ids,
        &memtable,
        &passive_buffers,
    )
    .await;

    assert!(
        result.is_ok(),
        "aggregation queries should be supported in streaming mode"
    );
}
