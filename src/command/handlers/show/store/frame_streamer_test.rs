use std::sync::Arc;

use super::frame_streamer::StoredFrameStreamer;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::MaterializedStore;
use crate::engine::materialize::catalog::SchemaSnapshot;
use serde_json::json;

fn build_schema_snapshots() -> Vec<SchemaSnapshot> {
    vec![
        SchemaSnapshot::new("timestamp", "Number"),
        SchemaSnapshot::new("event_id", "Number"),
    ]
}

fn build_column_batch(len: usize) -> ColumnBatch {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
        ])
        .expect("schema"),
    );

    let mut timestamps = Vec::with_capacity(len);
    let mut event_ids = Vec::with_capacity(len);
    for idx in 0..len {
        timestamps.push(json!(idx as u64));
        event_ids.push(json!(idx as u64));
    }

    ColumnBatch::new(schema, vec![timestamps, event_ids], len, None).expect("batch")
}

fn populate_store(path: &std::path::Path) {
    let mut store = MaterializedStore::open(path).expect("store");
    let batch = build_column_batch(1);
    store
        .append_batch(&build_schema_snapshots(), &batch)
        .expect("append");
}

#[test]
fn loads_existing_frames() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    populate_store(temp_dir.path());

    let streamer = StoredFrameStreamer::new(temp_dir.path().into()).expect("streamer");
    assert_eq!(streamer.frame_count(), 1);
}

#[tokio::test]
async fn streams_batches_from_store() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    populate_store(temp_dir.path());

    let streamer = StoredFrameStreamer::new(temp_dir.path().into()).expect("streamer");
    let metrics = FlowMetrics::new();
    let (sender, mut receiver) = FlowChannel::bounded(4, metrics);

    let handle = streamer.spawn_stream_task(sender).expect("spawned task");

    let batch = receiver.recv().await.expect("batch");
    assert_eq!(batch.len(), 1);

    handle.await.expect("task completed");
}
