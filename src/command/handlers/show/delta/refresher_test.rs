use std::sync::Arc;

use super::refresher::DeltaRefresher;
use super::schema::SchemaBuilder;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::command::types::MaterializedQuerySpec;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::{HighWaterMark, MaterializationEntry};
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::command_factory::CommandFactory;
use serde_json::json;

fn make_entry(root: &std::path::Path) -> MaterializationEntry {
    let command = CommandFactory::query().with_event_type("orders").create();

    let spec = MaterializedQuerySpec {
        name: "orders_view".to_string(),
        query: Box::new(command),
    };

    let mut entry = MaterializationEntry::new(spec, root).expect("entry");
    entry.schema = vec![
        SchemaSnapshot::new("timestamp", "Number"),
        SchemaSnapshot::new("event_id", "Number"),
    ];
    entry.high_water_mark = Some(HighWaterMark::new(0, 0));
    entry
}

#[allow(dead_code)]
fn build_schema() -> Arc<BatchSchema> {
    Arc::new(
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
    )
}

fn build_batch(schema: Arc<BatchSchema>, rows: &[(u64, u64)]) -> Arc<ColumnBatch> {
    let timestamps: Vec<ScalarValue> = rows
        .iter()
        .map(|(ts, _)| ScalarValue::from(json!(ts)))
        .collect();
    let event_ids: Vec<ScalarValue> = rows
        .iter()
        .map(|(_, id)| ScalarValue::from(json!(id)))
        .collect();
    Arc::new(
        ColumnBatch::new(schema, vec![timestamps, event_ids], rows.len(), None).expect("batch"),
    )
}

#[tokio::test]
async fn spawn_stream_task_appends_and_forwards_batches() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let entry = make_entry(temp_dir.path());
    let schema = SchemaBuilder::build(&entry).expect("schema");

    let refresher = DeltaRefresher::new(&entry, Some(0), Some(1)).expect("refresher");
    assert!(refresher.has_watermark_filtering());
    assert_eq!(refresher.initial_high_water(), HighWaterMark::default());

    let metrics_stream = FlowMetrics::new();
    let (delta_sender, delta_receiver) = FlowChannel::bounded(4, Arc::clone(&metrics_stream));
    let stream = QueryBatchStream::new(Arc::clone(&schema), delta_receiver, Vec::new());

    let metrics_forward = FlowMetrics::new();
    let (forward_sender, mut forward_receiver) = FlowChannel::bounded(4, metrics_forward);

    let handle = refresher.spawn_stream_task(stream, forward_sender, "orders_view");

    let batch = build_batch(Arc::clone(&schema), &[(1, 1), (2, 2)]);
    delta_sender
        .send(Arc::clone(&batch))
        .await
        .expect("send batch");
    drop(delta_sender);

    let forwarded = forward_receiver.recv().await.expect("forwarded batch");
    assert_eq!(forwarded.len(), 2);

    handle.await.expect("task");

    let sink = refresher.take_sink().expect("sink");
    assert_eq!(sink.total_rows(), 2);
    assert_eq!(sink.last_rows_appended(), 2);
}
