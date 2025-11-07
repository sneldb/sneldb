use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, duplex};

use crate::command::handlers::query::streaming::QueryResponseWriter;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;
use crate::shared::response::JsonRenderer;

fn build_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
        ])
        .expect("schema should build"),
    )
}

#[tokio::test]
async fn streaming_response_emits_schema_and_rows() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (sender, receiver) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let mut builder = BatchPool::new(4)
        .expect("pool")
        .acquire(Arc::clone(&schema));
    let row1 = vec![
        ScalarValue::from(json!("ctx-stream")),
        ScalarValue::from(json!(42u64)),
    ];
    builder.push_row(&row1).expect("push row should succeed");
    let batch = builder.finish().expect("batch finish");
    sender.send(Arc::new(batch)).await.expect("send batch");
    drop(sender);

    let stream = QueryBatchStream::new(Arc::clone(&schema), receiver, Vec::new());
    let (mut writer, mut reader) = duplex(4096);

    let renderer = JsonRenderer;
    QueryResponseWriter::new(&mut writer, &renderer, Arc::clone(&schema), Some(1), None)
        .write(stream)
        .await
        .expect("streaming write succeeds");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let output = String::from_utf8(buf).expect("utf8");
    let mut lines = output.lines();
    let schema_line = lines.next().expect("schema line");
    assert!(schema_line.contains("\"type\":\"schema\""));

    let row_line = lines.next().expect("row line");
    assert!(row_line.contains("ctx-stream"));

    let end_line = lines.next().expect("end line");
    assert!(end_line.contains("\"type\":\"end\""));
    assert!(lines.next().is_none());
}
