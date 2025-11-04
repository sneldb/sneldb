use std::sync::Arc;

use super::response_writer::ShowResponseWriter;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::shared::response::JsonRenderer;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, duplex};

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
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "String".to_string(),
            },
        ])
        .expect("schema"),
    )
}

fn build_batch(schema: Arc<BatchSchema>, rows: &[(u64, u64, &str)]) -> Arc<ColumnBatch> {
    let timestamps: Vec<Value> = rows.iter().map(|(ts, _, _)| json!(ts)).collect();
    let event_ids: Vec<Value> = rows.iter().map(|(_, id, _)| json!(id)).collect();
    let values: Vec<Value> = rows.iter().map(|(_, _, v)| json!(v)).collect();

    Arc::new(
        ColumnBatch::new(
            schema,
            vec![timestamps, event_ids, values],
            rows.len(),
            None,
        )
        .expect("batch"),
    )
}

#[tokio::test]
async fn writes_schema_and_deduplicated_rows() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    let stored_batch = build_batch(Arc::clone(&schema), &[(1, 1, "stored")]);
    let delta_batch = build_batch(schema.clone(), &[(2, 1, "dup"), (3, 2, "new")]);

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&stored_batch))
            .await
            .expect("send stored");
        stream_sender
            .send(Arc::clone(&delta_batch))
            .await
            .expect("send delta");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = JsonRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        1,
        false,
        None,
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");
    let content = String::from_utf8(buf).expect("utf8");
    let frames: Vec<&str> = content.lines().filter(|line| !line.is_empty()).collect();

    assert!(frames[0].contains("\"type\":\"schema\""));

    let stored_frame: Value = serde_json::from_str(frames[1]).expect("stored frame");
    let stored_rows = stored_frame["rows"].as_array().expect("rows");
    assert_eq!(stored_rows.len(), 1);
    assert_eq!(stored_rows[0], json!([1, 1, "stored"]));

    let delta_frame: Value = serde_json::from_str(frames[2]).expect("delta frame");
    let delta_rows = delta_frame["rows"].as_array().expect("rows");
    assert_eq!(delta_rows.len(), 1);
    assert_eq!(delta_rows[0], json!([3, 2, "new"]));

    let end_frame: Value = serde_json::from_str(frames[3]).expect("end frame");
    assert_eq!(end_frame["row_count"], json!(2));
}
