use std::sync::Arc;

use arrow_array::{Array, Int64Array, LargeStringArray, UInt64Array};
use arrow_ipc::reader::StreamReader;
use std::io::Cursor;

use super::response_writer::ShowResponseWriter;
use crate::command::handlers::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::shared::response::{ArrowRenderer, JsonRenderer};
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

#[tokio::test]
async fn writes_arrow_format_with_deduplication() {
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
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        1,
        false, // has_watermark_filtering = false means deduplication is enabled
        None,
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    // Parse Arrow stream
    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    // First record batch should be from stored_batch (1 row)
    let record1 = reader.next().expect("first batch").expect("batch");
    assert_eq!(record1.num_rows(), 1);
    assert_eq!(record1.num_columns(), 3);

    let event_id_array = record1
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("event_id should be Int64Array");
    assert_eq!(event_id_array.value(0), 1);

    let value_array = record1
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("value should be LargeStringArray");
    assert_eq!(value_array.value(0), "stored");

    // Second record batch should be from delta_batch but only 1 row (the new one, dup is filtered)
    let record2 = reader.next().expect("second batch").expect("batch");
    assert_eq!(record2.num_rows(), 1);

    let event_id_array2 = record2
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("event_id should be Int64Array");
    assert_eq!(event_id_array2.value(0), 2); // Only the new event_id, dup was filtered

    let value_array2 = record2
        .column(2)
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("value should be LargeStringArray");
    assert_eq!(value_array2.value(0), "new");

    assert!(reader.next().is_none(), "should be end of stream");
}

#[tokio::test]
async fn writes_arrow_format_with_limit() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    let batch1 = build_batch(Arc::clone(&schema), &[(1, 1, "a"), (2, 2, "b")]);
    let batch2 = build_batch(schema.clone(), &[(3, 3, "c"), (4, 4, "d")]);

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&batch1))
            .await
            .expect("send batch1");
        stream_sender
            .send(Arc::clone(&batch2))
            .await
            .expect("send batch2");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        0,
        true, // has_watermark_filtering = true means no deduplication
        Some(3), // limit to 3 rows
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    let record1 = reader.next().expect("first batch").expect("batch");
    assert_eq!(record1.num_rows(), 2); // First batch: 2 rows

    let record2 = reader.next().expect("second batch").expect("batch");
    assert_eq!(record2.num_rows(), 1); // Second batch: only 1 row (limit 3, already emitted 2)

    assert!(reader.next().is_none(), "should be end of stream");
}

#[tokio::test]
async fn writes_arrow_format_with_offset() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    let batch1 = build_batch(Arc::clone(&schema), &[(1, 1, "a"), (2, 2, "b")]);
    let batch2 = build_batch(schema.clone(), &[(3, 3, "c"), (4, 4, "d")]);

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&batch1))
            .await
            .expect("send batch1");
        stream_sender
            .send(Arc::clone(&batch2))
            .await
            .expect("send batch2");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        0,
        true, // has_watermark_filtering = true means no deduplication
        None,
        Some(2), // offset: skip first 2 rows
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    // Should skip first 2 rows from batch1, so record1 should be empty or not exist
    // Then record2 should have the remaining rows
    let record = reader.next().expect("first batch").expect("batch");
    // Should have 2 rows (from batch2, after skipping 2 from batch1)
    assert_eq!(record.num_rows(), 2);

    let event_id_array = record
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("event_id should be Int64Array");
    assert_eq!(event_id_array.value(0), 3); // First row after offset
    assert_eq!(event_id_array.value(1), 4);

    assert!(reader.next().is_none(), "should be end of stream");
}

#[tokio::test]
async fn writes_arrow_format_with_u64_event_ids() {
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
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "String".to_string(),
            },
        ])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    // Build batch with large u64 values that would be u64 in Arrow
    let rows = vec![
        (1u64, 1000000000000u64, "first"),
        (2u64, 1000000000001u64, "second"),
    ];
    let timestamps: Vec<Value> = rows.iter().map(|(ts, _, _)| json!(*ts)).collect();
    let event_ids: Vec<Value> = rows.iter().map(|(_, id, _)| json!(*id)).collect();
    let values: Vec<Value> = rows.iter().map(|(_, _, v)| json!(*v)).collect();

    let batch = Arc::new(
        ColumnBatch::new(
            Arc::clone(&schema),
            vec![timestamps, event_ids, values],
            rows.len(),
            None,
        )
        .expect("batch"),
    );

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&batch))
            .await
            .expect("send batch");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        0,
        false, // Enable deduplication to test event_id access
        None,
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    let record = reader.next().expect("first batch").expect("batch");
    assert_eq!(record.num_rows(), 2);

    // Event ID column might be Int64Array or UInt64Array depending on values
    let event_id_array = record.column(1);
    let event_id_value = if let Some(int_array) = event_id_array.as_any().downcast_ref::<Int64Array>() {
        int_array.value(0) as u64
    } else if let Some(uint_array) = event_id_array.as_any().downcast_ref::<UInt64Array>() {
        uint_array.value(0)
    } else {
        panic!("event_id should be Int64Array or UInt64Array");
    };

    assert_eq!(event_id_value, 1000000000000u64);

    assert!(reader.next().is_none(), "should be end of stream");
}

#[tokio::test]
async fn writes_arrow_format_handles_empty_batches() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    // Create empty batch
    let empty_batch = Arc::new(
        ColumnBatch::new(
            Arc::clone(&schema),
            vec![vec![], vec![], vec![]],
            0,
            None,
        )
        .expect("empty batch"),
    );

    let batch = build_batch(schema.clone(), &[(1, 1, "data")]);

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&empty_batch))
            .await
            .expect("send empty");
        stream_sender
            .send(Arc::clone(&batch))
            .await
            .expect("send batch");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        0,
        true,
        None,
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    // Empty batch should be skipped, only the non-empty batch should appear
    let record = reader.next().expect("first batch").expect("batch");
    assert_eq!(record.num_rows(), 1);

    assert!(reader.next().is_none(), "should be end of stream");
}

#[tokio::test]
async fn writes_arrow_format_optimizes_contiguous_row_indices() {
    let schema = build_schema();
    let metrics = FlowMetrics::new();
    let (stream_sender, stream_receiver) = FlowChannel::bounded(8, Arc::clone(&metrics));
    let stream = QueryBatchStream::new(Arc::clone(&schema), stream_receiver, Vec::new());

    // Create a batch with all rows valid (no filtering)
    let batch = build_batch(Arc::clone(&schema), &[(1, 1, "a"), (2, 2, "b"), (3, 3, "c")]);

    let send_task = tokio::spawn(async move {
        stream_sender
            .send(Arc::clone(&batch))
            .await
            .expect("send batch");
        drop(stream_sender);
    });

    let (mut writer, mut reader) = duplex(16384);
    let renderer = ArrowRenderer;
    let response_future = ShowResponseWriter::new(
        &mut writer,
        &renderer,
        Arc::clone(&schema),
        0,
        true, // No deduplication, all rows should be included
        None,
        None,
    )
    .write(stream);

    let (write_result, _) = tokio::join!(response_future, send_task);
    write_result.expect("write succeeded");
    drop(writer);

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.expect("read output");

    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).expect("stream reader");

    // All rows should be included (contiguous indices optimization should use fast path)
    let record = reader.next().expect("first batch").expect("batch");
    assert_eq!(record.num_rows(), 3);

    let event_id_array = record
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("event_id should be Int64Array");
    assert_eq!(event_id_array.value(0), 1);
    assert_eq!(event_id_array.value(1), 2);
    assert_eq!(event_id_array.value(2), 3);

    assert!(reader.next().is_none(), "should be end of stream");
}
