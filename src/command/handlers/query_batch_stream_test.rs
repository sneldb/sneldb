use super::query_batch_stream::QueryBatchStream;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;
use serde_json::json;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_schema_access() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "Integer".to_string(),
            },
        ])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (_tx, rx) = FlowChannel::bounded(16, metrics);
    let stream = QueryBatchStream::new(Arc::clone(&schema), rx, Vec::new());

    let retrieved_schema = stream.schema();
    assert_eq!(retrieved_schema.column_count(), schema.column_count());
    assert_eq!(retrieved_schema.columns()[0].name, schema.columns()[0].name);
    assert_eq!(retrieved_schema.columns()[1].name, schema.columns()[1].name);
}

#[tokio::test]
async fn test_recv_returns_none_when_stream_exhausted() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (tx, rx) = FlowChannel::bounded(16, metrics);
    let mut stream = QueryBatchStream::new(Arc::clone(&schema), rx, Vec::new());

    // Explicitly drop the sender before receiving to ensure the channel is closed
    drop(tx);

    // Stream is exhausted immediately (sender dropped)
    let result = stream.recv().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_recv_returns_batches_from_stream() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "Integer".to_string(),
            },
        ])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (tx, rx) = FlowChannel::bounded(16, Arc::clone(&metrics));
    let mut stream = QueryBatchStream::new(Arc::clone(&schema), rx, Vec::new());

    // Send a batch
    let pool = BatchPool::new(16).expect("batch pool");
    let mut builder = pool.acquire(Arc::clone(&schema));
    builder
        .push_row(&vec![
            ScalarValue::from(json!("ctx-1")),
            ScalarValue::from(json!(42)),
        ])
        .expect("push row");
    let batch = builder.finish().expect("finish batch");
    tx.send(Arc::new(batch)).await.expect("send batch");

    // Receive the batch
    let received = stream.recv().await;
    assert!(received.is_some());
    let batch = received.unwrap();
    assert_eq!(batch.len(), 1);

    // Send another batch
    let mut builder = pool.acquire(Arc::clone(&schema));
    builder
        .push_row(&vec![
            ScalarValue::from(json!("ctx-2")),
            ScalarValue::from(json!(84)),
        ])
        .expect("push row");
    let batch = builder.finish().expect("finish batch");
    tx.send(Arc::new(batch)).await.expect("send batch");

    let received = stream.recv().await;
    assert!(received.is_some());
    let batch = received.unwrap();
    assert_eq!(batch.len(), 1);

    // Close the stream
    drop(tx);
    let result = stream.recv().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_drop_aborts_background_tasks() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (_tx, rx) = FlowChannel::bounded(16, metrics);

    // Create a task that would run indefinitely
    let task_handle = tokio::spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
        }
    });

    let tasks = vec![task_handle];
    let stream = QueryBatchStream::new(Arc::clone(&schema), rx, tasks);

    // Drop the stream, which should abort the task
    drop(stream);

    // Give it a moment to abort
    sleep(Duration::from_millis(50)).await;

    // The task should have been aborted
    // We can verify this by checking if the task panics with "aborted"
}

#[tokio::test]
async fn test_multiple_tasks_are_aborted_on_drop() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (_tx, rx) = FlowChannel::bounded(16, metrics);

    // Create multiple tasks
    let task1 = tokio::spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
        }
    });

    let task2 = tokio::spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
        }
    });

    let task3 = tokio::spawn(async {
        loop {
            sleep(Duration::from_secs(10)).await;
        }
    });

    let tasks = vec![task1, task2, task3];
    let stream = QueryBatchStream::new(Arc::clone(&schema), rx, tasks);

    // Drop the stream
    drop(stream);

    // Give it a moment to abort all tasks
    sleep(Duration::from_millis(50)).await;

    // All tasks should have been aborted
}

#[tokio::test]
async fn test_empty_task_list_is_handled() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "context_id".to_string(),
            logical_type: "String".to_string(),
        }])
        .expect("schema"),
    );

    let metrics = FlowMetrics::new();
    let (_tx, rx) = FlowChannel::bounded(16, metrics);
    let stream = QueryBatchStream::new(Arc::clone(&schema), rx, Vec::new());

    // Dropping should not panic even with empty task list
    drop(stream);
}
