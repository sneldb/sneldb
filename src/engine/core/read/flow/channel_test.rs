use std::sync::Arc;

use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;

use super::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;

fn build_batch(schema: &Arc<BatchSchema>, pool: &BatchPool, value: i64) -> super::ColumnBatch {
    let mut builder = pool.acquire(Arc::clone(schema));
    builder
        .push_row(&[json!(value)])
        .expect("row fits in batch");
    builder.finish().expect("batch builds")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_updates_metrics() {
    let metrics = FlowMetrics::new();
    let (sender, mut receiver) = FlowChannel::bounded(1, Arc::clone(&metrics));

    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "value".into(),
            logical_type: "Integer".into(),
        }])
        .expect("schema builds"),
    );
    let pool = BatchPool::new(2).expect("pool builds");

    assert_eq!(sender.capacity(), 1);

    let batch = build_batch(&schema, &pool, 1);
    sender.send(batch).await.expect("send succeeds");
    assert_eq!(metrics.total_sent_batches(), 1);
    assert_eq!(metrics.pending_batches(), 1);

    let received = receiver.recv().await.expect("receive succeeds");
    assert_eq!(received.len(), 1);
    assert_eq!(metrics.total_received_batches(), 1);
    assert_eq!(metrics.pending_batches(), 0);

    // Saturate channel again to check backpressure accounting.
    let saturated = build_batch(&schema, &pool, 2);
    sender
        .send(saturated)
        .await
        .expect("second send should enqueue");

    let blocked = build_batch(&schema, &pool, 3);
    match sender.try_send(blocked) {
        Err(TrySendError::Full(returned)) => {
            // returned batch drops here, recycling buffers via Drop impl
            drop(returned);
        }
        _ => panic!("try_send should report full"),
    }
    assert_eq!(metrics.backpressure_events(), 1);
    assert_eq!(metrics.total_sent_batches(), 2);
    assert_eq!(metrics.pending_batches(), 1);

    receiver.recv().await.expect("drain saturated batch");
    assert_eq!(metrics.pending_batches(), 0);

    receiver.close();
    assert!(receiver.recv().await.is_none());
}
