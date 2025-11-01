use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, BatchSender, FlowChannel, FlowContext, FlowMetrics, FlowTelemetry,
    OrderedStreamMerger,
};
use crate::engine::core::read::result::ColumnSpec;

fn flow_context(batch_size: usize) -> Arc<FlowContext> {
    let metrics = FlowMetrics::new();
    let pool = BatchPool::new(batch_size).unwrap();
    Arc::new(FlowContext::new(
        batch_size,
        pool,
        metrics,
        None::<&str>,
        FlowTelemetry::default(),
    ))
}

fn create_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "timestamp".into(),
            logical_type: "Timestamp".into(),
        }])
        .unwrap(),
    )
}

async fn send_batch(
    sender: &BatchSender,
    schema: Arc<BatchSchema>,
    values: Vec<u64>,
    batch_size: usize,
) {
    let mut builder = BatchPool::new(batch_size)
        .unwrap()
        .acquire(Arc::clone(&schema));
    for value in values {
        builder.push_row(&[json!(value)]).unwrap();
        if builder.is_full() {
            let batch = builder.finish().unwrap();
            sender.send(batch).await.unwrap();
            builder = BatchPool::new(batch_size)
                .unwrap()
                .acquire(Arc::clone(&schema));
        }
    }
    if builder.len() > 0 {
        let batch = builder.finish().unwrap();
        sender.send(batch).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_merges_two_streams() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    // Send streams in different orders
    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 3, 5], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![2, 4, 6], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,    // order_index for timestamp
        true, // ascending
        0,    // offset
        None, // limit
        out_tx,
        4, // batch_size
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_handles_empty_streams() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    // Send only one stream, other is empty
    let schema1 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 2, 3], 4).await;
    });

    tokio::spawn(async move {
        drop(tx2); // Empty stream
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,
        true,
        0,
        None,
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_respects_limit() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 3, 5], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![2, 4, 6], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,
        true,
        0,
        Some(3), // limit
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_respects_offset() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 3, 5], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![2, 4, 6], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,
        true,
        2, // offset
        None,
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results, vec![3, 4, 5, 6]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_sorts_descending() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![5, 3, 1], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![6, 4, 2], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,
        false, // descending
        0,
        None,
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results, vec![6, 5, 4, 3, 2, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_handles_duplicate_values() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 3, 3], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![2, 3, 4], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2],
        0,
        true,
        0,
        None,
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    // Should handle duplicates correctly (uses shard_idx as tiebreaker)
    assert_eq!(results.len(), 6);
    assert!(results.contains(&1));
    assert!(results.contains(&2));
    assert_eq!(results.iter().filter(|&&v| v == 3).count(), 3);
    assert!(results.contains(&4));
}

#[tokio::test]
async fn ordered_merger_rejects_empty_receivers() {
    let schema = create_schema();
    let result = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![],
        0,
        true,
        0,
        None,
        FlowChannel::bounded(4, FlowMetrics::new()).0,
        4,
    );

    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_merger_handles_three_streams() {
    let schema = create_schema();
    let ctx = flow_context(4);
    let metrics = Arc::clone(ctx.metrics());

    let (tx1, rx1) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx2, rx2) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (tx3, rx3) = FlowChannel::bounded(4, Arc::clone(&metrics));
    let (out_tx, mut out_rx) = FlowChannel::bounded(4, Arc::clone(&metrics));

    let schema1 = Arc::clone(&schema);
    let schema2 = Arc::clone(&schema);
    let schema3 = Arc::clone(&schema);
    tokio::spawn(async move {
        send_batch(&tx1, schema1, vec![1, 4], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx2, schema2, vec![2, 5], 4).await;
    });

    tokio::spawn(async move {
        send_batch(&tx3, schema3, vec![3, 6], 4).await;
    });

    let handle = OrderedStreamMerger::spawn(
        Arc::clone(&schema),
        vec![rx1, rx2, rx3],
        0,
        true,
        0,
        None,
        out_tx,
        4,
    )
    .unwrap();

    let mut results = Vec::new();
    while let Some(batch) = out_rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(value) = column[row_idx].as_u64() {
                results.push(value);
            }
        }
    }

    handle.await.unwrap();

    assert_eq!(results, vec![1, 2, 3, 4, 5, 6]);
}
