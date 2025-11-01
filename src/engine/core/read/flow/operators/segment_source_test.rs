use std::sync::Arc;

use serde_json::json;

use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowSource, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::test_helpers::factories::EventFactory;

use super::{SegmentSource, SegmentSourceConfig};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_source_emits_all_rows() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".into(),
                logical_type: "String".into(),
            },
            ColumnSpec {
                name: "value".into(),
                logical_type: "Integer".into(),
            },
        ])
        .unwrap(),
    );

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("value", 10)
            .create(),
        EventFactory::new()
            .with("context_id", "ctx2")
            .with("value", 20)
            .create(),
    ];

    let config = SegmentSourceConfig {
        events,
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_source_handles_empty_events() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "value".into(),
            logical_type: "Integer".into(),
        }])
        .unwrap(),
    );

    let config = SegmentSourceConfig {
        events: vec![],
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
    }

    assert_eq!(row_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_source_handles_batch_size_boundary() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "value".into(),
            logical_type: "Integer".into(),
        }])
        .unwrap(),
    );

    // Create exactly batch_size + 1 events to test batch boundary
    let events: Vec<_> = (0..5)
        .map(|i| EventFactory::new().with("value", i).create())
        .collect();

    let config = SegmentSourceConfig {
        events,
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut row_count = 0;
    let mut batch_count = 0;
    while let Some(batch) = rx.recv().await {
        row_count += batch.len();
        batch_count += 1;
    }

    assert_eq!(row_count, 5);
    assert!(batch_count >= 2); // Should span multiple batches
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_source_handles_missing_fields() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "context_id".into(),
                logical_type: "String".into(),
            },
            ColumnSpec {
                name: "value".into(),
                logical_type: "Integer".into(),
            },
            ColumnSpec {
                name: "missing".into(),
                logical_type: "String".into(),
            },
        ])
        .unwrap(),
    );

    let events = vec![
        EventFactory::new()
            .with("context_id", "ctx1")
            .with("value", 10)
            .create(),
    ];

    let config = SegmentSourceConfig {
        events,
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut batches = Vec::new();
    while let Some(batch) = rx.recv().await {
        batches.push(batch);
    }

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.len(), 1);

    // Missing field should be null
    let missing_col = batch.column(2).unwrap();
    assert!(missing_col[0].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_source_preserves_row_order() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "order".into(),
            logical_type: "Integer".into(),
        }])
        .unwrap(),
    );

    let events: Vec<_> = (0..10)
        .map(|i| {
            EventFactory::new()
                .with("payload", json!({ "order": i }))
                .create()
        })
        .collect();

    let config = SegmentSourceConfig {
        events,
        schema: Arc::clone(&schema),
    };

    let source = SegmentSource::new(config);
    let ctx = flow_context(4);
    let (tx, mut rx) = FlowChannel::bounded(4, Arc::clone(ctx.metrics()));

    tokio::spawn(async move {
        source.run(tx, ctx).await.unwrap();
    });

    let mut orders = Vec::new();
    while let Some(batch) = rx.recv().await {
        let column = batch.column(0).unwrap();
        for row_idx in 0..batch.len() {
            if let Some(order) = column[row_idx].as_u64() {
                orders.push(order);
            }
        }
    }

    assert_eq!(orders, (0..10).collect::<Vec<_>>());
}
