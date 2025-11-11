use std::collections::HashMap;
use std::sync::Arc;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::merge::sequence_stream::SequenceStreamMerger;
use crate::command::types::{Command, CompareOp, EventSequence, EventTarget, Expr, SequenceLink};
use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, FlowChannel, FlowMetrics};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::shard::manager::ShardManager;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::SchemaRegistryFactory;

fn create_context_with_sequence(
    event_sequence: EventSequence,
    where_clause: Option<Expr>,
) -> QueryContext<'static> {
    let command = Box::leak(Box::new(Command::Query {
        event_type: "page_view".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: Some("timestamp".to_string()),
        where_clause,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: Some("user_id".to_string()),
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: Some(event_sequence),
    }));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    QueryContext::new(command, manager, registry)
}

fn create_followed_by_sequence() -> EventSequence {
    EventSequence {
        head: EventTarget {
            event: "page_view".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::FollowedBy,
            EventTarget {
                event: "purchase".to_string(),
                field: None,
            },
        )],
    }
}

fn create_preceded_by_sequence() -> EventSequence {
    EventSequence {
        head: EventTarget {
            event: "purchase".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::PrecededBy,
            EventTarget {
                event: "page_view".to_string(),
                field: None,
            },
        )],
    }
}

fn create_schema_with_fields(fields: Vec<(&str, &str)>) -> Arc<BatchSchema> {
    let columns: Vec<ColumnSpec> = fields
        .into_iter()
        .map(|(name, logical_type)| ColumnSpec {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
        })
        .collect();
    Arc::new(BatchSchema::new(columns).expect("schema"))
}

async fn build_handle(schema: Arc<BatchSchema>, rows: Vec<Vec<ScalarValue>>) -> ShardFlowHandle {
    let metrics = FlowMetrics::new();
    let (tx, rx) = FlowChannel::bounded(16, Arc::clone(&metrics));

    let pool = BatchPool::new(16).expect("batch pool");
    let mut builder = pool.acquire(Arc::clone(&schema));
    for row in rows.into_iter() {
        builder.push_row(&row).expect("push row");
    }
    let batch = builder.finish().expect("finish batch");

    let send_task = tokio::spawn(async move {
        let _ = tx.send(Arc::new(batch)).await;
    });

    ShardFlowHandle::new(rx, schema, vec![send_task])
}

#[tokio::test]
async fn test_new_creates_merger() {
    let sequence = create_followed_by_sequence();
    let merger = SequenceStreamMerger::new(
        sequence.clone(),
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    // Just verify it was created successfully - fields are private
    // The merge tests will verify functionality
    drop(merger);
}

#[tokio::test]
async fn test_new_with_limit() {
    let sequence = create_followed_by_sequence();
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        Some(10),
    );

    // Just verify it was created successfully
    drop(merger);
}

#[tokio::test]
async fn test_merge_followed_by_basic() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    // Create batches for page_view events
    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    // Create batches for purchase events
    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(2000), // After page_view
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get both events in sequence
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2); // page_view + purchase
}

#[tokio::test]
async fn test_merge_preceded_by_basic() {
    let sequence = create_preceded_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    // Create batches for page_view events (preceding)
    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    // Create batches for purchase events (head)
    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(2000), // After page_view
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get both events in sequence
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2); // page_view + purchase
}

#[tokio::test]
async fn test_merge_with_limit() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        Some(1), // Limit to 1 sequence
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    // Create multiple sequences
    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![
            vec![
                ScalarValue::Utf8("u1".to_string()),
                ScalarValue::Timestamp(1000),
                ScalarValue::Utf8("page_view".to_string()),
                ScalarValue::Utf8("user1".to_string()),
                ScalarValue::Utf8("/product".to_string()),
            ],
            vec![
                ScalarValue::Utf8("u2".to_string()),
                ScalarValue::Timestamp(3000),
                ScalarValue::Utf8("page_view".to_string()),
                ScalarValue::Utf8("user2".to_string()),
                ScalarValue::Utf8("/product".to_string()),
            ],
        ],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![
            vec![
                ScalarValue::Utf8("u1".to_string()),
                ScalarValue::Timestamp(2000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user1".to_string()),
                ScalarValue::Int64(100),
            ],
            vec![
                ScalarValue::Utf8("u2".to_string()),
                ScalarValue::Timestamp(4000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user2".to_string()),
                ScalarValue::Int64(200),
            ],
        ],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get only 1 sequence (2 events) due to limit
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2); // 1 sequence = 2 events
}

#[tokio::test]
async fn test_merge_with_where_clause() {
    let sequence = create_followed_by_sequence();
    let where_clause = Some(Expr::Compare {
        field: "page_view.page".to_string(), // Use event-prefixed field name
        op: CompareOp::Eq,
        value: serde_json::Value::String("/product".to_string()),
    });
    let ctx = create_context_with_sequence(sequence.clone(), where_clause);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![
            vec![
                ScalarValue::Utf8("u1".to_string()),
                ScalarValue::Timestamp(1000),
                ScalarValue::Utf8("page_view".to_string()),
                ScalarValue::Utf8("user1".to_string()),
                ScalarValue::Utf8("/product".to_string()), // Matches WHERE
            ],
            vec![
                ScalarValue::Utf8("u2".to_string()),
                ScalarValue::Timestamp(3000),
                ScalarValue::Utf8("page_view".to_string()),
                ScalarValue::Utf8("user2".to_string()),
                ScalarValue::Utf8("/home".to_string()), // Doesn't match WHERE
            ],
        ],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![
            vec![
                ScalarValue::Utf8("u1".to_string()),
                ScalarValue::Timestamp(2000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user1".to_string()),
                ScalarValue::Int64(100),
            ],
            vec![
                ScalarValue::Utf8("u2".to_string()),
                ScalarValue::Timestamp(4000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user2".to_string()),
                ScalarValue::Int64(200),
            ],
        ],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get only 1 sequence (user1 with /product page_view)
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2); // Only user1 sequence matches WHERE clause
}

#[tokio::test]
async fn test_merge_no_matches() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(2000), // After purchase
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000), // Before page_view - wrong order
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get no events (no valid sequences)
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 0);
}

#[tokio::test]
async fn test_merge_empty_handles() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let handles_by_type = HashMap::new();

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get empty stream
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 0);
}

#[tokio::test]
async fn test_merge_multiple_handles_per_event_type() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    // Create two handles for page_view (simulating multiple shards)
    let page_view_handle1 = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let page_view_handle2 = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u2".to_string()),
            ScalarValue::Timestamp(3000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user2".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![
            vec![
                ScalarValue::Utf8("u1".to_string()),
                ScalarValue::Timestamp(2000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user1".to_string()),
                ScalarValue::Int64(100),
            ],
            vec![
                ScalarValue::Utf8("u2".to_string()),
                ScalarValue::Timestamp(4000),
                ScalarValue::Utf8("purchase".to_string()),
                ScalarValue::Utf8("user2".to_string()),
                ScalarValue::Int64(200),
            ],
        ],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert(
        "page_view".to_string(),
        vec![page_view_handle1, page_view_handle2],
    );
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get both sequences
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 4); // 2 sequences = 4 events
}

#[tokio::test]
async fn test_merge_with_custom_time_field() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "created_at".to_string(), // Custom time field
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("created_at", "Timestamp"), // Custom time field
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Timestamp(1000), // created_at
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("created_at", "Timestamp"), // Custom time field
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(2000),
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Timestamp(2000), // created_at - after page_view
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get both events in sequence
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2);
}

#[tokio::test]
async fn test_merge_with_timestamp_as_int64() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    // Use Int64 for timestamp (should be converted)
    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Int64(1000), // Int64 instead of Timestamp
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Int64(2000), // Int64 instead of Timestamp
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get both events in sequence
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 2);
}

#[tokio::test]
async fn test_merge_different_user_ids_no_match() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    let page_view_handle = build_handle(
        Arc::clone(&page_view_schema),
        vec![vec![
            ScalarValue::Utf8("u1".to_string()),
            ScalarValue::Timestamp(1000),
            ScalarValue::Utf8("page_view".to_string()),
            ScalarValue::Utf8("user1".to_string()),
            ScalarValue::Utf8("/product".to_string()),
        ]],
    )
    .await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(
        Arc::clone(&purchase_schema),
        vec![vec![
            ScalarValue::Utf8("u2".to_string()),
            ScalarValue::Timestamp(2000),
            ScalarValue::Utf8("purchase".to_string()),
            ScalarValue::Utf8("user2".to_string()), // Different user_id
            ScalarValue::Int64(100),
        ]],
    )
    .await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get no events (different user_ids don't match)
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 0);
}

#[tokio::test]
async fn test_merge_empty_batches() {
    let sequence = create_followed_by_sequence();
    let ctx = create_context_with_sequence(sequence.clone(), None);
    let merger = SequenceStreamMerger::new(
        sequence,
        "user_id".to_string(),
        "timestamp".to_string(),
        None,
    );

    let page_view_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("page", "String"),
    ]);

    // Empty handle
    let page_view_handle = build_handle(Arc::clone(&page_view_schema), vec![]).await;

    let purchase_schema = create_schema_with_fields(vec![
        ("context_id", "String"),
        ("timestamp", "Timestamp"),
        ("event_type", "String"),
        ("user_id", "String"),
        ("amount", "Integer"),
    ]);

    let purchase_handle = build_handle(Arc::clone(&purchase_schema), vec![]).await;

    let mut handles_by_type = HashMap::new();
    handles_by_type.insert("page_view".to_string(), vec![page_view_handle]);
    handles_by_type.insert("purchase".to_string(), vec![purchase_handle]);

    let mut stream = merger
        .merge(&ctx, handles_by_type)
        .await
        .expect("merge should succeed");

    // Should get empty stream
    let mut events_received = 0;
    while let Some(batch) = stream.recv().await {
        events_received += batch.len();
    }

    assert_eq!(events_received, 0);
}
