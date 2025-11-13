use super::partial_converter::PartialConverter;
use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::partial::{AggPartial, AggState, GroupKey};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, FlowChannel, FlowContext, FlowMetrics, FlowTelemetry,
};
use crate::engine::core::read::result::ColumnSpec;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;

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

fn create_schema(columns: Vec<(&str, &str)>) -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(
            columns
                .into_iter()
                .map(|(name, logical_type)| ColumnSpec {
                    name: name.to_string(),
                    logical_type: logical_type.to_string(),
                })
                .collect(),
        )
        .expect("valid schema"),
    )
}

fn make_group_key(bucket: Option<u64>, groups: Vec<String>) -> GroupKey {
    GroupKey { bucket, groups }
}

// Basic conversion tests -----------------------------------------------------

#[tokio::test]
async fn partial_converter_converts_single_group_count_all() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: None,
        specs: vec![AggregateOpSpec::CountAll],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec![]),
                vec![AggState::CountAll { count: 5 }],
            );
            map
        },
    };

    let schema = create_schema(vec![("count", "Integer")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_i64().unwrap(), 5);
}

#[tokio::test]
async fn partial_converter_converts_multiple_groups() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::CountAll],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::CountAll { count: 3 }],
            );
            map.insert(
                make_group_key(None, vec!["EU".to_string()]),
                vec![AggState::CountAll { count: 2 }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("count", "Integer")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 2);

    // Check that both groups are present
    let mut regions = Vec::new();
    for i in 0..batch.len() {
        regions.push(batch.column(0).unwrap()[i].as_str().unwrap().to_string());
    }
    regions.sort();
    assert_eq!(regions, vec!["EU", "US"]);
}

#[tokio::test]
async fn partial_converter_converts_with_time_bucket() {
    let bucket_ts = 1735689600u64; // 2025-01-01 00:00:00
    let partial = AggPartial {
        time_bucket: Some(TimeGranularity::Hour),
        group_by: None,
        specs: vec![AggregateOpSpec::CountAll],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(Some(bucket_ts), vec![]),
                vec![AggState::CountAll { count: 10 }],
            );
            map
        },
    };

    let schema = create_schema(vec![("bucket", "Timestamp"), ("count", "Integer")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch.column(0).unwrap()[0].as_i64().unwrap(),
        bucket_ts as i64
    );
    assert_eq!(batch.column(1).unwrap()[0].as_i64().unwrap(), 10);
}

#[tokio::test]
async fn partial_converter_converts_with_time_bucket_and_group_by() {
    let bucket_ts = 1735689600u64;
    let partial = AggPartial {
        time_bucket: Some(TimeGranularity::Hour),
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::CountAll],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(Some(bucket_ts), vec!["US".to_string()]),
                vec![AggState::CountAll { count: 5 }],
            );
            map
        },
    };

    let schema = create_schema(vec![
        ("bucket", "Timestamp"),
        ("region", "String"),
        ("count", "Integer"),
    ]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch.column(0).unwrap()[0].as_i64().unwrap(),
        bucket_ts as i64
    );
    assert_eq!(batch.column(1).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(2).unwrap()[0].as_i64().unwrap(), 5);
}

// Aggregate state type tests -------------------------------------------------

#[tokio::test]
async fn partial_converter_converts_count_unique() {
    use std::collections::HashSet;
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            let mut values = HashSet::new();
            values.insert("user1".to_string());
            values.insert("user2".to_string());
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::CountUnique { values }],
            );
            map
        },
    };

    let schema = create_schema(vec![
        ("region", "String"),
        ("count_unique_user_id_values", "String"),
    ]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    let json_str = batch.column(1).unwrap()[0].as_str().unwrap().to_string();
    let parsed: Vec<String> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 2);
}

#[tokio::test]
async fn partial_converter_converts_total() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Sum { sum: 150 }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("total_amount", "Integer")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_i64().unwrap(), 150);
}

#[tokio::test]
async fn partial_converter_converts_avg() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Avg { sum: 60, count: 3 }],
            );
            map
        },
    };

    let schema = create_schema(vec![
        ("region", "String"),
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
    ]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_i64().unwrap(), 60);
    assert_eq!(batch.column(2).unwrap()[0].as_i64().unwrap(), 3);
}

#[tokio::test]
async fn partial_converter_converts_min_numeric() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Min {
            field: "score".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Min {
                    min_num: Some(30),
                    min_str: None,
                }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("min_score", "String")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_i64().unwrap(), 30);
}

#[tokio::test]
async fn partial_converter_converts_min_string() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Min {
            field: "name".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Min {
                    min_num: None,
                    min_str: Some("Alice".to_string()),
                }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("min_name", "String")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_str().unwrap(), "Alice");
}

#[tokio::test]
async fn partial_converter_converts_min_empty() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Min {
            field: "name".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Min {
                    min_num: None,
                    min_str: None,
                }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("min_name", "String")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_str().unwrap(), "");
}

#[tokio::test]
async fn partial_converter_converts_max_numeric() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Max {
            field: "score".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Max {
                    max_num: Some(90),
                    max_str: None,
                }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("max_score", "String")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_i64().unwrap(), 90);
}

#[tokio::test]
async fn partial_converter_converts_max_string() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::Max {
            field: "name".to_string(),
        }],
        groups: {
            let mut map = HashMap::new();
            map.insert(
                make_group_key(None, vec!["US".to_string()]),
                vec![AggState::Max {
                    max_num: None,
                    max_str: Some("Zebra".to_string()),
                }],
            );
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("max_name", "String")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.column(0).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(1).unwrap()[0].as_str().unwrap(), "Zebra");
}

// Batch splitting tests -------------------------------------------------------

#[tokio::test]
async fn partial_converter_splits_into_multiple_batches() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: Some(vec!["region".to_string()]),
        specs: vec![AggregateOpSpec::CountAll],
        groups: {
            let mut map = HashMap::new();
            // Create more groups than batch size
            for i in 0..5 {
                map.insert(
                    make_group_key(None, vec![format!("region{}", i)]),
                    vec![AggState::CountAll { count: i as i64 }],
                );
            }
            map
        },
    };

    let schema = create_schema(vec![("region", "String"), ("count", "Integer")]);
    let ctx = flow_context(2); // Small batch size to force splitting
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let mut batches = Vec::new();
    while let Some(batch) = rx.recv().await {
        batches.push(batch);
    }

    assert!(batches.len() >= 2); // Should be split into multiple batches
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();
    assert_eq!(total_rows, 5);
}

#[tokio::test]
async fn partial_converter_handles_empty_groups() {
    let partial = AggPartial {
        time_bucket: None,
        group_by: None,
        specs: vec![AggregateOpSpec::CountAll],
        groups: HashMap::new(),
    };

    let schema = create_schema(vec![("count", "Integer")]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    // Should not receive any batches - channel should be closed
    assert!(rx.recv().await.is_none());
}

// Complex aggregation tests ---------------------------------------------------

#[tokio::test]
async fn partial_converter_converts_all_aggregate_types() {
    use std::collections::HashSet;
    let partial = AggPartial {
        time_bucket: Some(TimeGranularity::Hour),
        group_by: Some(vec!["region".to_string()]),
        specs: vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Total {
                field: "quantity".to_string(),
            },
            AggregateOpSpec::Min {
                field: "score".to_string(),
            },
            AggregateOpSpec::Max {
                field: "name".to_string(),
            },
        ],
        groups: {
            let mut map = HashMap::new();
            let mut unique_values = HashSet::new();
            unique_values.insert("user1".to_string());
            unique_values.insert("user2".to_string());
            map.insert(
                make_group_key(Some(1735689600u64), vec!["US".to_string()]),
                vec![
                    AggState::CountAll { count: 3 },
                    AggState::CountUnique {
                        values: unique_values,
                    },
                    AggState::Avg { sum: 60, count: 3 },
                    AggState::Sum { sum: 6 },
                    AggState::Min {
                        min_num: Some(30),
                        min_str: None,
                    },
                    AggState::Max {
                        max_num: None,
                        max_str: Some("Zebra".to_string()),
                    },
                ],
            );
            map
        },
    };

    let schema = create_schema(vec![
        ("bucket", "Timestamp"),
        ("region", "String"),
        ("count", "Integer"),
        ("count_unique_user_id_values", "String"),
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
        ("total_quantity", "Integer"),
        ("min_score", "String"),
        ("max_name", "String"),
    ]);
    let ctx = flow_context(10);
    let (tx, mut rx) = FlowChannel::bounded(10, Arc::clone(ctx.metrics()));

    PartialConverter::to_batches(partial, schema, ctx, tx)
        .await
        .unwrap();

    let batch = rx.recv().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch.schema().column_count(), 9);
    assert_eq!(batch.column(0).unwrap()[0].as_i64().unwrap(), 1735689600i64);
    assert_eq!(batch.column(1).unwrap()[0].as_str().unwrap(), "US");
    assert_eq!(batch.column(2).unwrap()[0].as_i64().unwrap(), 3);
    assert!(!batch.column(3).unwrap()[0].as_str().unwrap().is_empty());
    assert_eq!(batch.column(4).unwrap()[0].as_i64().unwrap(), 60);
    assert_eq!(batch.column(5).unwrap()[0].as_i64().unwrap(), 3);
    assert_eq!(batch.column(6).unwrap()[0].as_i64().unwrap(), 6);
    assert_eq!(batch.column(7).unwrap()[0].as_i64().unwrap(), 30);
    assert_eq!(batch.column(8).unwrap()[0].as_str().unwrap(), "Zebra");
}
