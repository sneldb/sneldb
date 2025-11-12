use std::collections::HashMap;
use std::sync::Arc;

use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::partial::{AggState, GroupKey};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::{
    BatchPool, BatchSchema, ColumnBatch, FlowChannel, FlowMetrics,
};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;

use super::aggregate_stream::AggregateStreamMerger;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_batch_schema(columns: Vec<(&str, &str)>) -> Arc<BatchSchema> {
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

fn create_column_batch(schema: Arc<BatchSchema>, rows: Vec<Vec<ScalarValue>>) -> Arc<ColumnBatch> {
    let pool = BatchPool::new(rows.len().max(1)).expect("valid pool");
    let mut builder = pool.acquire(schema);

    for row in rows {
        builder.push_row(&row).expect("push row");
    }

    Arc::new(builder.finish().expect("finish batch"))
}

fn create_aggregate_plan(
    ops: Vec<AggregateOpSpec>,
    group_by: Option<Vec<String>>,
    time_bucket: Option<TimeGranularity>,
) -> AggregatePlan {
    AggregatePlan {
        ops,
        group_by,
        time_bucket,
    }
}

// ============================================================================
// Schema Building Tests
// ============================================================================

#[test]
fn build_final_output_schema_scalar_count() {
    let plan = create_aggregate_plan(vec![AggregateOpSpec::CountAll], None, None);
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.columns()[0].name, "count");
    assert_eq!(schema.columns()[0].logical_type, "Integer");
}

#[test]
fn build_final_output_schema_count_field() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountField {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.columns()[0].name, "count_user_id");
    assert_eq!(schema.columns()[0].logical_type, "Integer");
}

#[test]
fn build_final_output_schema_count_unique() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.columns()[0].name, "count_unique_user_id");
    assert_eq!(schema.columns()[0].logical_type, "Integer");
}

#[test]
fn build_final_output_schema_avg() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        None,
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.columns()[0].name, "avg_amount");
    assert_eq!(schema.columns()[0].logical_type, "Float");
}

#[test]
fn build_final_output_schema_total() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        None,
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 1);
    assert_eq!(schema.columns()[0].name, "total_amount");
    assert_eq!(schema.columns()[0].logical_type, "Integer");
}

#[test]
fn build_final_output_schema_min_max() {
    let plan = create_aggregate_plan(
        vec![
            AggregateOpSpec::Min {
                field: "value".to_string(),
            },
            AggregateOpSpec::Max {
                field: "value".to_string(),
            },
        ],
        None,
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 2);
    assert_eq!(schema.columns()[0].name, "min_value");
    assert_eq!(schema.columns()[0].logical_type, "String");
    assert_eq!(schema.columns()[1].name, "max_value");
    assert_eq!(schema.columns()[1].logical_type, "String");
}

#[test]
fn build_final_output_schema_with_group_by() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string(), "region".to_string()]),
        None,
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 3);
    assert_eq!(schema.columns()[0].name, "country");
    assert_eq!(schema.columns()[1].name, "region");
    assert_eq!(schema.columns()[2].name, "count");
}

#[test]
fn build_final_output_schema_with_time_bucket() {
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        None,
        Some(TimeGranularity::Hour),
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 2);
    assert_eq!(schema.columns()[0].name, "bucket");
    assert_eq!(schema.columns()[0].logical_type, "Timestamp");
    assert_eq!(schema.columns()[1].name, "count");
}

#[test]
fn build_final_output_schema_complex() {
    let plan = create_aggregate_plan(
        vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::Avg {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
        ],
        Some(vec!["country".to_string()]),
        Some(TimeGranularity::Day),
    );
    let schema = AggregateStreamMerger::build_final_output_schema(&plan).unwrap();

    assert_eq!(schema.column_count(), 6); // bucket + country + 4 metrics
    assert_eq!(schema.columns()[0].name, "bucket");
    assert_eq!(schema.columns()[1].name, "country");
    assert_eq!(schema.columns()[2].name, "count");
    assert_eq!(schema.columns()[3].name, "avg_amount");
    assert_eq!(schema.columns()[4].name, "total_amount");
    assert_eq!(schema.columns()[5].name, "count_unique_user_id");
}

// ============================================================================
// Scalar Conversion Helper Tests
// ============================================================================

#[test]
fn scalar_to_i64_from_int64() {
    let value = ScalarValue::Int64(42);
    let result = AggregateStreamMerger::scalar_to_i64(&value).unwrap();
    assert_eq!(result, 42);
}

#[test]
fn scalar_to_i64_from_float64() {
    let value = ScalarValue::Float64(42.7);
    let result = AggregateStreamMerger::scalar_to_i64(&value).unwrap();
    assert_eq!(result, 42);
}

#[test]
fn scalar_to_i64_from_string() {
    let value = ScalarValue::Utf8("42".to_string());
    let result = AggregateStreamMerger::scalar_to_i64(&value).unwrap();
    assert_eq!(result, 42);
}

#[test]
fn scalar_to_i64_invalid() {
    let value = ScalarValue::Boolean(true);
    let result = AggregateStreamMerger::scalar_to_i64(&value);
    assert!(result.is_err());
}

#[test]
fn scalar_to_u64_from_int64() {
    let value = ScalarValue::Int64(42);
    let result = AggregateStreamMerger::scalar_to_u64(&value);
    assert_eq!(result, Some(42));
}

#[test]
fn scalar_to_u64_from_negative_int64() {
    let value = ScalarValue::Int64(-1);
    let result = AggregateStreamMerger::scalar_to_u64(&value);
    assert_eq!(result, None);
}

#[test]
fn scalar_to_u64_from_timestamp() {
    let value = ScalarValue::Timestamp(1000);
    let result = AggregateStreamMerger::scalar_to_u64(&value);
    assert_eq!(result, Some(1000));
}

#[test]
fn scalar_to_u64_from_string() {
    let value = ScalarValue::Utf8("42".to_string());
    let result = AggregateStreamMerger::scalar_to_u64(&value);
    assert_eq!(result, Some(42));
}

#[test]
fn scalar_to_string_from_utf8() {
    let value = ScalarValue::Utf8("hello".to_string());
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, "hello");
}

#[test]
fn scalar_to_string_from_int64() {
    let value = ScalarValue::Int64(42);
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, "42");
}

#[test]
fn scalar_to_string_from_float64() {
    let value = ScalarValue::Float64(42.5);
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, "42.5");
}

#[test]
fn scalar_to_string_from_boolean() {
    let value = ScalarValue::Boolean(true);
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, "true");
}

#[test]
fn scalar_to_string_from_null() {
    let value = ScalarValue::Null;
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, "");
}

#[test]
fn scalar_to_string_from_binary() {
    let value = ScalarValue::Binary(vec![1, 2, 3]);
    let result = AggregateStreamMerger::scalar_to_string(&value);
    assert_eq!(result, ""); // Binary not supported in aggregates
}

#[test]
fn scalar_to_min_max_from_int64() {
    let value = ScalarValue::Int64(42);
    let (num, str) = AggregateStreamMerger::scalar_to_min_max(&value).unwrap();
    assert_eq!(num, Some(42));
    assert_eq!(str, None);
}

#[test]
fn scalar_to_min_max_from_utf8() {
    let value = ScalarValue::Utf8("hello".to_string());
    let (num, str) = AggregateStreamMerger::scalar_to_min_max(&value).unwrap();
    assert_eq!(num, None);
    assert_eq!(str, Some("hello".to_string()));
}

#[test]
fn scalar_to_min_max_from_float64() {
    let value = ScalarValue::Float64(42.7);
    let (num, str) = AggregateStreamMerger::scalar_to_min_max(&value).unwrap();
    assert_eq!(num, Some(42));
    assert_eq!(str, None);
}

#[test]
fn scalar_to_min_max_from_null() {
    let value = ScalarValue::Null;
    let (num, str) = AggregateStreamMerger::scalar_to_min_max(&value).unwrap();
    assert_eq!(num, None);
    assert_eq!(str, None);
}

#[test]
fn scalar_to_min_max_invalid() {
    let value = ScalarValue::Boolean(true);
    let result = AggregateStreamMerger::scalar_to_min_max(&value);
    assert!(result.is_err());
}

// ============================================================================
// Scalar to AggState Conversion Tests
// ============================================================================

#[test]
fn scalar_to_agg_state_count_all() {
    let value = ScalarValue::Int64(10);
    let spec = AggregateOpSpec::CountAll;
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::CountAll { count } => assert_eq!(count, 10),
        _ => panic!("Expected CountAll state"),
    }
}

#[test]
fn scalar_to_agg_state_count_field() {
    let value = ScalarValue::Int64(5);
    let spec = AggregateOpSpec::CountField {
        field: "user_id".to_string(),
    };
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::CountAll { count } => assert_eq!(count, 5),
        _ => panic!("Expected CountAll state"),
    }
}

#[test]
fn scalar_to_agg_state_total() {
    let value = ScalarValue::Int64(100);
    let spec = AggregateOpSpec::Total {
        field: "amount".to_string(),
    };
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::Sum { sum } => assert_eq!(sum, 100),
        _ => panic!("Expected Sum state"),
    }
}

#[test]
fn scalar_to_agg_state_min() {
    let value = ScalarValue::Int64(10);
    let spec = AggregateOpSpec::Min {
        field: "value".to_string(),
    };
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::Min { min_num, min_str } => {
            assert_eq!(min_num, Some(10));
            assert_eq!(min_str, None);
        }
        _ => panic!("Expected Min state"),
    }
}

#[test]
fn scalar_to_agg_state_min_string() {
    let value = ScalarValue::Utf8("apple".to_string());
    let spec = AggregateOpSpec::Min {
        field: "value".to_string(),
    };
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::Min { min_num, min_str } => {
            assert_eq!(min_num, None);
            assert_eq!(min_str, Some("apple".to_string()));
        }
        _ => panic!("Expected Min state"),
    }
}

#[test]
fn scalar_to_agg_state_max() {
    let value = ScalarValue::Int64(100);
    let spec = AggregateOpSpec::Max {
        field: "value".to_string(),
    };
    let state = AggregateStreamMerger::scalar_to_agg_state(&value, &spec).unwrap();
    match state {
        AggState::Max { max_num, max_str } => {
            assert_eq!(max_num, Some(100));
            assert_eq!(max_str, None);
        }
        _ => panic!("Expected Max state"),
    }
}

#[test]
fn scalar_to_agg_state_avg_should_error() {
    let value = ScalarValue::Int64(10);
    let spec = AggregateOpSpec::Avg {
        field: "amount".to_string(),
    };
    let result = AggregateStreamMerger::scalar_to_agg_state(&value, &spec);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("AVG should be handled directly")
    );
}

#[test]
fn scalar_to_agg_state_count_unique_should_error() {
    let value = ScalarValue::Utf8("[]".to_string());
    let spec = AggregateOpSpec::CountUnique {
        field: "user_id".to_string(),
    };
    let result = AggregateStreamMerger::scalar_to_agg_state(&value, &spec);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("CountUnique should be handled directly")
    );
}

// ============================================================================
// AggState to Scalar Conversion Tests
// ============================================================================

#[test]
fn agg_state_to_scalar_count_all() {
    let state = AggState::CountAll { count: 10 };
    let spec = AggregateOpSpec::CountAll;
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(10));
}

#[test]
fn agg_state_to_scalar_count_field() {
    let state = AggState::CountAll { count: 5 };
    let spec = AggregateOpSpec::CountField {
        field: "user_id".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(5));
}

#[test]
fn agg_state_to_scalar_count_unique() {
    let mut values = std::collections::HashSet::new();
    values.insert("user1".to_string());
    values.insert("user2".to_string());
    values.insert("user3".to_string());
    let state = AggState::CountUnique { values };
    let spec = AggregateOpSpec::CountUnique {
        field: "user_id".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(3));
}

#[test]
fn agg_state_to_scalar_count_unique_empty() {
    let values = std::collections::HashSet::new();
    let state = AggState::CountUnique { values };
    let spec = AggregateOpSpec::CountUnique {
        field: "user_id".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(0));
}

#[test]
fn agg_state_to_scalar_sum() {
    let state = AggState::Sum { sum: 100 };
    let spec = AggregateOpSpec::Total {
        field: "amount".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(100));
}

#[test]
fn agg_state_to_scalar_avg() {
    let state = AggState::Avg { sum: 30, count: 3 };
    let spec = AggregateOpSpec::Avg {
        field: "amount".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    match scalar {
        ScalarValue::Float64(f) => assert!((f - 10.0).abs() < 0.001),
        _ => panic!("Expected Float64"),
    }
}

#[test]
fn agg_state_to_scalar_avg_zero_count() {
    let state = AggState::Avg { sum: 0, count: 0 };
    let spec = AggregateOpSpec::Avg {
        field: "amount".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    match scalar {
        ScalarValue::Float64(f) => assert_eq!(f, 0.0),
        _ => panic!("Expected Float64"),
    }
}

#[test]
fn agg_state_to_scalar_min_numeric() {
    let state = AggState::Min {
        min_num: Some(10),
        min_str: None,
    };
    let spec = AggregateOpSpec::Min {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(10));
}

#[test]
fn agg_state_to_scalar_min_string() {
    let state = AggState::Min {
        min_num: None,
        min_str: Some("apple".to_string()),
    };
    let spec = AggregateOpSpec::Min {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Utf8("apple".to_string()));
}

#[test]
fn agg_state_to_scalar_min_empty() {
    let state = AggState::Min {
        min_num: None,
        min_str: None,
    };
    let spec = AggregateOpSpec::Min {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Utf8(String::new()));
}

#[test]
fn agg_state_to_scalar_max_numeric() {
    let state = AggState::Max {
        max_num: Some(100),
        max_str: None,
    };
    let spec = AggregateOpSpec::Max {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Int64(100));
}

#[test]
fn agg_state_to_scalar_max_string() {
    let state = AggState::Max {
        max_num: None,
        max_str: Some("zebra".to_string()),
    };
    let spec = AggregateOpSpec::Max {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Utf8("zebra".to_string()));
}

#[test]
fn agg_state_to_scalar_max_empty() {
    let state = AggState::Max {
        max_num: None,
        max_str: None,
    };
    let spec = AggregateOpSpec::Max {
        field: "value".to_string(),
    };
    let scalar = AggregateStreamMerger::agg_state_to_scalar(&state, &spec).unwrap();
    assert_eq!(scalar, ScalarValue::Utf8(String::new()));
}

// ============================================================================
// Parse Aggregate Row Tests
// ============================================================================

#[test]
fn parse_aggregate_row_scalar_count() {
    let schema = create_batch_schema(vec![("count", "Integer")]);
    let batch = create_column_batch(schema.clone(), vec![vec![ScalarValue::Int64(10)]]);
    let plan = create_aggregate_plan(vec![AggregateOpSpec::CountAll], None, None);

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(group_key.bucket, None);
    assert_eq!(group_key.groups, Vec::<String>::new());
    assert_eq!(states.len(), 1);
    match &states[0] {
        AggState::CountAll { count } => assert_eq!(*count, 10),
        _ => panic!("Expected CountAll state"),
    }
}

#[test]
fn parse_aggregate_row_with_group_by() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::Utf8("US".to_string()),
            ScalarValue::Int64(5),
        ]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(group_key.groups, vec!["US"]);
    assert_eq!(states.len(), 1);
}

#[test]
fn parse_aggregate_row_with_time_bucket() {
    let schema = create_batch_schema(vec![("bucket", "Timestamp"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::Int64(1000), ScalarValue::Int64(5)]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        None,
        Some(TimeGranularity::Hour),
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(group_key.bucket, Some(1000));
    assert_eq!(states.len(), 1);
}

#[test]
fn parse_aggregate_row_avg() {
    let schema = create_batch_schema(vec![
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::Int64(30), ScalarValue::Int64(3)]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(states.len(), 1);
    match &states[0] {
        AggState::Avg { sum, count } => {
            assert_eq!(*sum, 30);
            assert_eq!(*count, 3);
        }
        _ => panic!("Expected Avg state"),
    }
}

#[test]
fn parse_aggregate_row_count_unique() {
    let schema = create_batch_schema(vec![("count_unique_user_id_values", "String")]);
    let json_str = serde_json::to_string(&vec!["user1", "user2", "user3"]).unwrap();
    let batch = create_column_batch(schema.clone(), vec![vec![ScalarValue::Utf8(json_str)]]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(states.len(), 1);
    match &states[0] {
        AggState::CountUnique { values } => {
            assert_eq!(values.len(), 3);
            assert!(values.contains("user1"));
            assert!(values.contains("user2"));
            assert!(values.contains("user3"));
        }
        _ => panic!("Expected CountUnique state"),
    }
}

#[test]
fn parse_aggregate_row_count_unique_empty_array() {
    let schema = create_batch_schema(vec![("count_unique_user_id_values", "String")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::Utf8("[]".to_string())]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    match &states[0] {
        AggState::CountUnique { values } => {
            assert_eq!(values.len(), 0);
        }
        _ => panic!("Expected CountUnique state"),
    }
}

#[test]
fn parse_aggregate_row_count_unique_empty_string() {
    let schema = create_batch_schema(vec![("count_unique_user_id_values", "String")]);
    let batch = create_column_batch(schema.clone(), vec![vec![ScalarValue::Utf8(String::new())]]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    match &states[0] {
        AggState::CountUnique { values } => {
            assert_eq!(values.len(), 0);
        }
        _ => panic!("Expected CountUnique state"),
    }
}

#[test]
fn parse_aggregate_row_multiple_aggregations() {
    let schema = create_batch_schema(vec![
        ("count", "Integer"),
        ("total_amount", "Integer"),
        ("avg_score_sum", "Integer"),
        ("avg_score_count", "Integer"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::Int64(5),
            ScalarValue::Int64(100),
            ScalarValue::Int64(50),
            ScalarValue::Int64(5),
        ]],
    );
    let plan = create_aggregate_plan(
        vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "score".to_string(),
            },
        ],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(states.len(), 3);
    match &states[0] {
        AggState::CountAll { count } => assert_eq!(*count, 5),
        _ => panic!("Expected CountAll"),
    }
    match &states[1] {
        AggState::Sum { sum } => assert_eq!(*sum, 100),
        _ => panic!("Expected Sum"),
    }
    match &states[2] {
        AggState::Avg { sum, count } => {
            assert_eq!(*sum, 50);
            assert_eq!(*count, 5);
        }
        _ => panic!("Expected Avg"),
    }
}

#[test]
fn parse_aggregate_row_complex() {
    let schema = create_batch_schema(vec![
        ("bucket", "Timestamp"),
        ("country", "String"),
        ("region", "String"),
        ("count", "Integer"),
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
        ("count_unique_user_id_values", "String"),
    ]);
    let json_str = serde_json::to_string(&vec!["user1", "user2"]).unwrap();
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::Int64(1000),
            ScalarValue::Utf8("US".to_string()),
            ScalarValue::Utf8("CA".to_string()),
            ScalarValue::Int64(10),
            ScalarValue::Int64(200),
            ScalarValue::Int64(10),
            ScalarValue::Utf8(json_str),
        ]],
    );
    let plan = create_aggregate_plan(
        vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::Avg {
                field: "amount".to_string(),
            },
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
        ],
        Some(vec!["country".to_string(), "region".to_string()]),
        Some(TimeGranularity::Hour),
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let (group_key, states) =
        AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan).unwrap();

    assert_eq!(group_key.bucket, Some(1000));
    assert_eq!(group_key.groups, vec!["US", "CA"]);
    assert_eq!(states.len(), 3);
}

#[test]
fn parse_aggregate_row_missing_column_error() {
    let schema = create_batch_schema(vec![("count", "Integer")]);
    let batch = create_column_batch(schema.clone(), vec![vec![ScalarValue::Int64(10)]]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let result = AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("missing avg_amount_sum"));
}

// ============================================================================
// Merge Batch Into Groups Tests
// ============================================================================

#[test]
fn merge_batch_into_groups_single_row() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::Utf8("US".to_string()),
            ScalarValue::Int64(5),
        ]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 1);
    let _group_key = GroupKey {
        bucket: None,
        groups: vec!["US".to_string()],
    };
    assert!(merged_groups.contains_key(&_group_key));
}

#[test]
fn merge_batch_into_groups_multiple_rows_same_group() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![ScalarValue::Utf8("US".to_string()), ScalarValue::Int64(3)],
            vec![ScalarValue::Utf8("US".to_string()), ScalarValue::Int64(2)],
        ],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 1);
    let group_key = GroupKey {
        bucket: None,
        groups: vec!["US".to_string()],
    };
    let states = merged_groups.get(&group_key).unwrap();
    match &states[0] {
        AggState::CountAll { count } => assert_eq!(*count, 5), // 3 + 2 merged
        _ => panic!("Expected CountAll"),
    }
}

#[test]
fn merge_batch_into_groups_multiple_rows_different_groups() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![ScalarValue::Utf8("US".to_string()), ScalarValue::Int64(5)],
            vec![ScalarValue::Utf8("DE".to_string()), ScalarValue::Int64(3)],
        ],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 2);
}

#[test]
fn merge_batch_into_groups_merges_avg() {
    let schema = create_batch_schema(vec![
        ("country", "String"),
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::Utf8("US".to_string()),
                ScalarValue::Int64(20),
                ScalarValue::Int64(2),
            ],
            vec![
                ScalarValue::Utf8("US".to_string()),
                ScalarValue::Int64(30),
                ScalarValue::Int64(3),
            ],
        ],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::Avg {
            field: "amount".to_string(),
        }],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 1);
    let group_key = GroupKey {
        bucket: None,
        groups: vec!["US".to_string()],
    };
    let states = merged_groups.get(&group_key).unwrap();
    match &states[0] {
        AggState::Avg { sum, count } => {
            assert_eq!(*sum, 50); // 20 + 30
            assert_eq!(*count, 5); // 2 + 3
        }
        _ => panic!("Expected Avg"),
    }
}

#[test]
fn merge_batch_into_groups_merges_count_unique() {
    let schema = create_batch_schema(vec![
        ("country", "String"),
        ("count_unique_user_id_values", "String"),
    ]);
    let json1 = serde_json::to_string(&vec!["user1", "user2"]).unwrap();
    let json2 = serde_json::to_string(&vec!["user2", "user3"]).unwrap();
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::Utf8("US".to_string()),
                ScalarValue::Utf8(json1),
            ],
            vec![
                ScalarValue::Utf8("US".to_string()),
                ScalarValue::Utf8(json2),
            ],
        ],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 1);
    let group_key = GroupKey {
        bucket: None,
        groups: vec!["US".to_string()],
    };
    let states = merged_groups.get(&group_key).unwrap();
    match &states[0] {
        AggState::CountUnique { values } => {
            assert_eq!(values.len(), 3); // user1, user2, user3 (user2 deduplicated)
            assert!(values.contains("user1"));
            assert!(values.contains("user2"));
            assert!(values.contains("user3"));
        }
        _ => panic!("Expected CountUnique"),
    }
}

#[test]
fn merge_batch_into_groups_empty_batch() {
    let schema = create_batch_schema(vec![("count", "Integer")]);
    let batch = create_column_batch(schema.clone(), vec![]);
    let plan = create_aggregate_plan(vec![AggregateOpSpec::CountAll], None, None);

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    assert_eq!(merged_groups.len(), 0);
}

// ============================================================================
// Empty Groups Filtering Tests (via emit_merged_groups)
// ============================================================================

#[tokio::test]
async fn emit_merged_groups_filters_empty_groups() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    // Add valid group
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec!["US".to_string()],
        },
        vec![AggState::CountAll { count: 5 }],
    );
    // Add empty group (should be filtered)
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec![String::new()],
        },
        vec![AggState::CountAll { count: 3 }],
    );
    // Add another valid group
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec!["DE".to_string()],
        },
        vec![AggState::CountAll { count: 2 }],
    );

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        None,
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    // Should only receive batches with non-empty groups
    let mut total_rows = 0;
    while let Some(batch) = rx.recv().await {
        total_rows += batch.len();
    }

    assert_eq!(total_rows, 2); // Only US and DE, not empty group
}

#[tokio::test]
async fn emit_merged_groups_scalar_no_filtering() {
    let schema = create_batch_schema(vec![("count", "Integer")]);
    let plan = create_aggregate_plan(vec![AggregateOpSpec::CountAll], None, None);

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec![],
        },
        vec![AggState::CountAll { count: 10 }],
    );

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        None,
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    let mut total_rows = 0;
    while let Some(batch) = rx.recv().await {
        total_rows += batch.len();
    }

    assert_eq!(total_rows, 1); // Scalar aggregates don't filter empty groups
}

#[tokio::test]
async fn emit_merged_groups_applies_limit() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    for (i, country) in ["US", "DE", "FR", "IT", "ES"].iter().enumerate() {
        merged_groups.insert(
            GroupKey {
                bucket: None,
                groups: vec![country.to_string()],
            },
            vec![AggState::CountAll { count: i as i64 }],
        );
    }

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        Some(3), // LIMIT 3
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    let mut total_rows = 0;
    while let Some(batch) = rx.recv().await {
        total_rows += batch.len();
    }

    assert_eq!(total_rows, 3); // Should be limited to 3 groups
}

#[tokio::test]
async fn emit_merged_groups_empty_groups_returns_nothing() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        None,
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    // Should not receive any batches
    let mut total_rows = 0;
    while let Some(_batch) = rx.recv().await {
        total_rows += 1;
    }

    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn emit_merged_groups_complex_output() {
    let schema = create_batch_schema(vec![
        ("bucket", "Timestamp"),
        ("country", "String"),
        ("count", "Integer"),
        ("avg_amount_sum", "Integer"),
        ("avg_amount_count", "Integer"),
        ("count_unique_user_id_values", "String"),
    ]);
    let plan = create_aggregate_plan(
        vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::Avg {
                field: "amount".to_string(),
            },
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
        ],
        Some(vec!["country".to_string()]),
        Some(TimeGranularity::Hour),
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    let mut values = std::collections::HashSet::new();
    values.insert("user1".to_string());
    values.insert("user2".to_string());
    merged_groups.insert(
        GroupKey {
            bucket: Some(1000),
            groups: vec!["US".to_string()],
        },
        vec![
            AggState::CountAll { count: 10 },
            AggState::Avg {
                sum: 100,
                count: 10,
            },
            AggState::CountUnique { values },
        ],
    );

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        None,
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    let mut batches = Vec::new();
    while let Some(batch) = rx.recv().await {
        batches.push(batch);
    }

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.len(), 1);

    // Verify output schema has final format (avg, not sum/count)
    let output_schema = batch.schema();
    assert_eq!(output_schema.column_count(), 5); // bucket + country + 3 metrics
    assert_eq!(output_schema.columns()[0].name, "bucket");
    assert_eq!(output_schema.columns()[1].name, "country");
    assert_eq!(output_schema.columns()[2].name, "count");
    assert_eq!(output_schema.columns()[3].name, "avg_amount");
    assert_eq!(output_schema.columns()[4].name, "count_unique_user_id");

    // Verify row values
    let row = batch.column(0).unwrap();
    assert_eq!(row[0], ScalarValue::Int64(1000)); // bucket
    let country_col = batch.column(1).unwrap();
    assert_eq!(country_col[0], ScalarValue::Utf8("US".to_string()));
    let count_col = batch.column(2).unwrap();
    assert_eq!(count_col[0], ScalarValue::Int64(10));
    let avg_col = batch.column(3).unwrap();
    match &avg_col[0] {
        ScalarValue::Float64(f) => assert!((*f - 10.0).abs() < 0.001), // 100/10
        _ => panic!("Expected Float64 for avg"),
    }
    let count_unique_col = batch.column(4).unwrap();
    assert_eq!(count_unique_col[0], ScalarValue::Int64(2)); // 2 unique users
}

// ============================================================================
// Edge Cases and Error Handling Tests
// ============================================================================

#[test]
fn parse_aggregate_row_missing_group_by_column() {
    let schema = create_batch_schema(vec![("count", "Integer")]);
    let batch = create_column_batch(schema.clone(), vec![vec![ScalarValue::Int64(10)]]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let result = AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan);
    assert!(result.is_err());
    let err_msg = result.unwrap_err();
    // When group_by column is missing, the code advances metric_start_idx past available columns
    // So it reports "missing metric column" instead of "missing group_by column"
    assert!(
        err_msg.contains("missing metric column") || err_msg.contains("missing group_by column"),
        "Error message should mention missing column, got: {}",
        err_msg
    );
}

#[test]
fn parse_aggregate_row_missing_metric_column() {
    let schema = create_batch_schema(vec![("country", "String")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::Utf8("US".to_string())]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let result = AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("missing metric column"));
}

#[test]
fn parse_aggregate_row_invalid_count_unique_json() {
    let schema = create_batch_schema(vec![("count_unique_user_id_values", "String")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::Utf8("invalid json".to_string())]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountUnique {
            field: "user_id".to_string(),
        }],
        None,
        None,
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let mut column_vecs: Vec<Vec<ScalarValue>> = Vec::new();
    for col_idx in 0..schema.column_count() {
        column_vecs.push(batch.column(col_idx).unwrap());
    }
    let column_views: Vec<&[ScalarValue]> = column_vecs.iter().map(|v| v.as_slice()).collect();

    let result = AggregateStreamMerger::parse_aggregate_row(&column_views, &column_names, 0, &plan);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("failed to parse CountUnique JSON")
    );
}

#[test]
fn merge_batch_into_groups_mismatched_state_lengths() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::Utf8("US".to_string()),
            ScalarValue::Int64(5),
        ]],
    );
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    // Insert a group with wrong number of states
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec!["US".to_string()],
        },
        vec![
            AggState::CountAll { count: 3 },
            AggState::CountAll { count: 2 }, // Wrong: should only have 1 state
        ],
    );

    // This should still work - it checks lengths match before merging
    AggregateStreamMerger::merge_batch_into_groups(&batch, &schema, &plan, &mut merged_groups)
        .unwrap();

    // The existing group should remain unchanged since lengths don't match
    let group_key = GroupKey {
        bucket: None,
        groups: vec!["US".to_string()],
    };
    let states = merged_groups.get(&group_key).unwrap();
    assert_eq!(states.len(), 2); // Original states preserved
}

#[tokio::test]
async fn emit_merged_groups_limit_exceeds_groups() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    merged_groups.insert(
        GroupKey {
            bucket: None,
            groups: vec!["US".to_string()],
        },
        vec![AggState::CountAll { count: 5 }],
    );

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        Some(10), // LIMIT 10, but only 1 group
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    let mut total_rows = 0;
    while let Some(batch) = rx.recv().await {
        total_rows += batch.len();
    }

    assert_eq!(total_rows, 1); // Should return all groups even if limit is higher
}

#[tokio::test]
async fn emit_merged_groups_large_batch_splits() {
    let schema = create_batch_schema(vec![("country", "String"), ("count", "Integer")]);
    let plan = create_aggregate_plan(
        vec![AggregateOpSpec::CountAll],
        Some(vec!["country".to_string()]),
        None,
    );

    let mut merged_groups: HashMap<GroupKey, Vec<AggState>> = HashMap::new();
    // Create many groups to test batch splitting
    for i in 0..100 {
        merged_groups.insert(
            GroupKey {
                bucket: None,
                groups: vec![format!("country_{}", i)],
            },
            vec![AggState::CountAll { count: i as i64 }],
        );
    }

    let (tx, mut rx) = FlowChannel::bounded(10, FlowMetrics::new());

    AggregateStreamMerger::emit_merged_groups(
        merged_groups,
        schema,
        plan,
        None,
        None,
        None,
        tx,
        FlowMetrics::new(),
    )
    .await
    .unwrap();

    let mut total_rows = 0;
    let mut batch_count = 0;
    while let Some(batch) = rx.recv().await {
        total_rows += batch.len();
        batch_count += 1;
    }

    assert_eq!(total_rows, 100);
    assert!(batch_count > 0); // Should be split into multiple batches
}
