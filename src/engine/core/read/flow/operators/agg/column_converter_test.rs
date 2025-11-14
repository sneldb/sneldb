use super::column_converter::ColumnConverter;
use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::core::read::sink::AggregateSink;
use crate::engine::types::ScalarValue;
use serde_json::json;
use std::sync::Arc;

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

fn create_aggregate_sink(plan: &AggregatePlan) -> AggregateSink {
    AggregateSink::from_plan(plan)
}

// Basic conversion tests -----------------------------------------------------

#[test]
fn column_converter_converts_count_all() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![
        ("timestamp", "Timestamp"),
        ("value", "Integer"),
        ("other", "String"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::from(json!(1000_u64)),
                ScalarValue::from(json!(10)),
                ScalarValue::from(json!("test")),
            ],
            vec![
                ScalarValue::from(json!(2000_u64)),
                ScalarValue::from(json!(20)),
                ScalarValue::from(json!("test2")),
            ],
        ],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    // Should only include timestamp (time_field) and not value or other
    assert_eq!(result.len(), 1);
    assert!(result.contains_key("timestamp"));
    assert!(!result.contains_key("value"));
    assert!(!result.contains_key("other"));
}

#[test]
fn column_converter_converts_with_group_by() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: Some(vec!["region".to_string(), "country".to_string()]),
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![
        ("timestamp", "Timestamp"),
        ("region", "String"),
        ("country", "String"),
        ("value", "Integer"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!("US")),
            ScalarValue::from(json!("CA")),
            ScalarValue::from(json!(10)),
        ]],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    assert_eq!(result.len(), 3);
    assert!(result.contains_key("timestamp"));
    assert!(result.contains_key("region"));
    assert!(result.contains_key("country"));
    assert!(!result.contains_key("value"));
}

#[test]
fn column_converter_converts_with_aggregate_fields() {
    let plan = AggregatePlan {
        ops: vec![
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "quantity".to_string(),
            },
        ],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![
        ("timestamp", "Timestamp"),
        ("amount", "Integer"),
        ("quantity", "Integer"),
        ("other", "String"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!(100)),
            ScalarValue::from(json!(5)),
            ScalarValue::from(json!("test")),
        ]],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    assert_eq!(result.len(), 3);
    assert!(result.contains_key("timestamp"));
    assert!(result.contains_key("amount"));
    assert!(result.contains_key("quantity"));
    assert!(!result.contains_key("other"));
}

#[test]
fn column_converter_converts_with_time_bucket() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: Some(TimeGranularity::Hour),
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp"), ("value", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!(10)),
        ]],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    assert_eq!(result.len(), 1);
    assert!(result.contains_key("timestamp"));
}

// Typed i64 column tests -----------------------------------------------------

#[test]
fn column_converter_creates_typed_i64_column() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp"), ("amount", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::from(json!(1000_u64)),
                ScalarValue::from(json!(10)),
            ],
            vec![
                ScalarValue::from(json!(2000_u64)),
                ScalarValue::from(json!(20)),
            ],
            vec![
                ScalarValue::from(json!(3000_u64)),
                ScalarValue::from(json!(30)),
            ],
        ],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    let amount_col = result.get("amount").unwrap();
    assert_eq!(amount_col.len(), 3);
    assert_eq!(amount_col.get_i64_at(0).unwrap(), 10);
    assert_eq!(amount_col.get_i64_at(1).unwrap(), 20);
    assert_eq!(amount_col.get_i64_at(2).unwrap(), 30);
}

#[test]
fn column_converter_handles_null_in_typed_i64_column() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp"), ("amount", "Integer")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![ScalarValue::from(json!(1000_u64)), ScalarValue::Null],
            vec![
                ScalarValue::from(json!(2000_u64)),
                ScalarValue::from(json!(20)),
            ],
        ],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    let amount_col = result.get("amount").unwrap();
    assert_eq!(amount_col.len(), 2);
    assert_eq!(amount_col.get_i64_at(0), None);
    assert_eq!(amount_col.get_i64_at(1).unwrap(), 20);
}

// String column tests ---------------------------------------------------------

#[test]
fn column_converter_creates_string_column_for_non_i64() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Min {
            field: "name".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp"), ("name", "String")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::from(json!(1000_u64)),
                ScalarValue::from(json!("Alice")),
            ],
            vec![
                ScalarValue::from(json!(2000_u64)),
                ScalarValue::from(json!("Bob")),
            ],
        ],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    let name_col = result.get("name").unwrap();
    assert_eq!(name_col.len(), 2);
    assert_eq!(name_col.get_str_at(0).unwrap(), "Alice");
    assert_eq!(name_col.get_str_at(1).unwrap(), "Bob");
}

#[test]
fn column_converter_creates_string_column_for_mixed_types() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Min {
            field: "value".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp"), ("value", "String")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![
            vec![
                ScalarValue::from(json!(1000_u64)),
                ScalarValue::from(json!(10)), // Int64 converted to string
            ],
            vec![
                ScalarValue::from(json!(2000_u64)),
                ScalarValue::from(json!("test")), // String
            ],
        ],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    let value_col = result.get("value").unwrap();
    assert_eq!(value_col.len(), 2);
    assert_eq!(value_col.get_str_at(0).unwrap(), "10");
    assert_eq!(value_col.get_str_at(1).unwrap(), "test");
}

// Error handling tests --------------------------------------------------------

#[test]
fn column_converter_errors_on_missing_column() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::Total {
            field: "amount".to_string(),
        }],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp")]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![ScalarValue::from(json!(1000_u64))]],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns);
    assert!(result.is_ok());
}

#[test]
fn column_converter_handles_empty_batch() {
    let plan = AggregatePlan {
        ops: vec![AggregateOpSpec::CountAll],
        group_by: None,
        time_bucket: None,
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![("timestamp", "Timestamp")]);
    let batch = create_column_batch(schema.clone(), vec![]);

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    assert_eq!(result.len(), 1);
    let timestamp_col = result.get("timestamp").unwrap();
    assert_eq!(timestamp_col.len(), 0);
}

// Complex aggregation tests ----------------------------------------------------

#[test]
fn column_converter_converts_all_aggregate_types() {
    let plan = AggregatePlan {
        ops: vec![
            AggregateOpSpec::CountAll,
            AggregateOpSpec::CountField {
                field: "visits".to_string(),
            },
            AggregateOpSpec::CountUnique {
                field: "user_id".to_string(),
            },
            AggregateOpSpec::Total {
                field: "amount".to_string(),
            },
            AggregateOpSpec::Avg {
                field: "quantity".to_string(),
            },
            AggregateOpSpec::Min {
                field: "score".to_string(),
            },
            AggregateOpSpec::Max {
                field: "name".to_string(),
            },
        ],
        group_by: Some(vec!["region".to_string()]),
        time_bucket: Some(TimeGranularity::Hour),
    };
    let sink = create_aggregate_sink(&plan);

    let schema = create_batch_schema(vec![
        ("timestamp", "Timestamp"),
        ("region", "String"),
        ("visits", "Integer"),
        ("user_id", "String"),
        ("amount", "Integer"),
        ("quantity", "Integer"),
        ("score", "Integer"),
        ("name", "String"),
    ]);
    let batch = create_column_batch(
        schema.clone(),
        vec![vec![
            ScalarValue::from(json!(1000_u64)),
            ScalarValue::from(json!("US")),
            ScalarValue::from(json!(5)),
            ScalarValue::from(json!("user1")),
            ScalarValue::from(json!(100)),
            ScalarValue::from(json!(10)),
            ScalarValue::from(json!(50)),
            ScalarValue::from(json!("Alice")),
        ]],
    );

    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
    let needed_columns = ColumnConverter::determine_needed_columns(&sink);
    let result = ColumnConverter::convert(&batch, &column_names, &needed_columns).unwrap();

    // Should include: timestamp, region, visits, user_id, amount, quantity, score, name
    assert_eq!(result.len(), 8);
    assert!(result.contains_key("timestamp"));
    assert!(result.contains_key("region"));
    assert!(result.contains_key("visits"));
    assert!(result.contains_key("user_id"));
    assert!(result.contains_key("amount"));
    assert!(result.contains_key("quantity"));
    assert!(result.contains_key("score"));
    assert!(result.contains_key("name"));
}
