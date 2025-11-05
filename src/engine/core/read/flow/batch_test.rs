use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    TimestampMillisecondArray,
    builder::{
        BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder,
        TimestampMillisecondBuilder,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde_json::{Value, json};

use super::{BatchError, BatchPool, BatchSchema, ColumnBatch, ColumnBatchBuilder};
use crate::engine::core::read::result::ColumnSpec;
use crate::test_helpers::factories::ColumnSpecFactory;

fn make_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "id".into(),
                logical_type: "Integer".into(),
            },
            ColumnSpec {
                name: "value".into(),
                logical_type: "String".into(),
            },
        ])
        .expect("schema should be valid"),
    )
}

fn make_multi_type_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpecFactory::integer("id"),
            ColumnSpecFactory::float("score"),
            ColumnSpecFactory::new()
                .with_name("active")
                .with_logical_type("Boolean")
                .create(),
            ColumnSpecFactory::timestamp("ts"),
            ColumnSpecFactory::string("name"),
        ])
        .expect("schema should be valid"),
    )
}

// ==================== BatchSchema Tests ====================

#[test]
fn schema_requires_columns() {
    let err = BatchSchema::new(vec![]).expect_err("empty schema should error");
    assert!(matches!(err, BatchError::InvalidSchema(_)));
}

#[test]
fn schema_creation_with_valid_columns() {
    let schema = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("valid schema should succeed");

    assert_eq!(schema.column_count(), 2);
    assert_eq!(schema.columns().len(), 2);
    assert_eq!(schema.columns()[0].name, "id");
    assert_eq!(schema.columns()[1].name, "name");
}

#[test]
fn schema_column_count() {
    let schema = BatchSchema::new(vec![
        ColumnSpecFactory::integer("col1"),
        ColumnSpecFactory::integer("col2"),
        ColumnSpecFactory::integer("col3"),
    ])
    .expect("schema should be valid");

    assert_eq!(schema.column_count(), 3);
}

#[test]
fn schema_is_compatible_with_identical_schema() {
    let schema1 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("schema should be valid");

    let schema2 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("schema should be valid");

    assert!(schema1.is_compatible_with(&schema2));
}

#[test]
fn schema_is_compatible_with_different_names() {
    let schema1 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("schema should be valid");

    let schema2 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("value"), // Different name
    ])
    .expect("schema should be valid");

    assert!(!schema1.is_compatible_with(&schema2));
}

#[test]
fn schema_is_compatible_with_different_types() {
    let schema1 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("schema should be valid");

    let schema2 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::integer("name"), // Different type
    ])
    .expect("schema should be valid");

    assert!(!schema1.is_compatible_with(&schema2));
}

#[test]
fn schema_is_compatible_with_different_column_count() {
    let schema1 = BatchSchema::new(vec![
        ColumnSpecFactory::integer("id"),
        ColumnSpecFactory::string("name"),
    ])
    .expect("schema should be valid");

    let schema2 =
        BatchSchema::new(vec![ColumnSpecFactory::integer("id")]).expect("schema should be valid");

    assert!(!schema1.is_compatible_with(&schema2));
}

// ==================== ColumnBatch Tests - JSON Creation ====================

#[test]
fn column_batch_validates_lengths() {
    let schema = make_schema();
    let columns = vec![vec![json!(1)], vec![json!("alpha"), json!("beta")]];
    let err = ColumnBatch::new(Arc::clone(&schema), columns, 1, None)
        .expect_err("length mismatch should error");

    assert!(matches!(
        err,
        BatchError::InconsistentColumnLength {
            column: 1,
            expected: 1,
            got: 2
        }
    ));
}

#[test]
fn column_batch_validates_column_count() {
    let schema = make_schema();
    let columns = vec![vec![json!(1)]]; // Only one column, schema has two
    let err = ColumnBatch::new(Arc::clone(&schema), columns, 1, None)
        .expect_err("column count mismatch should error");

    assert!(matches!(
        err,
        BatchError::InvalidColumnCount {
            expected: 2,
            got: 1
        }
    ));
}

#[test]
fn column_batch_from_json_basic() {
    let schema = make_schema();
    let columns = vec![
        vec![json!(1), json!(2), json!(3)],
        vec![json!("a"), json!("b"), json!("c")],
    ];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 3, None).expect("batch should be created");

    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
    assert_eq!(batch.schema().column_count(), 2);
}

#[test]
fn column_batch_empty_batch() {
    let schema = make_schema();
    let columns = vec![vec![], vec![]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns, 0, None)
        .expect("empty batch should be created");

    assert_eq!(batch.len(), 0);
    assert!(batch.is_empty());
}

#[test]
fn column_batch_all_data_types() {
    let schema = make_multi_type_schema();
    let columns = vec![
        vec![json!(1), json!(2)],
        vec![json!(1.5), json!(2.7)],
        vec![json!(true), json!(false)],
        vec![json!(1000), json!(2000)],
        vec![json!("a"), json!("b")],
    ];

    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 2, None).expect("batch should be created");

    assert_eq!(batch.len(), 2);

    // Test column access
    let col0 = batch.column(0).expect("column 0 should exist");
    assert_eq!(col0[0], json!(1));
    assert_eq!(col0[1], json!(2));

    let col1 = batch.column(1).expect("column 1 should exist");
    assert_eq!(col1[0], json!(1.5));

    let col2 = batch.column(2).expect("column 2 should exist");
    assert_eq!(col2[0], json!(true));
    assert_eq!(col2[1], json!(false));

    let col3 = batch.column(3).expect("column 3 should exist");
    assert_eq!(col3[0], json!(1000));

    let col4 = batch.column(4).expect("column 4 should exist");
    assert_eq!(col4[0], json!("a"));
}

#[test]
fn column_batch_row_access() {
    let schema = make_schema();
    let columns = vec![
        vec![json!(1), json!(2), json!(3)],
        vec![json!("a"), json!("b"), json!("c")],
    ];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 3, None).expect("batch should be created");

    let row0 = batch.row(0).expect("row 0 should exist");
    assert_eq!(row0[0], json!(1));
    assert_eq!(row0[1], json!("a"));

    let row1 = batch.row(1).expect("row 1 should exist");
    assert_eq!(row1[0], json!(2));
    assert_eq!(row1[1], json!("b"));

    let row2 = batch.row(2).expect("row 2 should exist");
    assert_eq!(row2[0], json!(3));
    assert_eq!(row2[1], json!("c"));
}

#[test]
fn column_batch_row_out_of_bounds() {
    let schema = make_schema();
    let columns = vec![vec![json!(1)], vec![json!("a")]];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 1, None).expect("batch should be created");

    let err = batch.row(1).expect_err("row 1 should be out of bounds");
    assert!(matches!(
        err,
        BatchError::RowOutOfBounds { index: 1, len: 1 }
    ));
}

#[test]
fn column_batch_column_out_of_bounds() {
    let schema = make_schema();
    let columns = vec![vec![json!(1)], vec![json!("a")]];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 1, None).expect("batch should be created");

    let err = batch
        .column(2)
        .expect_err("column 2 should be out of bounds");
    assert!(matches!(err, BatchError::ColumnOutOfBounds(2)));
}

#[test]
fn column_batch_lazy_json_conversion() {
    let schema = make_schema();
    let columns = vec![vec![json!(1), json!(2)], vec![json!("a"), json!("b")]];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 2, None).expect("batch should be created");

    // First access converts from Arrow
    let col0 = batch.column(0).expect("column 0 should exist");
    assert_eq!(col0.len(), 2);

    // Second access should use cache (but we can't directly verify that)
    let col0_again = batch.column(0).expect("column 0 should exist again");
    assert_eq!(col0_again, col0);
}

#[test]
fn column_batch_columns_caches_result() {
    let schema = make_schema();
    let columns = vec![vec![json!(1), json!(2)], vec![json!("a"), json!("b")]];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 2, None).expect("batch should be created");

    let all_columns1 = batch.columns();
    let all_columns2 = batch.columns();

    assert_eq!(all_columns1.len(), 2);
    assert_eq!(all_columns2.len(), 2);
    assert_eq!(all_columns1[0], all_columns2[0]);
    assert_eq!(all_columns1[1], all_columns2[1]);
}

#[test]
fn column_batch_detach() {
    let schema = make_schema();
    let columns = vec![vec![json!(1), json!(2)], vec![json!("a"), json!("b")]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 2, None)
        .expect("batch should be created");

    let (detached_schema, detached_columns) = batch.detach();

    assert_eq!(detached_schema.column_count(), 2);
    assert_eq!(detached_columns.len(), 2);
    assert_eq!(detached_columns[0], columns[0]);
    assert_eq!(detached_columns[1], columns[1]);
}

#[test]
fn column_batch_record_batch_access() {
    let schema = make_schema();
    let columns = vec![vec![json!(1), json!(2)], vec![json!("a"), json!("b")]];
    let batch =
        ColumnBatch::new(Arc::clone(&schema), columns, 2, None).expect("batch should be created");

    let record_batch = batch.record_batch();
    assert_eq!(record_batch.num_rows(), 2);
    assert_eq!(record_batch.num_columns(), 2);
}

// ==================== ColumnBatch Tests - Arrow Creation ====================

fn create_test_record_batch(schema: &Arc<BatchSchema>) -> RecordBatch {
    use crate::shared::response::arrow::build_arrow_schema;

    let arrow_schema = build_arrow_schema(schema).expect("arrow schema should build");
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col_spec in schema.columns() {
        let array: ArrayRef = match col_spec.logical_type.as_str() {
            "Integer" => {
                let mut builder = Int64Builder::with_capacity(2);
                builder.append_value(10);
                builder.append_value(20);
                Arc::new(builder.finish())
            }
            "String" => {
                let mut builder = LargeStringBuilder::with_capacity(2, 16);
                builder.append_value("x");
                builder.append_value("y");
                Arc::new(builder.finish())
            }
            "Float" => {
                let mut builder = Float64Builder::with_capacity(2);
                builder.append_value(1.5);
                builder.append_value(2.5);
                Arc::new(builder.finish())
            }
            "Boolean" => {
                let mut builder = BooleanBuilder::with_capacity(2);
                builder.append_value(true);
                builder.append_value(false);
                Arc::new(builder.finish())
            }
            "Timestamp" => {
                let mut builder = TimestampMillisecondBuilder::with_capacity(2);
                builder.append_value(1000);
                builder.append_value(2000);
                Arc::new(builder.finish())
            }
            _ => {
                let mut builder = LargeStringBuilder::with_capacity(2, 16);
                builder.append_value("default");
                builder.append_value("default");
                Arc::new(builder.finish())
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema, arrays).expect("record batch should be created")
}

#[test]
fn column_batch_from_record_batch() {
    let schema = make_schema();
    let record_batch = create_test_record_batch(&schema);

    let batch = ColumnBatch::from_record_batch(Arc::clone(&schema), record_batch, None)
        .expect("batch should be created from record batch");

    assert_eq!(batch.len(), 2);
    assert_eq!(batch.schema().column_count(), 2);

    // Test that we can access columns (lazy conversion)
    let col0 = batch.column(0).expect("column 0 should exist");
    assert_eq!(col0[0], json!(10));
    assert_eq!(col0[1], json!(20));

    let col1 = batch.column(1).expect("column 1 should exist");
    assert_eq!(col1[0], json!("x"));
    assert_eq!(col1[1], json!("y"));
}

#[test]
fn column_batch_from_record_batch_column_count_mismatch() {
    let schema = make_schema(); // 2 columns
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let record_batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
    )
    .expect("record batch should be created");

    let err = ColumnBatch::from_record_batch(Arc::clone(&schema), record_batch, None)
        .expect_err("column count mismatch should error");

    assert!(matches!(
        err,
        BatchError::InvalidColumnCount {
            expected: 2,
            got: 1
        }
    ));
}

#[test]
fn column_batch_from_record_batch_all_types() {
    let schema = make_multi_type_schema();
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("name", DataType::LargeUtf8, true),
    ]));

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Float64Array::from(vec![1.5, 2.5])),
        Arc::new(BooleanArray::from(vec![true, false])),
        Arc::new(TimestampMillisecondArray::from(vec![1000, 2000])),
        Arc::new(LargeStringArray::from(vec!["a", "b"])),
    ];

    let record_batch =
        RecordBatch::try_new(arrow_schema, arrays).expect("record batch should be created");

    let batch = ColumnBatch::from_record_batch(Arc::clone(&schema), record_batch, None)
        .expect("batch should be created");

    assert_eq!(batch.len(), 2);

    // Test all columns
    assert_eq!(batch.column(0).expect("col 0")[0], json!(1));
    assert_eq!(batch.column(1).expect("col 1")[0], json!(1.5));
    assert_eq!(batch.column(2).expect("col 2")[0], json!(true));
    assert_eq!(batch.column(3).expect("col 3")[0], json!(1000));
    assert_eq!(batch.column(4).expect("col 4")[0], json!("a"));
}

#[test]
fn column_batch_from_record_batch_with_nulls() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpecFactory::integer("id")]).expect("schema should be valid"),
    );
    let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

    let mut builder = Int64Builder::with_capacity(3);
    builder.append_value(1);
    builder.append_null();
    builder.append_value(3);
    let array = Arc::new(builder.finish());

    let record_batch =
        RecordBatch::try_new(arrow_schema, vec![array]).expect("record batch should be created");

    let batch = ColumnBatch::from_record_batch(Arc::clone(&schema), record_batch, None)
        .expect("batch should be created");

    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!(1));
    assert_eq!(col[1], Value::Null);
    assert_eq!(col[2], json!(3));

    let row1 = batch.row(1).expect("row 1 should exist");
    assert_eq!(row1[0], Value::Null);
}

// ==================== ColumnBatchBuilder Tests ====================

#[test]
fn builder_push_row_and_finalize() {
    let schema = make_schema();
    let pool = BatchPool::new(4).expect("pool allocates");
    let mut builder = pool.acquire(Arc::clone(&schema));

    builder
        .push_row(&[json!(7_i64), json!("seven")])
        .expect("first row stores");
    builder
        .push_row(&[json!(42_i64), json!("forty-two")])
        .expect("second row stores");

    assert_eq!(builder.len(), 2);
    assert!(!builder.is_full());

    let batch = builder.finish().expect("finish succeeds");
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.schema().column_count(), 2);

    let row = batch.row(1).expect("row available");
    assert_eq!(row[0], json!(42_i64));
    assert_eq!(row[1], json!("forty-two"));
}

#[test]
fn builder_respects_capacity() {
    let schema = make_schema();
    let mut builder = ColumnBatchBuilder::new(
        Arc::clone(&schema),
        vec![Vec::with_capacity(1), Vec::with_capacity(1)],
        1,
        None,
    );

    builder
        .push_row(&[json!(1_i64), json!("one")])
        .expect("first row stores");
    let err = builder
        .push_row(&[json!(2_i64), json!("two")])
        .expect_err("capacity exceeded");

    assert!(matches!(err, BatchError::BatchFull(1)));
}

#[test]
fn builder_push_row_validates_column_count() {
    let schema = make_schema();
    let mut builder =
        ColumnBatchBuilder::new(Arc::clone(&schema), vec![Vec::new(), Vec::new()], 10, None);

    let err = builder
        .push_row(&[json!(1)]) // Only 1 value, but schema has 2 columns
        .expect_err("column count mismatch should error");

    assert!(matches!(
        err,
        BatchError::InvalidColumnCount {
            expected: 2,
            got: 1
        }
    ));
}

#[test]
fn builder_clear() {
    let schema = make_schema();
    let mut builder =
        ColumnBatchBuilder::new(Arc::clone(&schema), vec![Vec::new(), Vec::new()], 10, None);

    builder
        .push_row(&[json!(1), json!("a")])
        .expect("row should be added");
    assert_eq!(builder.len(), 1);

    builder.clear();
    assert_eq!(builder.len(), 0);
    assert!(!builder.is_full());

    // Should be able to push again after clear
    builder
        .push_row(&[json!(2), json!("b")])
        .expect("row should be added after clear");
    assert_eq!(builder.len(), 1);
}

#[test]
fn builder_schema_access() {
    let schema = make_schema();
    let builder =
        ColumnBatchBuilder::new(Arc::clone(&schema), vec![Vec::new(), Vec::new()], 10, None);

    assert_eq!(builder.schema().column_count(), 2);
    assert_eq!(builder.capacity(), 10);
    assert_eq!(builder.len(), 0);
    assert!(!builder.is_full());
}

#[test]
fn builder_finish_with_empty_batch() {
    let schema = make_schema();
    let builder =
        ColumnBatchBuilder::new(Arc::clone(&schema), vec![Vec::new(), Vec::new()], 10, None);

    let batch = builder.finish().expect("finish should succeed");
    assert_eq!(batch.len(), 0);
    assert!(batch.is_empty());
}

// ==================== Data Type Conversion Tests ====================

#[test]
fn column_batch_int64_conversion_from_string() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpecFactory::integer("id")]).expect("schema should be valid"),
    );
    // String values are parsed when creating Arrow arrays, but cached JSON preserves original format
    // When created from JSON, the original JSON values are cached and returned
    let columns = vec![vec![json!("123"), json!("456")]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 2, None)
        .expect("batch should be created");

    // The cached JSON values preserve the original string format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!("123"));
    assert_eq!(col[1], json!("456"));

    // But the RecordBatch contains the converted numeric values
    let record_batch = batch.record_batch();
    let int_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("should be Int64Array");
    assert_eq!(int_array.value(0), 123);
    assert_eq!(int_array.value(1), 456);
}

#[test]
fn column_batch_float64_conversion_from_string() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpecFactory::float("score")]).expect("schema should be valid"),
    );
    let columns = vec![vec![json!("1.5"), json!("2.7")]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 2, None)
        .expect("batch should be created");

    // Cached JSON preserves original format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!("1.5"));
    assert_eq!(col[1], json!("2.7"));

    // But RecordBatch contains converted float values
    let record_batch = batch.record_batch();
    let float_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .expect("should be Float64Array");
    assert_eq!(float_array.value(0), 1.5);
    assert_eq!(float_array.value(1), 2.7);
}

#[test]
fn column_batch_boolean_conversion_from_string() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpecFactory::new()
                .with_name("active")
                .with_logical_type("Boolean")
                .create(),
        ])
        .expect("schema should be valid"),
    );
    let columns = vec![vec![json!("true"), json!("false"), json!("1"), json!("0")]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 4, None)
        .expect("batch should be created");

    // Cached JSON preserves original format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!("true"));
    assert_eq!(col[1], json!("false"));
    assert_eq!(col[2], json!("1"));
    assert_eq!(col[3], json!("0"));

    // But RecordBatch contains converted boolean values
    let record_batch = batch.record_batch();
    let bool_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .expect("should be BooleanArray");
    assert_eq!(bool_array.value(0), true);
    assert_eq!(bool_array.value(1), false);
    assert_eq!(bool_array.value(2), true); // "1" -> true
    assert_eq!(bool_array.value(3), false); // "0" -> false
}

#[test]
fn column_batch_boolean_conversion_from_number() {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpecFactory::new()
                .with_name("active")
                .with_logical_type("Boolean")
                .create(),
        ])
        .expect("schema should be valid"),
    );
    let columns = vec![vec![json!(1), json!(0), json!(42)]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 3, None)
        .expect("batch should be created");

    // Cached JSON preserves original number format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!(1));
    assert_eq!(col[1], json!(0));
    assert_eq!(col[2], json!(42));

    // But RecordBatch contains converted boolean values
    let record_batch = batch.record_batch();
    let bool_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .expect("should be BooleanArray");
    assert_eq!(bool_array.value(0), true); // 1 -> true
    assert_eq!(bool_array.value(1), false); // 0 -> false
    assert_eq!(bool_array.value(2), true); // 42 -> true (non-zero)
}

#[test]
fn column_batch_timestamp_conversion_from_string() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpecFactory::timestamp("ts")]).expect("schema should be valid"),
    );
    let columns = vec![vec![json!("1000"), json!("2000")]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 2, None)
        .expect("batch should be created");

    // Cached JSON preserves original string format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!("1000"));
    assert_eq!(col[1], json!("2000"));

    // But RecordBatch contains converted timestamp values
    let record_batch = batch.record_batch();
    let ts_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
        .expect("should be TimestampMillisecondArray");
    assert_eq!(ts_array.value(0), 1000);
    assert_eq!(ts_array.value(1), 2000);
}

#[test]
fn column_batch_string_conversion_from_other_types() {
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpecFactory::string("value")]).expect("schema should be valid"),
    );
    // String arrays convert other types to strings when creating Arrow arrays
    let columns = vec![vec![json!(123), json!(true), json!(null)]];
    let batch = ColumnBatch::new(Arc::clone(&schema), columns.clone(), 3, None)
        .expect("batch should be created");

    // Cached JSON preserves original format
    let col = batch.column(0).expect("column should exist");
    assert_eq!(col[0], json!(123));
    assert_eq!(col[1], json!(true));
    assert_eq!(col[2], Value::Null);

    // But RecordBatch contains string values
    let record_batch = batch.record_batch();
    let string_array = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::LargeStringArray>()
        .expect("should be LargeStringArray");
    assert_eq!(string_array.value(0), "123");
    assert_eq!(string_array.value(1), "true");
    assert_eq!(record_batch.column(0).is_null(2), true);
}

// ==================== Error Display Tests ====================

#[test]
fn batch_error_display_messages() {
    let err1 = BatchError::InvalidSchema("test".into());
    assert!(err1.to_string().contains("invalid schema"));

    let err2 = BatchError::InvalidColumnCount {
        expected: 2,
        got: 1,
    };
    assert!(err2.to_string().contains("invalid column count"));

    let err3 = BatchError::InconsistentColumnLength {
        column: 0,
        expected: 2,
        got: 1,
    };
    assert!(err3.to_string().contains("inconsistent length"));

    let err4 = BatchError::ColumnOutOfBounds(5);
    assert!(err4.to_string().contains("column index 5"));

    let err5 = BatchError::RowOutOfBounds { index: 10, len: 5 };
    assert!(err5.to_string().contains("row index 10"));

    let err6 = BatchError::BatchFull(100);
    assert!(err6.to_string().contains("batch capacity 100"));
}
