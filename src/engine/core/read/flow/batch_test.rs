use std::sync::Arc;

use serde_json::json;

use super::{BatchError, BatchPool, BatchSchema, ColumnBatch, ColumnBatchBuilder};
use crate::engine::core::read::result::ColumnSpec;

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

#[test]
fn schema_requires_columns() {
    let err = BatchSchema::new(vec![]).expect_err("empty schema should error");
    assert!(matches!(err, BatchError::InvalidSchema(_)));
}

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
    assert_eq!(row[0], &json!(42_i64));
    assert_eq!(row[1], &json!("forty-two"));
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
