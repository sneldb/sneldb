use std::sync::Arc;

use serde_json::json;

use super::{BatchError, BatchPool, BatchSchema};
use crate::engine::core::read::result::ColumnSpec;

fn make_schema(column_count: usize) -> Arc<BatchSchema> {
    let columns = (0..column_count)
        .map(|idx| ColumnSpec {
            name: format!("col{}", idx),
            logical_type: "Integer".into(),
        })
        .collect();
    Arc::new(BatchSchema::new(columns).expect("schema builds"))
}

#[test]
fn pool_rejects_zero_batch_size() {
    let err = BatchPool::new(0).expect_err("zero batch size should error");
    assert!(matches!(err, BatchError::InvalidPoolConfig(_)));
}

#[test]
fn pool_recycles_builders() {
    let schema = make_schema(1);
    let pool = BatchPool::new(2).expect("pool builds");

    {
        let mut builder = pool.acquire(Arc::clone(&schema));
        builder.push_row(&[json!(1_i64)]).expect("row stored");
        let batch = builder.finish().expect("batch builds");
        assert_eq!(batch.len(), 1);
        drop(batch);
    }

    let mut builder = pool.acquire(schema);
    assert_eq!(builder.len(), 0);
    assert_eq!(builder.capacity(), 2);
    builder
        .push_row(&[json!(2_i64)])
        .expect("reused builder still works");
}
