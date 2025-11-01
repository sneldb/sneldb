use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use super::batch::{BatchError, BatchSchema, ColumnBatchBuilder};

#[derive(Debug, Clone)]
pub struct BatchPool {
    inner: Arc<BatchPoolInner>,
}

impl BatchPool {
    pub fn new(batch_size: usize) -> Result<Self, BatchError> {
        if batch_size == 0 {
            return Err(BatchError::InvalidPoolConfig(
                "batch size must be greater than zero".into(),
            ));
        }

        Ok(Self {
            inner: Arc::new(BatchPoolInner::new(batch_size)),
        })
    }

    pub fn batch_size(&self) -> usize {
        self.inner.batch_size
    }

    pub fn acquire(&self, schema: Arc<BatchSchema>) -> ColumnBatchBuilder {
        let mut guard = self.inner.free.lock().expect("batch pool mutex poisoned");

        if let Some(idx) = guard
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.schema, &schema))
        {
            let mut reusable = guard
                .remove(idx)
                .expect("batch pool entry should exist for schema");
            for column in &mut reusable.columns {
                column.clear();
                column.reserve(self.inner.batch_size);
            }

            ColumnBatchBuilder::new(
                schema,
                reusable.columns,
                self.inner.batch_size,
                Some(Arc::clone(&self.inner)),
            )
        } else {
            let mut columns = Vec::with_capacity(schema.column_count());
            for _ in 0..schema.column_count() {
                columns.push(Vec::with_capacity(self.inner.batch_size));
            }

            ColumnBatchBuilder::new(
                schema,
                columns,
                self.inner.batch_size,
                Some(Arc::clone(&self.inner)),
            )
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchPoolInner {
    pub(crate) batch_size: usize,
    free: Mutex<VecDeque<ReusableColumns>>,
}

impl BatchPoolInner {
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            free: Mutex::new(VecDeque::new()),
        }
    }

    pub(crate) fn recycle(&self, schema: Arc<BatchSchema>, mut columns: Vec<Vec<Value>>) {
        for column in &mut columns {
            column.clear();
            column.shrink_to(self.batch_size);
        }

        let reusable = ReusableColumns { schema, columns };

        let mut guard = self.free.lock().expect("batch pool mutex poisoned");
        guard.push_back(reusable);
    }
}

#[derive(Debug)]
struct ReusableColumns {
    schema: Arc<BatchSchema>,
    columns: Vec<Vec<Value>>,
}
