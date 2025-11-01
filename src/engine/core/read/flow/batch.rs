use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use crate::engine::core::read::result::ColumnSpec;

use super::pool::BatchPoolInner;

#[derive(Debug, Clone)]
pub struct BatchSchema {
    columns: Vec<ColumnSpec>,
}

impl BatchSchema {
    pub fn new(columns: Vec<ColumnSpec>) -> Result<Self, BatchError> {
        if columns.is_empty() {
            return Err(BatchError::InvalidSchema(
                "schema must contain at least one column".into(),
            ));
        }

        Ok(Self { columns })
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    pub fn is_compatible_with(&self, other: &BatchSchema) -> bool {
        if self.column_count() != other.column_count() {
            return false;
        }

        self.columns
            .iter()
            .zip(other.columns())
            .all(|(left, right)| left.name == right.name && left.logical_type == right.logical_type)
    }
}

#[derive(Debug)]
pub struct ColumnBatch {
    schema: Arc<BatchSchema>,
    columns: Vec<Vec<Value>>,
    len: usize,
    pool: Option<Arc<BatchPoolInner>>,
}

impl ColumnBatch {
    pub(crate) fn new(
        schema: Arc<BatchSchema>,
        columns: Vec<Vec<Value>>,
        len: usize,
        pool: Option<Arc<BatchPoolInner>>,
    ) -> Result<Self, BatchError> {
        if columns.len() != schema.column_count() {
            return Err(BatchError::InvalidColumnCount {
                expected: schema.column_count(),
                got: columns.len(),
            });
        }

        for (idx, column) in columns.iter().enumerate() {
            if column.len() != len {
                return Err(BatchError::InconsistentColumnLength {
                    column: idx,
                    expected: len,
                    got: column.len(),
                });
            }
        }

        Ok(Self {
            schema,
            columns,
            len,
            pool,
        })
    }

    pub fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn column(&self, idx: usize) -> Result<&[Value], BatchError> {
        self.columns
            .get(idx)
            .map(|c| c.as_slice())
            .ok_or(BatchError::ColumnOutOfBounds(idx))
    }

    pub fn row(&self, idx: usize) -> Result<Vec<&Value>, BatchError> {
        if idx >= self.len {
            return Err(BatchError::RowOutOfBounds {
                index: idx,
                len: self.len,
            });
        }

        Ok(self.columns.iter().map(|col| &col[idx]).collect())
    }

    pub fn columns(&self) -> impl Iterator<Item = &[Value]> {
        self.columns.iter().map(|col| col.as_slice())
    }

    pub fn detach(mut self) -> (Arc<BatchSchema>, Vec<Vec<Value>>) {
        self.pool = None;
        let columns = std::mem::take(&mut self.columns);
        (Arc::clone(&self.schema), columns)
    }
}

impl Drop for ColumnBatch {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let mut columns = Vec::with_capacity(self.columns.len());
            std::mem::swap(&mut columns, &mut self.columns);
            pool.recycle(Arc::clone(&self.schema), columns);
        }
    }
}

#[derive(Debug)]
pub struct ColumnBatchBuilder {
    schema: Arc<BatchSchema>,
    columns: Vec<Vec<Value>>,
    len: usize,
    capacity: usize,
    pool: Option<Arc<BatchPoolInner>>,
}

impl ColumnBatchBuilder {
    pub(crate) fn new(
        schema: Arc<BatchSchema>,
        columns: Vec<Vec<Value>>,
        capacity: usize,
        pool: Option<Arc<BatchPoolInner>>,
    ) -> Self {
        Self {
            schema,
            columns,
            len: 0,
            capacity,
            pool,
        }
    }

    pub fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn push_row(&mut self, values: &[Value]) -> Result<(), BatchError> {
        if self.len >= self.capacity {
            return Err(BatchError::BatchFull(self.capacity));
        }

        if values.len() != self.columns.len() {
            return Err(BatchError::InvalidColumnCount {
                expected: self.columns.len(),
                got: values.len(),
            });
        }

        for (col, value) in self.columns.iter_mut().zip(values.iter()) {
            col.push(value.clone());
        }

        self.len += 1;
        Ok(())
    }

    pub fn clear(&mut self) {
        for column in &mut self.columns {
            column.clear();
        }
        self.len = 0;
    }

    pub fn finish(mut self) -> Result<ColumnBatch, BatchError> {
        let len = self.len;
        let schema = Arc::clone(&self.schema);
        let pool = self.pool.clone();

        self.pool = None;

        let mut columns = Vec::new();
        std::mem::swap(&mut columns, &mut self.columns);

        ColumnBatch::new(schema, columns, len, pool)
    }
}

impl Drop for ColumnBatchBuilder {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let schema = Arc::clone(&self.schema);
            for column in &mut self.columns {
                column.clear();
            }
            let mut columns = Vec::new();
            std::mem::swap(&mut columns, &mut self.columns);
            pool.recycle(schema, columns);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BatchError {
    InvalidSchema(String),
    InvalidPoolConfig(String),
    InvalidColumnCount {
        expected: usize,
        got: usize,
    },
    InconsistentColumnLength {
        column: usize,
        expected: usize,
        got: usize,
    },
    ColumnOutOfBounds(usize),
    RowOutOfBounds {
        index: usize,
        len: usize,
    },
    BatchFull(usize),
}

impl fmt::Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchError::InvalidSchema(msg) => write!(f, "invalid schema: {}", msg),
            BatchError::InvalidPoolConfig(msg) => write!(f, "invalid pool config: {}", msg),
            BatchError::InvalidColumnCount { expected, got } => {
                write!(
                    f,
                    "invalid column count: expected {}, got {}",
                    expected, got
                )
            }
            BatchError::InconsistentColumnLength {
                column,
                expected,
                got,
            } => write!(
                f,
                "column {} has inconsistent length: expected {}, got {}",
                column, expected, got
            ),
            BatchError::ColumnOutOfBounds(idx) => write!(f, "column index {} out of bounds", idx),
            BatchError::RowOutOfBounds { index, len } => {
                write!(f, "row index {} out of bounds (len={})", index, len)
            }
            BatchError::BatchFull(capacity) => {
                write!(f, "batch capacity {} reached", capacity)
            }
        }
    }
}

impl std::error::Error for BatchError {}
