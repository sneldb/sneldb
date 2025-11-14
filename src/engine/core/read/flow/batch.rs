use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

use crate::engine::core::read::result::ColumnSpec;
use crate::engine::types::ScalarValue;
use crate::shared::response::arrow::build_arrow_schema;

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
    // Store ScalarValue directly - no Arrow conversion overhead in pipeline
    columns: Vec<Vec<ScalarValue>>,
    pool: Option<Arc<BatchPoolInner>>,
}

impl ColumnBatch {
    /// Create a ColumnBatch from Arrow RecordBatch (converts to ScalarValue)
    /// This is used when data comes from Arrow format (e.g., materialized frames)
    #[allow(dead_code)] // Used in tests
    pub(crate) fn from_record_batch(
        schema: Arc<BatchSchema>,
        record_batch: RecordBatch,
        pool: Option<Arc<BatchPoolInner>>,
    ) -> Result<Self, BatchError> {
        if record_batch.num_columns() != schema.column_count() {
            return Err(BatchError::InvalidColumnCount {
                expected: schema.column_count(),
                got: record_batch.num_columns(),
            });
        }

        // Convert Arrow → ScalarValue (only when needed, e.g., from materialized storage)
        let mut columns = Vec::with_capacity(record_batch.num_columns());
        for col_idx in 0..record_batch.num_columns() {
            let array = record_batch.column(col_idx);
            let column_spec = &schema.columns()[col_idx];
            let values = array_to_scalar_values(array, column_spec.logical_type.as_str());
            columns.push(values);
        }

        Ok(Self {
            schema,
            columns,
            pool,
        })
    }

    /// Create a ColumnBatch from ScalarValues (no conversion - direct storage)
    pub(crate) fn new(
        schema: Arc<BatchSchema>,
        columns: Vec<Vec<ScalarValue>>,
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

        // No conversion - just store ScalarValues directly!
        Ok(Self {
            schema,
            columns,
            pool,
        })
    }

    /// Convert to Arrow RecordBatch on-demand (for Arrow output format only)
    /// This is the only place where ScalarValue → Arrow conversion happens
    pub fn to_record_batch(&self) -> Result<RecordBatch, BatchError> {
        let arrow_schema = build_arrow_schema(&self.schema).map_err(|e| {
            BatchError::InvalidSchema(format!("Failed to build Arrow schema: {}", e))
        })?;

        let mut arrays = Vec::with_capacity(self.schema.column_count());
        for (col_idx, column_spec) in self.schema.columns().iter().enumerate() {
            let data_type = logical_to_arrow_type(column_spec.logical_type.as_str());
            let values = &self.columns[col_idx];

            let array = match data_type {
                DataType::Int64 => build_int64_array_from_scalars(values),
                DataType::Float64 => build_float64_array_from_scalars(values),
                DataType::Boolean => build_bool_array_from_scalars(values),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    build_timestamp_array_from_scalars(values)
                }
                DataType::LargeUtf8 => build_string_array_from_scalars(values),
                _ => build_string_array_from_scalars(values),
            };
            arrays.push(array);
        }

        RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| BatchError::InvalidSchema(format!("Failed to create RecordBatch: {}", e)))
    }

    /// Get the Arrow RecordBatch (for backward compatibility)
    /// Converts on-demand - use to_record_batch() for better error handling
    pub fn record_batch(&self) -> RecordBatch {
        self.to_record_batch()
            .expect("Failed to convert to RecordBatch")
    }

    pub fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a column as ScalarValues (direct access - no conversion)
    pub fn column(&self, idx: usize) -> Result<Vec<ScalarValue>, BatchError> {
        if idx >= self.columns.len() {
            return Err(BatchError::ColumnOutOfBounds(idx));
        }
        Ok(self.columns[idx].clone())
    }

    /// Get a row as ScalarValues (direct access - no conversion)
    pub fn row(&self, idx: usize) -> Result<Vec<ScalarValue>, BatchError> {
        let len = self.len();
        if idx >= len {
            return Err(BatchError::RowOutOfBounds { index: idx, len });
        }

        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            row.push(column.get(idx).cloned().unwrap_or(ScalarValue::Null));
        }
        Ok(row)
    }

    /// Get all columns as ScalarValues (direct access - no conversion)
    pub fn columns(&self) -> Vec<Vec<ScalarValue>> {
        self.columns.clone()
    }

    /// Get a reference to the internal columns (for efficient access)
    pub fn columns_ref(&self) -> &[Vec<ScalarValue>] {
        &self.columns
    }

    // Note: columns_iter() removed - use columns() and convert to slices as needed
    // The iterator pattern is incompatible with owned Vec return from columns()

    pub fn detach(mut self) -> (Arc<BatchSchema>, Vec<Vec<ScalarValue>>) {
        self.pool = None;
        (Arc::clone(&self.schema), self.columns.clone())
    }
}

impl Drop for ColumnBatch {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            // Direct access - no conversion needed
            pool.recycle(Arc::clone(&self.schema), self.columns.clone());
        }
    }
}

#[derive(Debug)]
pub struct ColumnBatchBuilder {
    schema: Arc<BatchSchema>,
    columns: Vec<Vec<ScalarValue>>,
    len: usize,
    capacity: usize,
    pool: Option<Arc<BatchPoolInner>>,
}

impl ColumnBatchBuilder {
    pub(crate) fn new(
        schema: Arc<BatchSchema>,
        columns: Vec<Vec<ScalarValue>>,
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

    pub fn push_row(&mut self, values: &[ScalarValue]) -> Result<(), BatchError> {
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

// Helper functions for Arrow <-> JSON conversion

fn logical_to_arrow_type(logical_type: &str) -> DataType {
    match logical_type {
        "Integer" | "Number" => DataType::Int64,
        "Float" => DataType::Float64,
        "Boolean" => DataType::Boolean,
        "Timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "String" => DataType::LargeUtf8,
        "JSON" | "Object" | "Array" => DataType::LargeUtf8,
        other if other.starts_with("UInt") => DataType::Int64,
        _ => DataType::LargeUtf8,
    }
}

// Convert ScalarValues to Arrow arrays (used when creating from ScalarValues)
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, TimestampMillisecondBuilder,
};

fn build_int64_array_from_scalars(values: &[ScalarValue]) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Int64(i) => builder.append_value(*i),
            ScalarValue::Timestamp(t) => builder.append_value(*t),
            ScalarValue::Utf8(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    builder.append_value(i);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_float64_array_from_scalars(values: &[ScalarValue]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Float64(f) => builder.append_value(*f),
            ScalarValue::Utf8(s) => {
                if let Ok(f) = s.parse::<f64>() {
                    builder.append_value(f);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_bool_array_from_scalars(values: &[ScalarValue]) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Boolean(b) => builder.append_value(*b),
            ScalarValue::Utf8(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => builder.append_value(true),
                "false" | "0" => builder.append_value(false),
                _ => builder.append_null(),
            },
            ScalarValue::Int64(i) => builder.append_value(*i != 0),
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_timestamp_array_from_scalars(values: &[ScalarValue]) -> ArrayRef {
    let mut builder = TimestampMillisecondBuilder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Timestamp(t) => builder.append_value(*t),
            ScalarValue::Int64(i) => builder.append_value(*i),
            ScalarValue::Utf8(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    builder.append_value(i);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_string_array_from_scalars(values: &[ScalarValue]) -> ArrayRef {
    let mut builder = LargeStringBuilder::with_capacity(values.len(), values.len() * 8);
    for value in values {
        match value {
            ScalarValue::Null => builder.append_null(),
            ScalarValue::Utf8(s) => builder.append_value(s),
            other => {
                let string = other.to_string_repr();
                builder.append_value(string.as_str());
            }
        }
    }
    Arc::new(builder.finish())
}

// Convert Arrow arrays to ScalarValues (used for lazy conversion)
#[allow(dead_code)] // Used by from_record_batch which is test-only
fn array_to_scalar_values(array: &ArrayRef, logical_type: &str) -> Vec<ScalarValue> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);

    match logical_type {
        "Integer" | "Number" => {
            if let Some(int_array) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                for i in 0..len {
                    if array.is_null(i) {
                        values.push(ScalarValue::Null);
                    } else {
                        values.push(ScalarValue::Int64(int_array.value(i)));
                    }
                }
            } else {
                // Fallback: convert to string
                for i in 0..len {
                    let val = array_scalar_at(array, i, logical_type).unwrap_or(ScalarValue::Null);
                    values.push(val);
                }
            }
        }
        "Float" => {
            if let Some(float_array) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                for i in 0..len {
                    if array.is_null(i) {
                        values.push(ScalarValue::Null);
                    } else {
                        values.push(ScalarValue::Float64(float_array.value(i)));
                    }
                }
            } else {
                for i in 0..len {
                    let val = array_scalar_at(array, i, logical_type).unwrap_or(ScalarValue::Null);
                    values.push(val);
                }
            }
        }
        "Boolean" => {
            if let Some(bool_array) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                for i in 0..len {
                    if array.is_null(i) {
                        values.push(ScalarValue::Null);
                    } else {
                        values.push(ScalarValue::Boolean(bool_array.value(i)));
                    }
                }
            } else {
                for i in 0..len {
                    let val = array_scalar_at(array, i, logical_type).unwrap_or(ScalarValue::Null);
                    values.push(val);
                }
            }
        }
        "Timestamp" => {
            if let Some(ts_array) = array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                for i in 0..len {
                    if array.is_null(i) {
                        values.push(ScalarValue::Null);
                    } else {
                        // Store as Int64 for consistency with json!(value) which creates Int64
                        // This allows tests comparing ScalarValue::from(json!(ts)) to work correctly
                        values.push(ScalarValue::Int64(ts_array.value(i)));
                    }
                }
            } else {
                for i in 0..len {
                    let val = array_scalar_at(array, i, logical_type).unwrap_or(ScalarValue::Null);
                    values.push(val);
                }
            }
        }
        _ => {
            // String or other types
            if let Some(string_array) = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
            {
                for i in 0..len {
                    if array.is_null(i) {
                        values.push(ScalarValue::Null);
                    } else {
                        values.push(ScalarValue::Utf8(string_array.value(i).to_string()));
                    }
                }
            } else {
                for i in 0..len {
                    let val = array_scalar_at(array, i, logical_type).unwrap_or(ScalarValue::Null);
                    values.push(val);
                }
            }
        }
    }

    values
}

#[allow(dead_code)] // Used by array_to_scalar_values which is test-only
fn array_scalar_at(array: &ArrayRef, idx: usize, logical_type: &str) -> Option<ScalarValue> {
    if array.is_null(idx) {
        return Some(ScalarValue::Null);
    }

    match logical_type {
        "Integer" | "Number" => {
            if let Some(int_array) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                Some(ScalarValue::Int64(int_array.value(idx)))
            } else {
                None
            }
        }
        "Float" => {
            if let Some(float_array) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                Some(ScalarValue::Float64(float_array.value(idx)))
            } else {
                None
            }
        }
        "Boolean" => {
            if let Some(bool_array) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                Some(ScalarValue::Boolean(bool_array.value(idx)))
            } else {
                None
            }
        }
        "Timestamp" => {
            if let Some(ts_array) = array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                // Store as Int64 for consistency with json!(value) which creates Int64
                Some(ScalarValue::Int64(ts_array.value(idx)))
            } else {
                None
            }
        }
        _ => {
            if let Some(string_array) = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
            {
                Some(ScalarValue::Utf8(string_array.value(idx).to_string()))
            } else {
                None
            }
        }
    }
}
