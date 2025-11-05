use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use serde_json::{Number, Value};

use crate::engine::core::read::result::ColumnSpec;
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
    // Store Arrow RecordBatch as the primary internal format for performance
    record_batch: RecordBatch,
    // Lazy JSON conversion cache (None until first access, thread-safe)
    json_columns: Arc<Mutex<Option<Vec<Vec<Value>>>>>,
    pool: Option<Arc<BatchPoolInner>>,
}

impl ColumnBatch {
    /// Create a ColumnBatch from Arrow RecordBatch (preferred, zero conversion)
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

        Ok(Self {
            schema,
            record_batch,
            json_columns: Arc::new(Mutex::new(None)),
            pool,
        })
    }

    /// Create a ColumnBatch from JSON Values (backward compatibility, converts to Arrow internally)
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

        // Convert JSON Values to Arrow RecordBatch
        let arrow_schema = build_arrow_schema(&schema).map_err(|e| {
            BatchError::InvalidSchema(format!("Failed to build Arrow schema: {}", e))
        })?;

        let mut arrays = Vec::with_capacity(schema.column_count());
        for (col_idx, column_spec) in schema.columns().iter().enumerate() {
            let data_type = logical_to_arrow_type(column_spec.logical_type.as_str());
            let values = &columns[col_idx];

            let array = match data_type {
                DataType::Int64 => build_int64_array_from_values(values),
                DataType::Float64 => build_float64_array_from_values(values),
                DataType::Boolean => build_bool_array_from_values(values),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    build_timestamp_array_from_values(values)
                }
                DataType::LargeUtf8 => build_string_array_from_values(values),
                _ => build_string_array_from_values(values),
            };
            arrays.push(array);
        }

        let record_batch = RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
            BatchError::InvalidSchema(format!("Failed to create RecordBatch: {}", e))
        })?;

        Ok(Self {
            schema,
            record_batch,
            json_columns: Arc::new(Mutex::new(Some(columns))), // Cache the JSON for pool recycling
            pool,
        })
    }

    /// Get the Arrow RecordBatch directly (zero conversion)
    pub fn record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    pub fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.record_batch.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.record_batch.num_rows() == 0
    }

    /// Get a column as JSON Values (lazy conversion from Arrow)
    /// Returns a Vec that can be borrowed as &[Value]
    pub fn column(&self, idx: usize) -> Result<Vec<Value>, BatchError> {
        if idx >= self.record_batch.num_columns() {
            return Err(BatchError::ColumnOutOfBounds(idx));
        }

        // Check cache first - only convert this specific column if not cached
        {
            let cached = self.json_columns.lock().unwrap();
            if let Some(ref columns) = *cached {
                if idx < columns.len() && columns[idx].len() == self.record_batch.num_rows() {
                    return Ok(columns[idx].clone());
                }
            }
        }

        // Convert only this column from Arrow (not all columns!)
        let array = self.record_batch.column(idx);
        let column_spec = self
            .schema
            .columns()
            .get(idx)
            .ok_or(BatchError::ColumnOutOfBounds(idx))?;
        let values = array_to_json_values(array, column_spec.logical_type.as_str());

        // Update cache - initialize if needed, then cache this column
        {
            let mut cached = self.json_columns.lock().unwrap();
            // Initialize cache if None
            if cached.is_none() {
                *cached = Some(Vec::with_capacity(self.record_batch.num_columns()));
            }
            if let Some(ref mut columns) = *cached {
                // Ensure cache has enough columns
                while columns.len() <= idx {
                    columns.push(Vec::new());
                }
                // Cache this column
                columns[idx] = values.clone();
            }
        }

        Ok(values)
    }

    /// Get a row as JSON Values (lazy conversion from Arrow)
    pub fn row(&self, idx: usize) -> Result<Vec<Value>, BatchError> {
        if idx >= self.record_batch.num_rows() {
            return Err(BatchError::RowOutOfBounds {
                index: idx,
                len: self.record_batch.num_rows(),
            });
        }

        let mut row = Vec::with_capacity(self.record_batch.num_columns());
        for col_idx in 0..self.record_batch.num_columns() {
            let array = self.record_batch.column(col_idx);
            let column_spec = &self.schema.columns()[col_idx];
            let value = array_value_at(array, idx, column_spec.logical_type.as_str())
                .unwrap_or(Value::Null);
            row.push(value);
        }
        Ok(row)
    }

    /// Get all columns as JSON Values (lazy conversion, caches result)
    pub fn columns(&self) -> Vec<Vec<Value>> {
        // Check cache first
        {
            let cached = self.json_columns.lock().unwrap();
            if let Some(ref columns) = *cached {
                if columns.len() == self.record_batch.num_columns() {
                    return columns.clone();
                }
            }
        }

        // Convert all columns from Arrow
        let mut columns = Vec::with_capacity(self.record_batch.num_columns());
        for col_idx in 0..self.record_batch.num_columns() {
            let array = self.record_batch.column(col_idx);
            let column_spec = &self.schema.columns()[col_idx];
            let values = array_to_json_values(array, column_spec.logical_type.as_str());
            columns.push(values);
        }

        // Cache the result
        {
            let mut cached = self.json_columns.lock().unwrap();
            *cached = Some(columns.clone());
        }

        columns
    }

    // Note: columns_iter() removed - use columns() and convert to slices as needed
    // The iterator pattern is incompatible with owned Vec return from columns()

    pub fn detach(mut self) -> (Arc<BatchSchema>, Vec<Vec<Value>>) {
        self.pool = None;
        let columns = self.columns(); // Convert to JSON
        (Arc::clone(&self.schema), columns)
    }
}

impl Drop for ColumnBatch {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            // Convert to JSON for pool recycling (if needed)
            let columns = {
                let mut cached = self.json_columns.lock().unwrap();
                cached.take().unwrap_or_else(|| self.columns())
            };
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

// Convert JSON Values to Arrow arrays (used when creating from JSON)
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, TimestampMillisecondBuilder,
};

fn build_int64_array_from_values(values: &[Value]) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Number(num) => {
                // Fast path: direct number extraction (most common case)
                if let Some(i) = num.as_i64() {
                    builder.append_value(i);
                } else if let Some(u) = num.as_u64() {
                    builder.append_value(u as i64);
                } else {
                    builder.append_null();
                }
            }
            Value::String(s) => {
                // Slow path: string parsing (rare)
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

fn build_float64_array_from_values(values: &[Value]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Number(num) => {
                // Fast path: direct number extraction (most common case)
                if let Some(f) = num.as_f64() {
                    builder.append_value(f);
                } else {
                    builder.append_null();
                }
            }
            Value::String(s) => {
                // Slow path: string parsing (rare)
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

fn build_bool_array_from_values(values: &[Value]) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Bool(b) => builder.append_value(*b),
            Value::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => builder.append_value(true),
                "false" | "0" => builder.append_value(false),
                _ => builder.append_null(),
            },
            Value::Number(num) => {
                if let Some(n) = num.as_i64() {
                    builder.append_value(n != 0);
                } else if let Some(n) = num.as_u64() {
                    builder.append_value(n != 0);
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_timestamp_array_from_values(values: &[Value]) -> ArrayRef {
    let mut builder = TimestampMillisecondBuilder::with_capacity(values.len());
    for value in values {
        match value {
            Value::Number(num) => {
                // Fast path: direct number extraction (most common case)
                if let Some(i) = num.as_i64() {
                    builder.append_value(i);
                } else if let Some(u) = num.as_u64() {
                    builder.append_value(u as i64);
                } else {
                    builder.append_null();
                }
            }
            Value::String(s) => {
                // Slow path: string parsing (rare)
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

fn build_string_array_from_values(values: &[Value]) -> ArrayRef {
    let mut builder = LargeStringBuilder::with_capacity(values.len(), values.len() * 8);
    for value in values {
        match value {
            Value::Null => builder.append_null(),
            Value::String(s) => builder.append_value(s),
            other => {
                let string = other.to_string();
                builder.append_value(string.as_str());
            }
        }
    }
    Arc::new(builder.finish())
}

fn extract_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64().or_else(|| num.as_u64().map(|u| u as i64)),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn extract_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(num) => num.as_u64().or_else(|| {
            num.as_i64()
                .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
        }),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn extract_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

// Convert Arrow arrays to JSON Values (used for lazy conversion)
fn array_to_json_values(array: &ArrayRef, logical_type: &str) -> Vec<Value> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);

    match logical_type {
        "Integer" | "Number" => {
            if let Some(int_array) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                for i in 0..len {
                    if int_array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Number(Number::from(int_array.value(i))));
                    }
                }
            } else {
                // Fallback: convert to string
                for i in 0..len {
                    values.push(Value::String(
                        array_value_at(array, i, logical_type)
                            .unwrap_or(Value::Null)
                            .to_string(),
                    ));
                }
            }
        }
        "Float" => {
            if let Some(float_array) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                for i in 0..len {
                    if float_array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        if let Some(num) = Number::from_f64(float_array.value(i)) {
                            values.push(Value::Number(num));
                        } else {
                            values.push(Value::Null);
                        }
                    }
                }
            } else {
                for i in 0..len {
                    values.push(Value::String(
                        array_value_at(array, i, logical_type)
                            .unwrap_or(Value::Null)
                            .to_string(),
                    ));
                }
            }
        }
        "Boolean" => {
            if let Some(bool_array) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                for i in 0..len {
                    if bool_array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Bool(bool_array.value(i)));
                    }
                }
            } else {
                for i in 0..len {
                    values.push(Value::String(
                        array_value_at(array, i, logical_type)
                            .unwrap_or(Value::Null)
                            .to_string(),
                    ));
                }
            }
        }
        "Timestamp" => {
            if let Some(ts_array) = array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                for i in 0..len {
                    if ts_array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Number(Number::from(ts_array.value(i))));
                    }
                }
            } else {
                for i in 0..len {
                    values.push(Value::String(
                        array_value_at(array, i, logical_type)
                            .unwrap_or(Value::Null)
                            .to_string(),
                    ));
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
                    if string_array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::String(string_array.value(i).to_string()));
                    }
                }
            } else {
                for i in 0..len {
                    values.push(Value::String(
                        array_value_at(array, i, logical_type)
                            .unwrap_or(Value::Null)
                            .to_string(),
                    ));
                }
            }
        }
    }

    values
}

fn array_value_at(array: &ArrayRef, idx: usize, logical_type: &str) -> Option<Value> {
    if array.is_null(idx) {
        return Some(Value::Null);
    }

    match logical_type {
        "Integer" | "Number" => {
            if let Some(int_array) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                Some(Value::Number(Number::from(int_array.value(idx))))
            } else {
                None
            }
        }
        "Float" => {
            if let Some(float_array) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                Number::from_f64(float_array.value(idx)).map(Value::Number)
            } else {
                None
            }
        }
        "Boolean" => {
            if let Some(bool_array) = array.as_any().downcast_ref::<arrow_array::BooleanArray>() {
                Some(Value::Bool(bool_array.value(idx)))
            } else {
                None
            }
        }
        "Timestamp" => {
            if let Some(ts_array) = array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                Some(Value::Number(Number::from(ts_array.value(idx))))
            } else {
                None
            }
        }
        _ => {
            if let Some(string_array) = array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
            {
                Some(Value::String(string_array.value(idx).to_string()))
            } else {
                None
            }
        }
    }
}
