use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, TimestampMillisecondBuilder,
};
use arrow_schema::{DataType, TimeUnit};

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::format::PhysicalType;
use serde_json::{Number, Value};

#[derive(Clone, Debug)]
pub struct ColumnBlockSnapshot {
    phys: PhysicalType,
    values: ColumnValues,
}

impl ColumnBlockSnapshot {
    pub fn new(phys: PhysicalType, values: ColumnValues) -> Self {
        Self { phys, values }
    }

    pub fn empty() -> Self {
        Self {
            phys: PhysicalType::VarBytes,
            values: ColumnValues::empty(),
        }
    }

    #[inline]
    pub fn physical_type(&self) -> PhysicalType {
        self.phys
    }

    #[inline]
    pub fn values(&self) -> &ColumnValues {
        &self.values
    }

    #[inline]
    pub fn into_values(self) -> ColumnValues {
        self.values
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn to_strings(&self) -> Vec<String> {
        Self::values_to_strings(self.phys, &self.values)
    }

    pub fn into_strings(self) -> Vec<String> {
        let ColumnBlockSnapshot { phys, values } = self;
        Self::values_to_strings(phys, &values)
    }

    pub fn to_json_values(&self) -> Vec<Value> {
        Self::values_to_json(self.phys, &self.values)
    }

    pub fn into_json_values(self) -> Vec<Value> {
        let ColumnBlockSnapshot { phys, values } = self;
        Self::values_to_json(phys, &values)
    }

    /// Convert directly to Arrow array, bypassing JSON Values for better performance.
    /// The `logical_type` parameter is used to determine the Arrow data type
    /// (e.g., "Timestamp" vs "Integer" both map from PhysicalType::I64).
    pub fn to_arrow_array(&self, logical_type: &str) -> ArrayRef {
        Self::values_to_arrow_array(self.phys, logical_type, &self.values)
    }

    /// Convert directly to Arrow array, consuming self.
    pub fn into_arrow_array(self, logical_type: &str) -> ArrayRef {
        let ColumnBlockSnapshot { phys, values } = self;
        Self::values_to_arrow_array(phys, logical_type, &values)
    }

    fn values_to_strings(phys: PhysicalType, values: &ColumnValues) -> Vec<String> {
        let len = values.len();
        let mut out = Vec::with_capacity(len);
        match phys {
            PhysicalType::I64 => {
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::U64 => {
                for idx in 0..len {
                    match values.get_u64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::F64 => {
                for idx in 0..len {
                    match values.get_f64_at(idx) {
                        Some(v) => out.push(v.to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            PhysicalType::Bool => {
                for idx in 0..len {
                    match values.get_bool_at(idx) {
                        Some(true) => out.push("true".to_string()),
                        Some(false) => out.push("false".to_string()),
                        None => out.push(String::new()),
                    }
                }
            }
            _ => {
                for idx in 0..len {
                    out.push(values.get_str_at(idx).unwrap_or("").to_string());
                }
            }
        }
        out
    }

    fn values_to_json(phys: PhysicalType, values: &ColumnValues) -> Vec<Value> {
        let len = values.len();
        let mut out = Vec::with_capacity(len);
        match phys {
            PhysicalType::I64 => {
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => out.push(Value::Number(Number::from(v))),
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::U64 => {
                for idx in 0..len {
                    match values.get_u64_at(idx) {
                        Some(v) => out.push(Value::Number(Number::from(v))),
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::F64 => {
                for idx in 0..len {
                    match values.get_f64_at(idx) {
                        Some(v) => match Number::from_f64(v) {
                            Some(num) => out.push(Value::Number(num)),
                            None => out.push(Value::Null),
                        },
                        None => out.push(Value::Null),
                    }
                }
            }
            PhysicalType::Bool => {
                for idx in 0..len {
                    match values.get_bool_at(idx) {
                        Some(b) => out.push(Value::Bool(b)),
                        None => out.push(Value::Null),
                    }
                }
            }
            _ => {
                for idx in 0..len {
                    out.push(
                        values
                            .get_str_at(idx)
                            .map(|s| Value::String(s.to_string()))
                            .unwrap_or(Value::Null),
                    );
                }
            }
        }
        out
    }

    fn values_to_arrow_array(
        phys: PhysicalType,
        logical_type: &str,
        values: &ColumnValues,
    ) -> ArrayRef {
        let len = values.len();
        let arrow_type = match logical_type {
            "Integer" | "Number" => DataType::Int64,
            "Float" => DataType::Float64,
            "Boolean" => DataType::Boolean,
            "Timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
            "String" => DataType::LargeUtf8,
            "JSON" | "Object" | "Array" => DataType::LargeUtf8,
            other if other.starts_with("UInt") => DataType::Int64,
            _ => DataType::LargeUtf8,
        };

        match (phys, &arrow_type) {
            (PhysicalType::I64, DataType::Int64) => {
                let mut builder = Int64Builder::with_capacity(len);
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            (PhysicalType::I64, DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                let mut builder = TimestampMillisecondBuilder::with_capacity(len);
                for idx in 0..len {
                    match values.get_i64_at(idx) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            (PhysicalType::U64, DataType::Int64) => {
                let mut builder = Int64Builder::with_capacity(len);
                for idx in 0..len {
                    match values.get_u64_at(idx) {
                        Some(v) => builder.append_value(v as i64),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            (PhysicalType::F64, DataType::Float64) => {
                let mut builder = Float64Builder::with_capacity(len);
                for idx in 0..len {
                    match values.get_f64_at(idx) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            (PhysicalType::Bool, DataType::Boolean) => {
                let mut builder = BooleanBuilder::with_capacity(len);
                for idx in 0..len {
                    match values.get_bool_at(idx) {
                        Some(b) => builder.append_value(b),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // String or other types - convert to LargeUtf8
                let mut builder = LargeStringBuilder::with_capacity(len, len * 8);
                for idx in 0..len {
                    match values.get_str_at(idx) {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}
