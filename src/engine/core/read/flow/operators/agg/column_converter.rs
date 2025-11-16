use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::read::flow::FlowOperatorError;
use crate::engine::core::read::flow::batch::ColumnBatch;
use crate::engine::core::read::sink::AggregateSink;
use crate::engine::types::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Converts ColumnBatch to HashMap<String, ColumnValues> for aggregate processing
/// Only creates ColumnValues for columns needed by the aggregate operation
pub struct ColumnConverter;

impl ColumnConverter {
    /// Convert batch to columns map, only including columns needed by the sink
    pub fn convert(
        batch: &ColumnBatch,
        column_names: &[String],
        needed_columns: &HashSet<String>,
    ) -> Result<HashMap<String, ColumnValues>, FlowOperatorError> {
        let columns_ref = batch.columns_ref();
        let mut columns_map = HashMap::with_capacity(needed_columns.len());

        for (col_idx, name) in column_names.iter().enumerate() {
            if !needed_columns.contains(name) {
                continue;
            }

            let values = columns_ref.get(col_idx).ok_or_else(|| {
                FlowOperatorError::Batch(format!("column {} not found at index {}", name, col_idx))
            })?;

            let column_values = Self::create_column_values(values, name)?;
            columns_map.insert(name.clone(), column_values);
        }

        Ok(columns_map)
    }

    /// Determine which columns are needed by the aggregate operation
    /// This result can be cached since it doesn't change per batch
    pub fn determine_needed_columns(sink: &AggregateSink) -> HashSet<String> {
        let mut needed = HashSet::new();
        needed.insert(sink.time_field.clone());

        if let Some(ref group_by) = sink.group_by {
            for field in group_by {
                needed.insert(field.clone());
            }
        }

        for spec in &sink.specs {
            match spec {
                AggregateOpSpec::CountAll => {}
                AggregateOpSpec::CountField { field }
                | AggregateOpSpec::Total { field }
                | AggregateOpSpec::Avg { field }
                | AggregateOpSpec::Min { field }
                | AggregateOpSpec::Max { field }
                | AggregateOpSpec::CountUnique { field } => {
                    needed.insert(field.clone());
                }
            }
        }

        needed
    }

    fn create_column_values(
        values: &[ScalarValue],
        name: &str,
    ) -> Result<ColumnValues, FlowOperatorError> {
        if values
            .iter()
            .all(|v| matches!(v, ScalarValue::Int64(_) | ScalarValue::Null))
        {
            Self::create_typed_i64_column(values)
        } else {
            Self::create_string_column(values, name)
        }
    }

    fn create_typed_i64_column(values: &[ScalarValue]) -> Result<ColumnValues, FlowOperatorError> {
        let row_count = values.len();
        let null_bytes = (row_count + 7) / 8;
        let mut bytes = vec![0u8; null_bytes + row_count * 8];
        let payload_start = null_bytes;
        let mut has_null = false;

        for (idx, value) in values.iter().enumerate() {
            match value {
                ScalarValue::Int64(i) => {
                    let offset = payload_start + idx * 8;
                    bytes[offset..offset + 8].copy_from_slice(&i.to_le_bytes());
                }
                ScalarValue::Null => {
                    has_null = true;
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    bytes[byte_idx] |= 1 << bit_idx;
                }
                _ => unreachable!(),
            }
        }

        let nulls_block = has_null.then_some((0, null_bytes));
        let block = Arc::new(DecompressedBlock::from_bytes(bytes));
        Ok(ColumnValues::new_typed_i64(
            block,
            payload_start,
            row_count,
            nulls_block,
        ))
    }

    fn create_string_column(
        values: &[ScalarValue],
        _name: &str,
    ) -> Result<ColumnValues, FlowOperatorError> {
        let mut bytes = Vec::new();
        let mut ranges = Vec::with_capacity(values.len());

        for value in values {
            let str_repr = match value {
                ScalarValue::Utf8(s) => s.as_str(),
                ScalarValue::Int64(i) => {
                    let s = i.to_string();
                    let start = bytes.len();
                    bytes.extend_from_slice(s.as_bytes());
                    ranges.push((start, s.len()));
                    continue;
                }
                ScalarValue::Float64(f) => {
                    let s = f.to_string();
                    let start = bytes.len();
                    bytes.extend_from_slice(s.as_bytes());
                    ranges.push((start, s.len()));
                    continue;
                }
                ScalarValue::Boolean(b) => {
                    let s = if *b { "true" } else { "false" };
                    let start = bytes.len();
                    bytes.extend_from_slice(s.as_bytes());
                    ranges.push((start, s.len()));
                    continue;
                }
                ScalarValue::Timestamp(ts) => {
                    let s = ts.to_string();
                    let start = bytes.len();
                    bytes.extend_from_slice(s.as_bytes());
                    ranges.push((start, s.len()));
                    continue;
                }
                ScalarValue::Binary(_) => "",
                ScalarValue::Null => "",
            };

            if !str_repr.is_empty() {
                let start = bytes.len();
                bytes.extend_from_slice(str_repr.as_bytes());
                ranges.push((start, str_repr.len()));
            } else {
                ranges.push((bytes.len(), 0));
            }
        }

        let block = Arc::new(DecompressedBlock::from_bytes(bytes));
        Ok(ColumnValues::new(block, ranges))
    }
}
