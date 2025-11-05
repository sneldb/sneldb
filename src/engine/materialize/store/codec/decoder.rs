use std::sync::Arc;

use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::store::frame::header::FrameHeader;
use crate::engine::materialize::store::frame::metadata::StoredFrameMeta;
use crate::engine::types::ScalarValue;
use serde_json::Value;
use sonic_rs;

use super::bitmap::NullBitmap;
use super::schema::SchemaConverter;
use super::value_codec::ValueCodec;

pub struct Decoder;

impl Decoder {
    pub fn decode(
        meta: &StoredFrameMeta,
        payload: Vec<u8>,
        header: &FrameHeader,
    ) -> Result<ColumnBatch, MaterializationError> {
        let schema = SchemaConverter::to_batch_schema(&meta.schema)?;
        let schema_arc = Arc::new(schema);
        let row_count = header.row_count as usize;
        let column_count = header.column_count as usize;

        let null_len = header.null_bitmap_len as usize;

        if payload.len() < null_len + 4 {
            return Err(MaterializationError::Corrupt(format!(
                "Frame {} payload too small for columnar format",
                meta.file_name
            )));
        }

        let (null_bitmap, rest) = payload.split_at(null_len);

        // Read number of variable-size columns
        let (num_var_cols_bytes, rest) = rest.split_at(4);
        let num_var_cols = u32::from_le_bytes(num_var_cols_bytes.try_into().unwrap()) as usize;

        // Identify which columns are variable-size
        let mut variable_col_indices = Vec::new();
        for (col_idx, schema_col) in meta.schema.iter().enumerate() {
            match schema_col.logical_type.as_str() {
                "String" | "JSON" | "Object" | "Array" => {
                    variable_col_indices.push(col_idx);
                }
                _ => {}
            }
        }

        if variable_col_indices.len() != num_var_cols {
            return Err(MaterializationError::Corrupt(
                "Columnar format: variable column count mismatch".into(),
            ));
        }

        let mut cursor = rest;

        // Read length tables for variable-size columns
        let mut length_tables: Vec<Vec<u32>> = Vec::new();
        for _ in 0..num_var_cols {
            let mut length_table = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                if cursor.len() < 4 {
                    return Err(MaterializationError::Corrupt(
                        "Columnar format: incomplete length table".into(),
                    ));
                }
                let (len_bytes, rest) = cursor.split_at(4);
                length_table.push(u32::from_le_bytes(len_bytes.try_into().unwrap()));
                cursor = rest;
            }
            length_tables.push(length_table);
        }

        // Pre-allocate columns
        let mut columns: Vec<Vec<ScalarValue>> = (0..column_count)
            .map(|_| Vec::with_capacity(row_count))
            .collect();

        let logical_types: Vec<&str> = meta
            .schema
            .iter()
            .map(|s| s.logical_type.as_str())
            .collect();

        // Decode column-by-column (much faster - better cache locality!)
        let mut length_table_idx = 0;
        for col_idx in 0..column_count {
            let logical_type = logical_types[col_idx];
            let is_variable = variable_col_indices.contains(&col_idx);
            let length_table = if is_variable {
                let idx = length_table_idx;
                length_table_idx += 1;
                Some(&length_tables[idx])
            } else {
                None
            };

            // Decode all rows for this column at once
            for row in 0..row_count {
                let idx = row * column_count + col_idx;

                if NullBitmap::is_null(null_bitmap, idx) {
                    columns[col_idx].push(ScalarValue::Null);
                    continue;
                }

                // For fixed-size types, decode directly with bulk operations
                match logical_type {
                    "Timestamp" | "Integer" => {
                        if cursor.len() < 8 {
                            return Err(MaterializationError::Corrupt(
                                "Insufficient bytes for integer".into(),
                            ));
                        }
                        let (head, tail) = cursor.split_at(8);
                        let num = i64::from_le_bytes(head.try_into().unwrap());
                        columns[col_idx].push(ScalarValue::Int64(num));
                        cursor = tail;
                    }
                    "Float" => {
                        if cursor.len() < 8 {
                            return Err(MaterializationError::Corrupt(
                                "Insufficient bytes for float".into(),
                            ));
                        }
                        let (head, tail) = cursor.split_at(8);
                        let num = f64::from_le_bytes(head.try_into().unwrap());
                        columns[col_idx].push(ScalarValue::Float64(num));
                        cursor = tail;
                    }
                    "Boolean" => {
                        if cursor.is_empty() {
                            return Err(MaterializationError::Corrupt(
                                "Insufficient bytes for boolean".into(),
                            ));
                        }
                        let (head, tail) = cursor.split_at(1);
                        columns[col_idx].push(ScalarValue::Boolean(head[0] != 0));
                        cursor = tail;
                    }
                    _ => {
                        // Variable-size: use length table
                        if let Some(length_table) = length_table {
                            let len = length_table[row] as usize;
                            if len == 0 {
                                columns[col_idx].push(ScalarValue::Null);
                                continue;
                            }
                            if cursor.len() < len {
                                return Err(MaterializationError::Corrupt(
                                    "Insufficient bytes for variable value".into(),
                                ));
                            }
                            let (value_bytes, tail) = cursor.split_at(len);
                            let scalar_value = if logical_type == "String" {
                                // String data is stored without length prefix (length is in table)
                                let s = std::str::from_utf8(value_bytes).map_err(|e| {
                                    MaterializationError::Corrupt(format!(
                                        "UTF-8 decode error: {e}"
                                    ))
                                })?;
                                ScalarValue::Utf8(s.to_string())
                            } else {
                                // JSON data is stored without length prefix (length is in table)
                                let json_value: Value =
                                    sonic_rs::from_slice(value_bytes).map_err(|e| {
                                        MaterializationError::Corrupt(format!(
                                            "JSON parse error: {e}"
                                        ))
                                    })?;
                                ScalarValue::from(json_value)
                            };
                            columns[col_idx].push(scalar_value);
                            cursor = tail;
                        } else {
                            // Fallback to value codec for unknown types
                            let (value, remaining) =
                                ValueCodec::decode_value_fast(cursor, logical_type)?;
                            columns[col_idx].push(ScalarValue::from(value));
                            cursor = remaining;
                        }
                    }
                }
            }
        }

        if !cursor.is_empty() {
            return Err(MaterializationError::Corrupt(format!(
                "Frame {} has {} trailing bytes after decode",
                meta.file_name,
                cursor.len()
            )));
        }

        ColumnBatch::new(schema_arc, columns, row_count, None)
            .map_err(|e| MaterializationError::Batch(e.to_string()))
    }
}
