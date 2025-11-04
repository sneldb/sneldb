use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::SchemaSnapshot;

use super::bitmap::NullBitmap;
use super::schema::SchemaConverter;
use super::types::EncodedColumns;
use super::utils::ValueExtractor;
use super::value_codec::ValueCodec;

pub struct Encoder;

impl Encoder {
    pub fn encode(
        schema: &[SchemaSnapshot],
        batch: &ColumnBatch,
    ) -> Result<EncodedColumns, MaterializationError> {
        let row_count = batch.len();
        let column_count = schema.len();

        let mut column_views = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            let values = batch
                .column(idx)
                .map_err(|e| MaterializationError::Batch(e.to_string()))?;
            column_views.push(values);
        }

        let mut null_bitmap = vec![0u8; NullBitmap::size_for(row_count, column_count)];
        let mut column_data: Vec<Vec<u8>> = (0..column_count).map(|_| Vec::new()).collect();
        let mut length_tables: Vec<Vec<u32>> = Vec::new();

        let mut min_timestamp = u64::MAX;
        let mut max_timestamp = 0u64;
        let mut max_event_id = 0u64;

        let timestamp_index = schema.iter().position(|c| c.name == "timestamp");
        let event_id_index = schema.iter().position(|c| c.name == "event_id");

        // Classify columns: which are variable-size (need length tables)
        let mut variable_col_indices = Vec::new();
        for (col_idx, schema_col) in schema.iter().enumerate() {
            match schema_col.logical_type.as_str() {
                "String" | "JSON" | "Object" | "Array" => {
                    variable_col_indices.push(col_idx);
                    length_tables.push(Vec::with_capacity(row_count));
                }
                _ => {}
            }
        }

        // Encode column-by-column (much better for decoding!)
        for col_idx in 0..column_count {
            let logical_type = &schema[col_idx].logical_type;
            let needs_length_table = variable_col_indices.contains(&col_idx);
            let length_table_idx = if needs_length_table {
                variable_col_indices.iter().position(|&i| i == col_idx)
            } else {
                None
            };

            for row in 0..row_count {
                let idx = row * column_count + col_idx;
                let value = &column_views[col_idx][row];

                if value.is_null() {
                    NullBitmap::set_bit(&mut null_bitmap, idx);
                    if let Some(lt_idx) = length_table_idx {
                        length_tables[lt_idx].push(0); // 0 length for null
                    }
                    continue;
                }

                // Track timestamp/event_id for metadata
                if Some(col_idx) == timestamp_index {
                    if let Some(ts) = ValueExtractor::extract_u64(value) {
                        min_timestamp = min_timestamp.min(ts);
                        max_timestamp = max_timestamp.max(ts);
                    }
                }
                if Some(col_idx) == event_id_index {
                    if let Some(eid) = ValueExtractor::extract_u64(value) {
                        max_event_id = max_event_id.max(eid);
                    }
                }

                // Encode value - variable-size values don't include length prefix
                let start_len = column_data[col_idx].len();
                if needs_length_table {
                    // For variable-size types, encode without length prefix
                    match logical_type.as_str() {
                        "String" => {
                            let s = value
                                .as_str()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| value.to_string());
                            column_data[col_idx].extend_from_slice(s.as_bytes());
                        }
                        _ => {
                            // JSON and other variable types: serialize directly
                            let serialized = serde_json::to_vec(value).map_err(|e| {
                                MaterializationError::Corrupt(format!("JSON encode error: {e}"))
                            })?;
                            column_data[col_idx].extend_from_slice(&serialized);
                        }
                    }
                } else {
                    // For fixed-size types, use normal encode
                    ValueCodec::encode_value(value, logical_type, &mut column_data[col_idx])?;
                }
                let value_len = column_data[col_idx].len() - start_len;

                if let Some(lt_idx) = length_table_idx {
                    length_tables[lt_idx].push(value_len as u32);
                }
            }
        }

        if min_timestamp == u64::MAX {
            min_timestamp = 0;
        }

        Ok(EncodedColumns {
            null_bitmap,
            length_tables,
            column_data,
            schema_hash: SchemaConverter::hash(schema),
            row_count,
            min_timestamp,
            max_timestamp,
            max_event_id,
        })
    }
}
