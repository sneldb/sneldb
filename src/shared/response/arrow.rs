use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use serde_json::Value;

use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
use crate::engine::types::ScalarValue;
use crate::shared::response::render::{Renderer, StreamingFormat};
use crate::shared::response::types::{Response, ResponseBody, StatusCode};

type ArrowResult<T> = Result<T, ArrowError>;

pub struct ArrowRenderer;

impl Renderer for ArrowRenderer {
    fn render(&self, response: &Response) -> Vec<u8> {
        // For non-streaming responses we fall back to compact JSON to keep clients informed.
        let mut payload = serde_json::Map::new();
        payload.insert("status".into(), response.status.code().into());
        payload.insert("message".into(), response.message.clone().into());
        payload.insert(
            "count".into(),
            serde_json::Value::from(response.count as u64),
        );

        match &response.body {
            ResponseBody::Lines(lines) => {
                payload.insert(
                    "results".into(),
                    serde_json::Value::Array(
                        lines
                            .iter()
                            .map(|line| serde_json::Value::String(line.clone()))
                            .collect(),
                    ),
                );
            }
            ResponseBody::ScalarArray(values) => {
                let json_values: Vec<Value> = values.iter().map(|v| v.to_json()).collect();
                payload.insert("results".into(), serde_json::Value::Array(json_values));
            }
            ResponseBody::Table { columns, rows } => {
                let cols = columns
                    .iter()
                    .map(|(name, ty)| {
                        serde_json::json!({
                            "name": name,
                            "type": ty,
                        })
                    })
                    .collect::<Vec<_>>();
                let row_values: Vec<Value> = rows
                    .iter()
                    .map(|r| Value::Array(r.iter().map(|v| v.to_json()).collect()))
                    .collect();
                payload.insert(
                    "results".into(),
                    serde_json::json!({ "columns": cols, "rows": row_values }),
                );
            }
        }

        let mut buf = Vec::new();
        if serde_json::to_writer(&mut buf, &payload).is_err() {
            buf.clear();
            buf.extend_from_slice(br#"{"status":500,"message":"Failed to serialize response"}"#);
        }
        buf.push(b'\n');
        buf
    }

    fn streaming_format(&self) -> StreamingFormat {
        StreamingFormat::Arrow
    }

    fn stream_schema(&self, _columns: &[(String, String)], _out: &mut Vec<u8>) {
        // Schema is handled by ArrowStreamEncoder
        unreachable!("stream_schema should not be called directly for Arrow renderer")
    }

    fn stream_row(&self, _columns: &[&str], _values: &[ScalarValue], _out: &mut Vec<u8>) {
        // Single row streaming not supported for Arrow - use stream_column_batch
        unreachable!("stream_row should not be called for Arrow renderer")
    }

    fn stream_batch(&self, _columns: &[&str], _batch: &[Vec<ScalarValue>], _out: &mut Vec<u8>) {
        // Batch streaming not directly supported - use stream_column_batch
        unreachable!("stream_batch should not be called for Arrow renderer")
    }

    fn stream_column_batch(&self, _batch: &ColumnBatch, _out: &mut Vec<u8>) {
        // ColumnBatch streaming is handled by ArrowStreamEncoder
        unreachable!("stream_column_batch should not be called directly for Arrow renderer")
    }

    fn stream_end(&self, _row_count: usize, _out: &mut Vec<u8>) {
        // End frame is handled by ArrowStreamEncoder
        unreachable!("stream_end should not be called directly for Arrow renderer")
    }
}

pub struct ArrowStreamEncoder {
    schema: Arc<Schema>,
    data_gen: IpcDataGenerator,
    dictionary_tracker: DictionaryTracker,
    write_options: IpcWriteOptions,
}

impl ArrowStreamEncoder {
    pub fn new(batch_schema: &BatchSchema) -> ArrowResult<Self> {
        let schema = build_arrow_schema(batch_schema)?;
        Ok(Self {
            schema,
            data_gen: IpcDataGenerator::default(),
            dictionary_tracker: DictionaryTracker::new(true),
            write_options: IpcWriteOptions::default(),
        })
    }

    pub fn write_schema(&mut self, out: &mut Vec<u8>) -> ArrowResult<()> {
        out.clear();
        let encoded = self.data_gen.schema_to_bytes_with_dictionary_tracker(
            &self.schema,
            &mut self.dictionary_tracker,
            &self.write_options,
        );

        arrow_ipc::writer::write_message(&mut *out, encoded, &self.write_options)?;
        Ok(())
    }

    pub fn write_batch(
        &mut self,
        schema: &BatchSchema,
        batch: &ColumnBatch,
        row_indices: Option<&[usize]>,
        out: &mut Vec<u8>,
    ) -> ArrowResult<()> {
        // Convert ScalarValue → Arrow on-demand (only at output boundary)
        let (dict_batches, record_data) = if row_indices.is_none() {
            // Convert entire batch to Arrow
            let record_batch = batch.to_record_batch().map_err(|e| {
                ArrowError::InvalidArgumentError(format!("Failed to convert batch: {}", e))
            })?;
            self.data_gen.encoded_batch(
                &record_batch,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        } else {
            // For row_indices, we need to build a new RecordBatch with sliced arrays
            let record_batch = build_record_batch(&self.schema, schema, batch, row_indices)?;
            self.data_gen.encoded_batch(
                &record_batch,
                &mut self.dictionary_tracker,
                &self.write_options,
            )?
        };

        out.clear();
        for encoded in dict_batches {
            arrow_ipc::writer::write_message(&mut *out, encoded, &self.write_options)?;
        }
        arrow_ipc::writer::write_message(&mut *out, record_data, &self.write_options)?;
        Ok(())
    }

    pub fn write_end(&mut self, out: &mut Vec<u8>) -> ArrowResult<()> {
        out.clear();
        out.extend_from_slice(&CONTINUATION_MARKER.to_le_bytes());
        out.extend_from_slice(&0i32.to_le_bytes());
        Ok(())
    }
}

pub fn build_arrow_schema(batch_schema: &BatchSchema) -> ArrowResult<Arc<Schema>> {
    let fields = batch_schema
        .columns()
        .iter()
        .map(|spec| {
            let data_type = logical_to_arrow_type(spec.logical_type.as_str());
            Field::new(&spec.name, data_type, true)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

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

// Helper functions to convert ScalarValue to Arrow arrays (used at output boundary)
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
            ScalarValue::Int64(i) => builder.append_value(*i as f64),
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

fn build_record_batch(
    arrow_schema: &Arc<Schema>,
    batch_schema: &BatchSchema,
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
) -> ArrowResult<RecordBatch> {
    // Work with ScalarValue columns directly - convert to Arrow arrays
    let columns = batch.columns_ref();
    let mut arrays = Vec::with_capacity(batch_schema.column_count());

    for (col_idx, column_spec) in batch_schema.columns().iter().enumerate() {
        let logical_type = column_spec.logical_type.as_str();
        let data_type = logical_to_arrow_type(logical_type);
        let column = &columns[col_idx];

        let array: ArrayRef = if let Some(indices) = row_indices {
            // Build array with only selected row indices
            match data_type {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(indices.len());
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Int64(v)) => builder.append_value(*v),
                            Some(ScalarValue::Timestamp(v)) => builder.append_value(*v),
                            Some(ScalarValue::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(indices.len());
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Float64(v)) => builder.append_value(*v),
                            Some(ScalarValue::Int64(v)) => builder.append_value(*v as f64),
                            Some(ScalarValue::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(indices.len());
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Boolean(v)) => builder.append_value(*v),
                            Some(ScalarValue::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let mut builder = TimestampMillisecondBuilder::with_capacity(indices.len());
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Timestamp(v)) => builder.append_value(*v),
                            Some(ScalarValue::Int64(v)) => builder.append_value(*v),
                            Some(ScalarValue::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::LargeUtf8 => {
                    // Estimate string capacity
                    let estimated_bytes = if indices.len() > 50 {
                        let sample_size = 20.min(indices.len());
                        let sample_sum: usize = indices[..sample_size]
                            .iter()
                            .filter_map(|&idx| {
                                column.get(idx).and_then(|v| match v {
                                    ScalarValue::Utf8(s) => Some(s.len()),
                                    _ => None,
                                })
                            })
                            .sum();
                        if sample_size > 0 {
                            (sample_sum * indices.len() / sample_size).max(indices.len() * 4)
                        } else {
                            indices.len() * 8
                        }
                    } else {
                        indices.len() * 8
                    };
                    let mut builder =
                        LargeStringBuilder::with_capacity(indices.len(), estimated_bytes);
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Utf8(s)) => builder.append_value(s),
                            Some(ScalarValue::Null) => builder.append_null(),
                            Some(v) => {
                                // Convert other types to string
                                builder.append_value(v.to_string_repr());
                            }
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    // Fallback to string
                    let mut builder =
                        LargeStringBuilder::with_capacity(indices.len(), indices.len() * 8);
                    for &idx in indices {
                        match column.get(idx) {
                            Some(ScalarValue::Utf8(s)) => builder.append_value(s),
                            Some(ScalarValue::Null) => builder.append_null(),
                            Some(v) => builder.append_value(v.to_string_repr()),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
            }
        } else {
            // No row_indices - convert entire column
            match data_type {
                DataType::Int64 => build_int64_array_from_scalars(&column),
                DataType::Float64 => build_float64_array_from_scalars(&column),
                DataType::Boolean => build_bool_array_from_scalars(&column),
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    build_timestamp_array_from_scalars(&column)
                }
                DataType::LargeUtf8 => build_string_array_from_scalars(&column),
                _ => build_string_array_from_scalars(&column),
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema.clone(), arrays)
}

pub fn response_error_bytes(status: StatusCode, message: &str) -> Vec<u8> {
    let payload = serde_json::json!({
        "status": status.code(),
        "message": message,
    });
    let mut buf = Vec::new();
    if serde_json::to_writer(&mut buf, &payload).is_err() {
        buf.extend_from_slice(br#"{"status":500,"message":"Failed to serialize"}"#);
    }
    buf.push(b'\n');
    buf
}

const CONTINUATION_MARKER: u32 = 0xFFFF_FFFF;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::reader::StreamReader;
    use crate::engine::core::read::result::ColumnSpec;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn arrow_encoder_stream_roundtrip() {
        let schema = BatchSchema::new(vec![
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Integer".to_string(),
            },
            ColumnSpec {
                name: "value".to_string(),
                logical_type: "String".to_string(),
            },
        ])
        .expect("schema");
        let schema_arc = Arc::new(schema);

        let rows = vec![(json!(1), json!("alpha")), (json!(2), json!("beta"))];

        let columns = vec![
            rows.iter()
                .map(|(id, _)| ScalarValue::from(id.clone()))
                .collect::<Vec<_>>(),
            rows.iter()
                .map(|(_, v)| ScalarValue::from(v.clone()))
                .collect::<Vec<_>>(),
        ];

        let batch = ColumnBatch::new(Arc::clone(&schema_arc), columns, rows.len(), None)
            .expect("column batch");

        let mut encoder = ArrowStreamEncoder::new(&schema_arc).expect("encoder");
        let mut out = Vec::new();
        let mut stream = Vec::new();

        encoder.write_schema(&mut out).expect("schema frame");
        stream.extend_from_slice(&out);

        encoder
            .write_batch(&schema_arc, &batch, None, &mut out)
            .expect("batch frame");
        stream.extend_from_slice(&out);

        encoder.write_end(&mut out).expect("end frame");
        stream.extend_from_slice(&out);

        let mut reader = StreamReader::try_new(Cursor::new(stream), None).expect("reader");
        let record = reader.next().expect("record batch").expect("batch");
        assert_eq!(record.num_columns(), 2);
        assert_eq!(record.num_rows(), rows.len());

        let values = record
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("int column");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);

        let strings = record
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .expect("string column");
        assert_eq!(strings.value(0), "alpha");
        assert_eq!(strings.value(1), "beta");

        assert!(reader.next().is_none());
    }
}
