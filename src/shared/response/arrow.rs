use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeStringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use serde_json::Value;

use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
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
            ResponseBody::JsonArray(values) => {
                payload.insert("results".into(), serde_json::Value::Array(values.clone()));
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
                let row_values = rows
                    .iter()
                    .map(|r| serde_json::Value::Array(r.clone()))
                    .collect::<Vec<_>>();
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
        // Use the RecordBatch directly (zero conversion - it's already stored internally)
        // Avoid cloning when row_indices is None - use reference directly!
        let (dict_batches, record_data) = if row_indices.is_none() {
            // Zero conversion path - use RecordBatch reference directly (no clone!)
            let record_batch = batch.record_batch();
            self.data_gen.encoded_batch(
                record_batch,
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

fn build_record_batch(
    arrow_schema: &Arc<Schema>,
    _batch_schema: &BatchSchema,
    batch: &ColumnBatch,
    row_indices: Option<&[usize]>,
) -> ArrowResult<RecordBatch> {
    // Use the RecordBatch directly - no conversion needed!
    let record_batch = batch.record_batch();

    if let Some(indices) = row_indices {
        // Slice arrays directly using manual take (zero JSON conversion!)
        // Build new arrays by taking only the specified indices
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(record_batch.num_columns());

        for col_idx in 0..record_batch.num_columns() {
            let array = record_batch.column(col_idx);
            let column_spec = batch.schema().columns().get(col_idx).unwrap();
            let logical_type = column_spec.logical_type.as_str();

            // Build new array by taking only the specified indices
            let taken = match logical_type {
                "Integer" | "Number" => {
                    if let Some(int_array) =
                        array.as_any().downcast_ref::<arrow_array::Int64Array>()
                    {
                        let mut builder = Int64Builder::with_capacity(indices.len());
                        for &idx in indices {
                            if int_array.is_null(idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(int_array.value(idx));
                            }
                        }
                        Arc::new(builder.finish())
                    } else {
                        array.clone() // Fallback: return original
                    }
                }
                "Float" => {
                    if let Some(float_array) =
                        array.as_any().downcast_ref::<arrow_array::Float64Array>()
                    {
                        let mut builder = Float64Builder::with_capacity(indices.len());
                        for &idx in indices {
                            if float_array.is_null(idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(float_array.value(idx));
                            }
                        }
                        Arc::new(builder.finish())
                    } else {
                        array.clone()
                    }
                }
                "Boolean" => {
                    if let Some(bool_array) =
                        array.as_any().downcast_ref::<arrow_array::BooleanArray>()
                    {
                        let mut builder = BooleanBuilder::with_capacity(indices.len());
                        for &idx in indices {
                            if bool_array.is_null(idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(bool_array.value(idx));
                            }
                        }
                        Arc::new(builder.finish())
                    } else {
                        array.clone()
                    }
                }
                "Timestamp" => {
                    if let Some(ts_array) = array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                    {
                        let mut builder = TimestampMillisecondBuilder::with_capacity(indices.len());
                        for &idx in indices {
                            if ts_array.is_null(idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(ts_array.value(idx));
                            }
                        }
                        Arc::new(builder.finish())
                    } else {
                        array.clone()
                    }
                }
                _ => {
                    // String or other types
                    if let Some(string_array) = array
                        .as_any()
                        .downcast_ref::<arrow_array::LargeStringArray>()
                    {
                        // Estimate capacity: for large slices, sample to estimate average string length
                        // For small slices, use default estimate to avoid double iteration
                        let estimated_bytes = if indices.len() > 50 {
                            // For large slices (>50), sample first 20 to estimate average
                            let sample_size = 20.min(indices.len());
                            let sample_sum: usize = indices[..sample_size]
                                .iter()
                                .filter_map(|&idx| {
                                    if !string_array.is_null(idx) {
                                        Some(string_array.value(idx).len())
                                    } else {
                                        None
                                    }
                                })
                                .sum();
                            if sample_size > 0 {
                                (sample_sum * indices.len() / sample_size).max(indices.len() * 4)
                            } else {
                                indices.len() * 8 // Fallback
                            }
                        } else {
                            // For small slices, use default estimate (avoid double iteration)
                            indices.len() * 8
                        };

                        let mut builder =
                            LargeStringBuilder::with_capacity(indices.len(), estimated_bytes);
                        for &idx in indices {
                            if string_array.is_null(idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(string_array.value(idx));
                            }
                        }
                        Arc::new(builder.finish())
                    } else {
                        array.clone()
                    }
                }
            };
            arrays.push(taken);
        }

        RecordBatch::try_new(arrow_schema.clone(), arrays)
    } else {
        // No row_indices - just clone the RecordBatch
        Ok(record_batch.clone())
    }
}

fn build_indices(row_indices: Option<&[usize]>, len: usize) -> Cow<'_, [usize]> {
    match row_indices {
        Some(indices) => Cow::Borrowed(indices),
        None => {
            let mut all = Vec::with_capacity(len);
            for idx in 0..len {
                all.push(idx);
            }
            Cow::Owned(all)
        }
    }
}

fn build_int64_array(
    values: &[Value],
    row_indices: Option<&[usize]>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(row_count);
    let indices = build_indices(row_indices, values.len());
    for &row in indices.iter() {
        if let Some(num) = extract_i64(&values[row]) {
            builder.append_value(num);
        } else if let Some(num) = extract_u64(&values[row]) {
            builder.append_value(num as i64);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn build_float64_array(
    values: &[Value],
    row_indices: Option<&[usize]>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(row_count);
    let indices = build_indices(row_indices, values.len());
    for &row in indices.iter() {
        if let Some(num) = extract_f64(&values[row]) {
            builder.append_value(num);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn build_bool_array(values: &[Value], row_indices: Option<&[usize]>, row_count: usize) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(row_count);
    let indices = build_indices(row_indices, values.len());
    for &row in indices.iter() {
        match &values[row] {
            Value::Bool(value) => builder.append_value(*value),
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

fn build_timestamp_array(
    values: &[Value],
    row_indices: Option<&[usize]>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = TimestampMillisecondBuilder::with_capacity(row_count);
    let indices = build_indices(row_indices, values.len());
    for &row in indices.iter() {
        if let Some(num) = extract_i64(&values[row]) {
            builder.append_value(num);
        } else if let Some(num) = extract_u64(&values[row]) {
            builder.append_value(num as i64);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn build_string_array(
    values: &[Value],
    row_indices: Option<&[usize]>,
    row_count: usize,
) -> ArrayRef {
    let mut builder = LargeStringBuilder::with_capacity(row_count, row_count * 8);
    let indices = build_indices(row_indices, values.len());
    for &row in indices.iter() {
        match &values[row] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::core::read::result::ColumnSpec;
    use arrow_ipc::reader::StreamReader;
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
            rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            rows.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
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
