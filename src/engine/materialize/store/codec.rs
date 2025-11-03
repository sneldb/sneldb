use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::engine::core::column::compression::Lz4Codec;
use crate::engine::core::column::compression::compression_codec::CompressionCodec;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::SchemaSnapshot;
use serde_json::{Number, Value};

use super::frame::data::FrameData;
use super::frame::header::FrameHeader;
use super::frame::metadata::StoredFrameMeta;

pub struct EncodedFrame {
    pub schema: Vec<SchemaSnapshot>,
    pub schema_hash: u64,
    pub row_count: u32,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub max_event_id: u64,
    pub null_bitmap_len: u32,
    pub compressed: Vec<u8>,
    pub uncompressed_len: u32,
}

pub trait BatchCodec {
    fn encode(
        &self,
        schema: &[SchemaSnapshot],
        batch: &ColumnBatch,
    ) -> Result<EncodedFrame, MaterializationError>;

    fn decode(
        &self,
        meta: &StoredFrameMeta,
        data: FrameData,
    ) -> Result<ColumnBatch, MaterializationError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Lz4BatchCodec;

impl BatchCodec for Lz4BatchCodec {
    fn encode(
        &self,
        schema: &[SchemaSnapshot],
        batch: &ColumnBatch,
    ) -> Result<EncodedFrame, MaterializationError> {
        let encoded = encode_columns(schema, batch)?;
        let codec = Lz4Codec;
        let compressed = codec
            .compress(&encoded.buffer)
            .map_err(|e| MaterializationError::Corrupt(format!("LZ4 compress: {e}")))?;

        Ok(EncodedFrame {
            schema: schema.to_vec(),
            schema_hash: encoded.schema_hash,
            row_count: encoded.row_count as u32,
            min_timestamp: encoded.min_timestamp,
            max_timestamp: encoded.max_timestamp,
            max_event_id: encoded.max_event_id,
            null_bitmap_len: encoded.null_bitmap_len as u32,
            compressed,
            uncompressed_len: encoded.buffer.len() as u32,
        })
    }

    fn decode(
        &self,
        meta: &StoredFrameMeta,
        data: FrameData,
    ) -> Result<ColumnBatch, MaterializationError> {
        let codec = Lz4Codec;
        let payload = codec
            .decompress(&data.compressed, data.header.uncompressed_len as usize)
            .map_err(|e| MaterializationError::Corrupt(format!("LZ4 decompress: {e}")))?;

        if payload.len() != data.header.uncompressed_len as usize {
            return Err(MaterializationError::Corrupt(format!(
                "Payload length mismatch for frame {} (expected {}, got {})",
                meta.file_name,
                data.header.uncompressed_len,
                payload.len()
            )));
        }

        decode_columns(meta, payload, &data.header)
    }
}

pub fn schema_to_batch_schema(
    schema: &[SchemaSnapshot],
) -> Result<BatchSchema, MaterializationError> {
    let columns = schema
        .iter()
        .map(|s| ColumnSpec {
            name: s.name.clone(),
            logical_type: s.logical_type.clone(),
        })
        .collect();
    BatchSchema::new(columns).map_err(|e| MaterializationError::Batch(e.to_string()))
}

pub fn batch_schema_to_snapshots(schema: &BatchSchema) -> Vec<SchemaSnapshot> {
    schema
        .columns()
        .iter()
        .map(|c| SchemaSnapshot::new(&c.name, &c.logical_type))
        .collect()
}

pub fn schema_hash(schema: &[SchemaSnapshot]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for snapshot in schema {
        snapshot.name.hash(&mut hasher);
        snapshot.logical_type.hash(&mut hasher);
    }
    hasher.finish()
}

struct EncodedColumns {
    buffer: Vec<u8>,
    schema_hash: u64,
    row_count: usize,
    min_timestamp: u64,
    max_timestamp: u64,
    max_event_id: u64,
    null_bitmap_len: usize,
}

fn encode_columns(
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

    let mut null_bitmap = vec![0u8; ((row_count * column_count) + 7) / 8];
    let mut buffer = Vec::new();

    let mut min_timestamp = u64::MAX;
    let mut max_timestamp = 0u64;
    let mut max_event_id = 0u64;

    let timestamp_index = schema.iter().position(|c| c.name == "timestamp");
    let event_id_index = schema.iter().position(|c| c.name == "event_id");

    for row in 0..row_count {
        for (col_idx, column) in column_views.iter().enumerate() {
            let idx = row * column_count + col_idx;
            let value = &column[row];
            if value.is_null() {
                set_null_bit(&mut null_bitmap, idx);
                continue;
            }

            if Some(col_idx) == timestamp_index {
                if let Some(ts) = extract_u64(value) {
                    min_timestamp = min_timestamp.min(ts);
                    max_timestamp = max_timestamp.max(ts);
                }
            }

            if Some(col_idx) == event_id_index {
                if let Some(eid) = extract_u64(value) {
                    max_event_id = max_event_id.max(eid);
                }
            }

            encode_value(value, &schema[col_idx].logical_type, &mut buffer)?;
        }
    }

    if min_timestamp == u64::MAX {
        min_timestamp = 0;
    }

    let mut combined = Vec::with_capacity(null_bitmap.len() + buffer.len());
    combined.extend_from_slice(&null_bitmap);
    combined.extend_from_slice(&buffer);

    Ok(EncodedColumns {
        buffer: combined,
        schema_hash: schema_hash(schema),
        row_count,
        min_timestamp,
        max_timestamp,
        max_event_id,
        null_bitmap_len: null_bitmap.len(),
    })
}

fn decode_columns(
    meta: &StoredFrameMeta,
    payload: Vec<u8>,
    header: &FrameHeader,
) -> Result<ColumnBatch, MaterializationError> {
    let schema = schema_to_batch_schema(&meta.schema)?;
    let schema_arc = Arc::new(schema);
    let row_count = header.row_count as usize;
    let column_count = header.column_count as usize;

    let null_len = header.null_bitmap_len as usize;
    if payload.len() < null_len {
        return Err(MaterializationError::Corrupt(format!(
            "Frame {} payload too small (expected >= {}, got {})",
            meta.file_name,
            null_len,
            payload.len()
        )));
    }

    let (null_bitmap, values_data) = payload.split_at(null_len);
    let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(row_count); column_count];

    let mut cursor = values_data;
    for row in 0..row_count {
        for col_idx in 0..column_count {
            let idx = row * column_count + col_idx;
            if is_null(null_bitmap, idx) {
                columns[col_idx].push(Value::Null);
                continue;
            }

            let logical_type = &meta.schema[col_idx].logical_type;
            let (value, remaining) = decode_value(cursor, logical_type)?;
            columns[col_idx].push(value);
            cursor = remaining;
        }
    }

    if !cursor.is_empty() {
        return Err(MaterializationError::Corrupt(format!(
            "Frame {} payload has {} trailing bytes",
            meta.file_name,
            cursor.len()
        )));
    }

    ColumnBatch::new(schema_arc, columns, row_count, None)
        .map_err(|e| MaterializationError::Batch(e.to_string()))
}

fn encode_value(
    value: &Value,
    logical_type: &str,
    buffer: &mut Vec<u8>,
) -> Result<(), MaterializationError> {
    match logical_type {
        "Timestamp" | "Integer" => {
            if let Some(num) = extract_i64(value) {
                buffer.extend_from_slice(&num.to_le_bytes());
            } else if let Some(num) = extract_u64(value) {
                buffer.extend_from_slice(&(num as i64).to_le_bytes());
            } else {
                encode_json_value(value, buffer)?;
            }
        }
        "Float" => {
            if let Some(f) = extract_f64(value) {
                buffer.extend_from_slice(&f.to_le_bytes());
            } else {
                encode_json_value(value, buffer)?;
            }
        }
        "Boolean" => {
            let byte = if let Some(b) = value.as_bool() {
                if b { 1u8 } else { 0u8 }
            } else if let Some(s) = value.as_str() {
                if s.eq_ignore_ascii_case("true") {
                    1u8
                } else if s.eq_ignore_ascii_case("false") {
                    0u8
                } else {
                    encode_json_value(value, buffer)?;
                    return Ok(());
                }
            } else {
                encode_json_value(value, buffer)?;
                return Ok(());
            };
            buffer.push(byte);
        }
        "String" => {
            let s = value
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| value.to_string());
            encode_bytes(s.as_bytes(), buffer);
        }
        _ => {
            encode_json_value(value, buffer)?;
        }
    }
    Ok(())
}

fn decode_value<'a>(
    data: &'a [u8],
    logical_type: &str,
) -> Result<(Value, &'a [u8]), MaterializationError> {
    match logical_type {
        "Timestamp" | "Integer" => {
            if data.len() < 8 {
                return Err(MaterializationError::Corrupt(format!(
                    "Insufficient bytes for integer field (have {})",
                    data.len()
                )));
            }
            let (head, tail) = data.split_at(8);
            let num = i64::from_le_bytes(head.try_into().unwrap());
            Ok((Value::Number(Number::from(num)), tail))
        }
        "Float" => {
            if data.len() < 8 {
                return Err(MaterializationError::Corrupt(format!(
                    "Insufficient bytes for float field (have {})",
                    data.len()
                )));
            }
            let (head, tail) = data.split_at(8);
            let num = f64::from_le_bytes(head.try_into().unwrap());
            let value = Number::from_f64(num)
                .map(Value::Number)
                .unwrap_or(Value::Null);
            Ok((value, tail))
        }
        "Boolean" => {
            if data.is_empty() {
                return Err(MaterializationError::Corrupt(
                    "Insufficient bytes for boolean field".into(),
                ));
            }
            let (head, tail) = data.split_at(1);
            Ok((Value::Bool(head[0] != 0), tail))
        }
        "String" => decode_string(data),
        _ => decode_json(data),
    }
}

fn encode_json_value(value: &Value, buffer: &mut Vec<u8>) -> Result<(), MaterializationError> {
    let serialized = serde_json::to_vec(value)?;
    let len = serialized.len() as u32;
    buffer.extend_from_slice(&len.to_le_bytes());
    buffer.extend_from_slice(&serialized);
    Ok(())
}

fn decode_json<'a>(data: &'a [u8]) -> Result<(Value, &'a [u8]), MaterializationError> {
    if data.len() < 4 {
        return Err(MaterializationError::Corrupt(
            "Insufficient bytes for JSON length".into(),
        ));
    }
    let (len_bytes, rest) = data.split_at(4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if rest.len() < len {
        return Err(MaterializationError::Corrupt(format!(
            "Insufficient bytes for JSON payload (expected {}, got {})",
            len,
            rest.len()
        )));
    }
    let (json_bytes, tail) = rest.split_at(len);
    let value = serde_json::from_slice(json_bytes)?;
    Ok((value, tail))
}

fn decode_string<'a>(data: &'a [u8]) -> Result<(Value, &'a [u8]), MaterializationError> {
    let (bytes, rest) = decode_bytes(data)?;
    let s = std::str::from_utf8(bytes)
        .map_err(|e| MaterializationError::Corrupt(format!("UTF-8 decode error: {e}")))?;
    Ok((Value::String(s.to_string()), rest))
}

fn set_null_bit(bitmap: &mut [u8], index: usize) {
    let byte = index / 8;
    let bit = index % 8;
    bitmap[byte] |= 1 << bit;
}

fn is_null(bitmap: &[u8], index: usize) -> bool {
    let byte = index / 8;
    let bit = index % 8;
    (bitmap[byte] & (1 << bit)) != 0
}

fn encode_bytes(bytes: &[u8], buffer: &mut Vec<u8>) {
    let len = bytes.len() as u32;
    buffer.extend_from_slice(&len.to_le_bytes());
    buffer.extend_from_slice(bytes);
}

fn decode_bytes<'a>(data: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), MaterializationError> {
    if data.len() < 4 {
        return Err(MaterializationError::Corrupt(
            "Insufficient bytes for length prefix".into(),
        ));
    }
    let (len_bytes, rest) = data.split_at(4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if rest.len() < len {
        return Err(MaterializationError::Corrupt(
            "Insufficient bytes for value payload".into(),
        ));
    }
    let (bytes, remaining) = rest.split_at(len);
    Ok((bytes, remaining))
}

fn extract_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_i64()
                .and_then(|i| if i >= 0 { Some(i as u64) } else { None })
        })
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
}

fn extract_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().map(|u| u as i64))
        .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
}

fn extract_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}
