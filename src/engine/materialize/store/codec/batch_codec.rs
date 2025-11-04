use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::SchemaSnapshot;

use super::compression::{Compressor, Decompressor};
use super::decoder::Decoder;
use super::encoder::Encoder;
use super::types::EncodedFrame;
use crate::engine::materialize::store::frame::data::FrameData;
use crate::engine::materialize::store::frame::metadata::StoredFrameMeta;

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
        let encoded = Encoder::encode(schema, batch)?;

        // Serialize columnar format: [null_bitmap] [num_var_cols:u32] [length_tables] [column_data]
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&encoded.null_bitmap);

        // Write number of variable-size columns
        buffer.extend_from_slice(&(encoded.length_tables.len() as u32).to_le_bytes());

        // Write length tables (each is row_count * u32)
        for length_table in &encoded.length_tables {
            for &len in length_table {
                buffer.extend_from_slice(&len.to_le_bytes());
            }
        }

        // Write column data (columnar storage)
        for col_data in &encoded.column_data {
            buffer.extend_from_slice(col_data);
        }

        let compressed = Compressor::compress(&buffer)?;

        Ok(EncodedFrame {
            schema: schema.to_vec(),
            schema_hash: encoded.schema_hash,
            row_count: encoded.row_count as u32,
            min_timestamp: encoded.min_timestamp,
            max_timestamp: encoded.max_timestamp,
            max_event_id: encoded.max_event_id,
            null_bitmap_len: encoded.null_bitmap.len() as u32,
            compressed,
            uncompressed_len: buffer.len() as u32,
        })
    }

    fn decode(
        &self,
        meta: &StoredFrameMeta,
        data: FrameData,
    ) -> Result<ColumnBatch, MaterializationError> {
        let uncompressed_len = data.header.uncompressed_len as usize;

        // Use thread-local buffer pool for zero-allocation decompression
        // This reuses buffers across multiple frame decompressions on the same thread
        let payload = Decompressor::decompress_with_pool(&data.compressed, uncompressed_len)?;

        if payload.len() != uncompressed_len {
            return Err(MaterializationError::Corrupt(format!(
                "Payload length mismatch for frame {} (expected {}, got {})",
                meta.file_name,
                uncompressed_len,
                payload.len()
            )));
        }

        Decoder::decode(meta, payload, &data.header)
    }
}
