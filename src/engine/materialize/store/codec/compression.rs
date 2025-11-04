use std::cell::RefCell;

use crate::engine::core::column::compression::Lz4Codec;
use crate::engine::core::column::compression::compression_codec::CompressionCodec;
use crate::engine::materialize::MaterializationError;

// Thread-local buffer pool for decompression to avoid allocations
thread_local! {
    static DECOMPRESS_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

pub struct Decompressor;

impl Decompressor {
    pub fn decompress_with_pool(
        compressed: &[u8],
        uncompressed_len: usize,
    ) -> Result<Vec<u8>, MaterializationError> {
        DECOMPRESS_BUFFER.with(|slot| {
            let mut buf = {
                let mut cell = slot.borrow_mut();
                std::mem::take(&mut *cell)
            };

            // Resize buffer if needed (reuse existing capacity when possible)
            if buf.capacity() < uncompressed_len {
                buf.reserve(uncompressed_len.saturating_sub(buf.capacity()));
            }
            buf.resize(uncompressed_len, 0);

            // LZ4 compress() uses compress_prepend_size which adds a 4-byte size header.
            // decompress_into() expects the data WITHOUT the size header, so we need to strip it.
            if compressed.len() < 4 {
                return Err(MaterializationError::Corrupt(
                    "Compressed data too short to contain size header".into(),
                ));
            }
            let compressed_payload = &compressed[4..];

            // Decompress directly into the buffer
            let codec = Lz4Codec;
            codec
                .decompress_into(compressed_payload, &mut buf)
                .map_err(|e| MaterializationError::Corrupt(format!("LZ4 decompress: {e}")))?;

            // Return the buffer (thread-local slot will get a new empty Vec)
            Ok(buf)
        })
    }
}

pub struct Compressor;

impl Compressor {
    pub fn compress(data: &[u8]) -> Result<Vec<u8>, MaterializationError> {
        let codec = Lz4Codec;
        codec
            .compress(data)
            .map_err(|e| MaterializationError::Corrupt(format!("LZ4 compress: {e}")))
    }
}
