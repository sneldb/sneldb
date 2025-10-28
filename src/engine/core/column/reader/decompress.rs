use crate::engine::core::column::compression::{CompressionCodec, Lz4Codec};
use crate::engine::errors::QueryExecutionError;

use super::buffer::decompress_into_pool;

pub fn decompress_block(
    compressed: &[u8],
    expected_uncomp_len: usize,
) -> Result<Vec<u8>, QueryExecutionError> {
    // Blocks are size-prepended by our writer; extract expected out_len and strip header if present
    let (payload, out_len) = if compressed.len() >= 4 {
        let mut sz = [0u8; 4];
        sz.copy_from_slice(&compressed[..4]);
        let out_len = u32::from_le_bytes(sz) as usize;
        (&compressed[4..], out_len)
    } else {
        (compressed, expected_uncomp_len)
    };
    let codec = Lz4Codec;
    decompress_into_pool(out_len, |dst| {
        CompressionCodec::decompress_into(&codec, payload, dst)
            .map_err(|e| QueryExecutionError::ColRead(format!("decompress: {e}")))
    })
}
