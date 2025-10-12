use crate::engine::errors::StoreError;

use lz4_flex::block::{
    compress_prepend_size as lz4_compress, decompress_size_prepended as lz4_decompress,
};

pub const FLAG_COMPRESSED: u16 = 0x0001;
pub const ALGO_LZ4: u16 = 0x0001;

pub trait CompressionCodec {
    fn algo_id(&self) -> u16;
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>, StoreError>;
    fn decompress(&self, input: &[u8], _uncompressed_len: usize) -> Result<Vec<u8>, StoreError>;
}

pub struct Lz4Codec;

impl CompressionCodec for Lz4Codec {
    fn algo_id(&self) -> u16 {
        ALGO_LZ4
    }
    fn compress(&self, input: &[u8]) -> Result<Vec<u8>, StoreError> {
        Ok(lz4_compress(input))
    }
    fn decompress(&self, input: &[u8], _uncompressed_len: usize) -> Result<Vec<u8>, StoreError> {
        lz4_decompress(input).map_err(|e| StoreError::FlushFailed(format!("lz4 decompress: {e}")))
    }
}
