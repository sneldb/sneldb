use crate::engine::core::column::compression::{CompressionCodec, Lz4Codec};
use crate::engine::core::column::reader::decompress::decompress_block;

#[test]
fn decompress_block_reads_size_prefix() {
    let codec = Lz4Codec;
    let data = b"abcdef".to_vec();
    let comp = CompressionCodec::compress(&codec, &data).expect("compress");
    let out = decompress_block(&comp, data.len()).expect("decompress");
    assert_eq!(out, data);
}


