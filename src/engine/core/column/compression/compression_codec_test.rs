use crate::engine::core::column::compression::{CompressionCodec, Lz4Codec};

#[test]
fn lz4_roundtrip_prepend_size() {
    let codec = Lz4Codec;
    let data = b"0123456789abcdef".to_vec();
    let comp = CompressionCodec::compress(&codec, &data).expect("compress");
    // decompress() understands size-prepended blocks
    let out = CompressionCodec::decompress(&codec, &comp, data.len()).expect("decompress");
    assert_eq!(out, data);
}

#[test]
fn lz4_decompress_into_exact_size() {
    let codec = Lz4Codec;
    let data = b"hello world".to_vec();
    let comp = CompressionCodec::compress(&codec, &data).expect("compress");
    let mut out = vec![0u8; data.len()];
    // Strip the 4-byte size header when using decompress_into
    CompressionCodec::decompress_into(&codec, &comp[4..], &mut out).expect("decompress_into");
    assert_eq!(out, data);
}
