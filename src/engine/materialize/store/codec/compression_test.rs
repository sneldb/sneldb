use super::compression::{Compressor, Decompressor};
use crate::engine::materialize::MaterializationError;

#[test]
fn compress_compresses_data() {
    let data = b"hello world this is a test string that should compress well";
    let compressed = Compressor::compress(data).unwrap();

    // Compressed data should be smaller (or at least different)
    assert!(compressed.len() > 0);
    assert_ne!(compressed, data);
}

#[test]
fn compress_handles_empty_data() {
    let data = b"";
    let compressed = Compressor::compress(data).unwrap();

    // Even empty data should produce some compressed output (LZ4 header)
    assert!(compressed.len() >= 4); // At least size header
}

#[test]
fn compress_handles_large_data() {
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let compressed = Compressor::compress(&data).unwrap();

    assert!(compressed.len() > 0);
    assert!(compressed.len() < data.len() * 2); // Should not expand too much
}

#[test]
fn decompress_with_pool_decompresses_data() {
    let original = b"hello world this is a test string";
    let compressed = Compressor::compress(original).unwrap();

    let decompressed = Decompressor::decompress_with_pool(&compressed, original.len()).unwrap();

    assert_eq!(decompressed, original);
}

#[test]
fn decompress_with_pool_roundtrip() {
    let data1 = (0..1000).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    let data2 = (0..10000).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    let test_cases: Vec<&[u8]> = vec![b"short", b"medium length string", &data1, &data2];

    for original in test_cases {
        let compressed = Compressor::compress(original).unwrap();
        let decompressed = Decompressor::decompress_with_pool(&compressed, original.len()).unwrap();
        assert_eq!(decompressed.as_slice(), original);
    }
}

#[test]
fn decompress_with_pool_fails_on_too_short_compressed_data() {
    let data = vec![1, 2, 3]; // Less than 4 bytes (LZ4 header)

    let err = Decompressor::decompress_with_pool(&data, 100).unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(err.to_string().contains("Compressed data too short"));
}

#[test]
fn decompress_with_pool_reuses_buffer() {
    let data1 = b"first compression test";
    let data2 = b"second compression test";
    let data3 = b"third compression test";

    let compressed1 = Compressor::compress(data1).unwrap();
    let compressed2 = Compressor::compress(data2).unwrap();
    let compressed3 = Compressor::compress(data3).unwrap();

    // Decompress multiple times - should reuse thread-local buffer
    let decompressed1 = Decompressor::decompress_with_pool(&compressed1, data1.len()).unwrap();
    let decompressed2 = Decompressor::decompress_with_pool(&compressed2, data2.len()).unwrap();
    let decompressed3 = Decompressor::decompress_with_pool(&compressed3, data3.len()).unwrap();

    assert_eq!(decompressed1.as_slice(), data1);
    assert_eq!(decompressed2.as_slice(), data2);
    assert_eq!(decompressed3.as_slice(), data3);
}

#[test]
fn compress_handles_repeated_patterns() {
    // Highly compressible data
    let data = b"AAAA".repeat(1000);
    let compressed = Compressor::compress(&data).unwrap();

    // Should compress significantly
    assert!(compressed.len() < data.len());
}

#[test]
fn decompress_with_pool_handles_zero_length() {
    let data = b"";
    let compressed = Compressor::compress(data).unwrap();

    let decompressed = Decompressor::decompress_with_pool(&compressed, 0).unwrap();
    assert_eq!(decompressed.len(), 0);
}
