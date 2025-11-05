use super::utils::{ByteEncoder, ValueExtractor};
use crate::engine::materialize::MaterializationError;
use crate::engine::types::ScalarValue;
use serde_json::json;

#[test]
fn extract_u64_from_u64() {
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(42_u64))), Some(42));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(0_u64))), Some(0));
    assert_eq!(
        ValueExtractor::extract_u64(&ScalarValue::from(json!(u64::MAX))),
        Some(u64::MAX)
    );
}

#[test]
fn extract_u64_from_i64() {
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(42_i64))), Some(42));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(0_i64))), Some(0));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(-1_i64))), None); // Negative
}

#[test]
fn extract_u64_from_string() {
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!("42"))), Some(42));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!("0"))), Some(0));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!("999"))), Some(999));
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!("not a number"))), None);
}

#[test]
fn extract_u64_from_other_types() {
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::Null), None);
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(true))), None);
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!(3.14))), None);
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!([]))), None);
    assert_eq!(ValueExtractor::extract_u64(&ScalarValue::from(json!({}))), None);
}

#[test]
fn extract_i64_from_i64() {
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!(42_i64))), Some(42));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!(-42_i64))), Some(-42));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!(0_i64))), Some(0));
}

#[test]
fn extract_i64_from_u64() {
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!(42_u64))), Some(42));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!(0_u64))), Some(0));
}

#[test]
fn extract_i64_from_string() {
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!("42"))), Some(42));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!("-42"))), Some(-42));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!("0"))), Some(0));
    assert_eq!(ValueExtractor::extract_i64(&ScalarValue::from(json!("not a number"))), None);
}

#[test]
fn extract_f64_from_f64() {
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!(3.14))), Some(3.14));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!(-3.14))), Some(-3.14));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!(0.0))), Some(0.0));
}

#[test]
fn extract_f64_from_string() {
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!("3.14"))), Some(3.14));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!("-3.14"))), Some(-3.14));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!("0.0"))), Some(0.0));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!("not a number"))), None);
}

#[test]
fn extract_f64_from_other_types() {
    // json!(42) is a Number which can be converted to f64
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!(42))), Some(42.0));
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::Null), None);
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!(true))), None);
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!([]))), None);
    assert_eq!(ValueExtractor::extract_f64(&ScalarValue::from(json!({}))), None);
}

#[test]
fn encode_bytes_prepends_length() {
    let mut buffer = Vec::new();
    let data = b"hello world";

    ByteEncoder::encode_bytes(data, &mut buffer);

    // Should have 4 bytes for length + data
    assert_eq!(buffer.len(), 4 + data.len());
    assert_eq!(
        u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]),
        11
    );
    assert_eq!(&buffer[4..], data);
}

#[test]
fn encode_bytes_handles_empty_data() {
    let mut buffer = Vec::new();
    let data = b"";

    ByteEncoder::encode_bytes(data, &mut buffer);

    assert_eq!(buffer.len(), 4);
    assert_eq!(
        u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]),
        0
    );
}

#[test]
fn encode_bytes_appends_to_buffer() {
    let mut buffer = vec![1, 2, 3, 4];
    let data = b"test";

    ByteEncoder::encode_bytes(data, &mut buffer);

    assert_eq!(buffer.len(), 12); // 4 initial + 4 length + 4 data
    assert_eq!(&buffer[0..4], &[1, 2, 3, 4]);
    assert_eq!(
        u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]),
        4
    );
    assert_eq!(&buffer[8..12], data);
}

#[test]
fn decode_bytes_reads_length_prefix() {
    let data = b"hello";
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
    encoded.extend_from_slice(data);

    let (decoded, remaining) = ByteEncoder::decode_bytes(&encoded).unwrap();

    assert_eq!(decoded, data);
    assert!(remaining.is_empty());
}

#[test]
fn decode_bytes_returns_remaining_data() {
    let data = b"hello";
    let extra = b"extra";
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
    encoded.extend_from_slice(data);
    encoded.extend_from_slice(extra);

    let (decoded, remaining) = ByteEncoder::decode_bytes(&encoded).unwrap();

    assert_eq!(decoded, data);
    assert_eq!(remaining, extra);
}

#[test]
fn decode_bytes_fails_on_insufficient_length_bytes() {
    let data = vec![1, 2, 3]; // Only 3 bytes, need 4 for length

    let err = ByteEncoder::decode_bytes(&data).unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(
        err.to_string()
            .contains("Insufficient bytes for length prefix")
    );
}

#[test]
fn decode_bytes_fails_on_insufficient_payload() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(100u32).to_le_bytes()); // Claim 100 bytes
    encoded.extend_from_slice(b"only 10 bytes"); // But only provide 14

    let err = ByteEncoder::decode_bytes(&encoded).unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(
        err.to_string()
            .contains("Insufficient bytes for value payload")
    );
}

#[test]
fn encode_decode_bytes_roundtrip() {
    let original = b"test data with special chars: !@#$%^&*()";
    let mut buffer = Vec::new();

    ByteEncoder::encode_bytes(original, &mut buffer);
    let (decoded, remaining) = ByteEncoder::decode_bytes(&buffer).unwrap();

    assert_eq!(decoded, original);
    assert!(remaining.is_empty());
}

#[test]
fn encode_decode_bytes_multiple_values() {
    let data1 = b"first";
    let data2 = b"second";
    let data3 = b"third";

    let mut buffer = Vec::new();
    ByteEncoder::encode_bytes(data1, &mut buffer);
    ByteEncoder::encode_bytes(data2, &mut buffer);
    ByteEncoder::encode_bytes(data3, &mut buffer);

    let (decoded1, rest) = ByteEncoder::decode_bytes(&buffer).unwrap();
    assert_eq!(decoded1, data1);

    let (decoded2, rest) = ByteEncoder::decode_bytes(rest).unwrap();
    assert_eq!(decoded2, data2);

    let (decoded3, rest) = ByteEncoder::decode_bytes(rest).unwrap();
    assert_eq!(decoded3, data3);
    assert!(rest.is_empty());
}
