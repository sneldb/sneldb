use super::value_codec::ValueCodec;
use crate::engine::materialize::MaterializationError;
use serde_json::{Number, Value, json};

#[test]
fn encode_decode_timestamp_roundtrip() {
    let value = json!(1700000000_i64);
    let mut buffer = Vec::new();

    ValueCodec::encode_value(&value, "Timestamp", &mut buffer).unwrap();
    let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "Timestamp").unwrap();

    assert_eq!(decoded, json!(1700000000_i64));
    assert!(remaining.is_empty());
}

#[test]
fn encode_decode_integer_roundtrip() {
    let test_cases = vec![
        json!(0_i64),
        json!(42_i64),
        json!(-42_i64),
        json!(i64::MAX),
        json!(i64::MIN),
    ];

    for value in test_cases {
        let mut buffer = Vec::new();
        ValueCodec::encode_value(&value, "Integer", &mut buffer).unwrap();
        let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "Integer").unwrap();

        assert_eq!(decoded, value);
        assert!(remaining.is_empty());
    }
}

#[test]
fn encode_decode_float_roundtrip() {
    let test_cases = vec![
        json!(0.0),
        json!(3.14),
        json!(-3.14),
        json!(1e10),
        json!(1e-10),
    ];

    for value in test_cases {
        let mut buffer = Vec::new();
        ValueCodec::encode_value(&value, "Float", &mut buffer).unwrap();
        let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "Float").unwrap();

        if let (Some(orig), Some(dec)) = (value.as_f64(), decoded.as_f64()) {
            // Allow for floating point precision differences
            assert!((orig - dec).abs() < 1e-10);
        } else {
            panic!("Failed to decode float value");
        }
        assert!(remaining.is_empty());
    }
}

#[test]
fn encode_decode_boolean_roundtrip() {
    let test_cases = vec![json!(true), json!(false)];

    for value in test_cases {
        let mut buffer = Vec::new();
        ValueCodec::encode_value(&value, "Boolean", &mut buffer).unwrap();
        let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "Boolean").unwrap();

        assert_eq!(decoded, value);
        assert!(remaining.is_empty());
    }
}

#[test]
fn encode_decode_string_roundtrip() {
    let test_cases = vec![
        json!(""),
        json!("hello"),
        json!("hello world"),
        json!("special chars: !@#$%^&*()"),
        json!("unicode: 你好世界"),
        json!("newlines:\n\r\t"),
    ];

    for value in test_cases {
        let mut buffer = Vec::new();
        ValueCodec::encode_value(&value, "String", &mut buffer).unwrap();
        let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "String").unwrap();

        assert_eq!(decoded, value);
        assert!(remaining.is_empty());
    }
}

#[test]
fn encode_decode_json_roundtrip() {
    let test_cases = vec![
        json!({"key": "value"}),
        json!([1, 2, 3]),
        json!(null),
        json!(true),
        json!(false),
        json!(42),
        json!(3.14),
    ];

    for value in test_cases {
        let mut buffer = Vec::new();
        ValueCodec::encode_value(&value, "JSON", &mut buffer).unwrap();
        let (decoded, remaining) = ValueCodec::decode_value_fast(&buffer, "JSON").unwrap();

        assert_eq!(decoded, value);
        assert!(remaining.is_empty());
    }
}

#[test]
fn encode_boolean_from_string() {
    let mut buffer = Vec::new();
    ValueCodec::encode_value(&json!("true"), "Boolean", &mut buffer).unwrap();
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "Boolean").unwrap();
    assert_eq!(decoded, json!(true));

    let mut buffer = Vec::new();
    ValueCodec::encode_value(&json!("false"), "Boolean", &mut buffer).unwrap();
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "Boolean").unwrap();
    assert_eq!(decoded, json!(false));
}

#[test]
fn encode_integer_from_u64() {
    let value = json!(42_u64);
    let mut buffer = Vec::new();

    ValueCodec::encode_value(&value, "Integer", &mut buffer).unwrap();
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "Integer").unwrap();

    assert_eq!(decoded, json!(42_i64));
}

#[test]
fn decode_fails_on_insufficient_bytes_integer() {
    let data = vec![1, 2, 3, 4, 5, 6, 7]; // Only 7 bytes, need 8

    let err = ValueCodec::decode_value_fast(&data, "Integer").unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(err.to_string().contains("Insufficient bytes"));
}

#[test]
fn decode_fails_on_insufficient_bytes_float() {
    let data = vec![1, 2, 3, 4, 5, 6, 7]; // Only 7 bytes, need 8

    let err = ValueCodec::decode_value_fast(&data, "Float").unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(err.to_string().contains("Insufficient bytes"));
}

#[test]
fn decode_fails_on_insufficient_bytes_boolean() {
    let data = vec![]; // Empty, need at least 1 byte

    let err = ValueCodec::decode_value_fast(&data, "Boolean").unwrap_err();
    assert!(matches!(err, MaterializationError::Corrupt(_)));
    assert!(err.to_string().contains("Insufficient bytes"));
}

#[test]
fn encode_decode_multiple_values() {
    let values = vec![
        (json!(42_i64), "Integer"),
        (json!(3.14), "Float"),
        (json!(true), "Boolean"),
        (json!("hello"), "String"),
    ];

    let mut buffer = Vec::new();
    for (value, logical_type) in &values {
        ValueCodec::encode_value(value, logical_type, &mut buffer).unwrap();
    }

    let mut cursor = buffer.as_slice();
    for (expected, logical_type) in &values {
        let (decoded, remaining) = ValueCodec::decode_value_fast(cursor, logical_type).unwrap();
        assert_eq!(&decoded, expected);
        cursor = remaining;
    }
    assert!(cursor.is_empty());
}

#[test]
fn encode_fallback_to_json_for_invalid_types() {
    let value = json!({"nested": {"object": true}});
    let mut buffer = Vec::new();

    ValueCodec::encode_value(&value, "Integer", &mut buffer).unwrap(); // Should fallback to JSON
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "JSON").unwrap();

    assert_eq!(decoded, value);
}

#[test]
fn encode_string_from_non_string_value() {
    let value = json!(42); // Number, not string
    let mut buffer = Vec::new();

    ValueCodec::encode_value(&value, "String", &mut buffer).unwrap();
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "String").unwrap();

    // Should convert number to string
    assert_eq!(decoded, json!("42"));
}

#[test]
fn decode_unknown_type_falls_back_to_json() {
    let value = json!({"test": "data"});
    let mut buffer = Vec::new();

    ValueCodec::encode_value(&value, "JSON", &mut buffer).unwrap();
    let (decoded, _) = ValueCodec::decode_value_fast(&buffer, "UnknownType").unwrap();

    assert_eq!(decoded, value);
}
