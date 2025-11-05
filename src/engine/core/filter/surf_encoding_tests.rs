#![cfg(test)]

use crate::engine::core::filter::surf_encoding::encode_value;
use crate::engine::types::ScalarValue;
use serde_json::json;

#[test]
fn integral_float_normalizes_to_integer_encoding() {
    let b_i = encode_value(&ScalarValue::from(json!(1))).unwrap();
    let b_u = encode_value(&ScalarValue::from(json!(1u64))).unwrap();
    let b_f = encode_value(&ScalarValue::from(json!(1.0))).unwrap();

    assert_eq!(b_i, b_u);
    assert_eq!(b_i, b_f);
}

#[test]
fn float_encoding_is_order_preserving() {
    // Numeric order: -3.5 < -1.0 < -0.0 == 0.0 < 2.25 < 10.5
    let floats = vec![-3.5, -1.0, -0.0, 0.0, 2.25, 10.5];
    let mut pairs: Vec<(Vec<u8>, f64)> = floats
        .iter()
        .map(|f| (encode_value(&ScalarValue::from(json!(f))).unwrap(), *f))
        .collect();

    // Sort by encoded bytes (lexicographic)
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted_by_bytes: Vec<f64> = pairs.iter().map(|(_, f)| *f).collect();

    // Expected numeric order (treat -0.0 and 0.0 equal)
    let expected = vec![-3.5, -1.0, -0.0, 0.0, 2.25, 10.5];
    assert_eq!(sorted_by_bytes, expected);
}
