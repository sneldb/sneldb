#![cfg(test)]

use crate::engine::core::filter::surf_encoding::{
    encode_f64_into, encode_i64_into, encode_u64_into, encode_value, encode_value_into,
};
use serde_json::json;

#[test]
fn integral_float_normalizes_to_integer_encoding() {
    let b_i = encode_value(&json!(1)).unwrap();
    let b_u = encode_value(&json!(1u64)).unwrap();
    let b_f = encode_value(&json!(1.0)).unwrap();

    assert_eq!(b_i, b_u);
    assert_eq!(b_i, b_f);
}

#[test]
fn float_encoding_is_order_preserving() {
    // Numeric order: -3.5 < -1.0 < -0.0 == 0.0 < 2.25 < 10.5
    let floats = vec![-3.5, -1.0, -0.0, 0.0, 2.25, 10.5];
    let mut pairs: Vec<(Vec<u8>, f64)> = floats
        .iter()
        .map(|f| (encode_value(&json!(f)).unwrap(), *f))
        .collect();

    // Sort by encoded bytes (lexicographic)
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted_by_bytes: Vec<f64> = pairs.iter().map(|(_, f)| *f).collect();

    // Expected numeric order (treat -0.0 and 0.0 equal)
    let expected = vec![-3.5, -1.0, -0.0, 0.0, 2.25, 10.5];
    assert_eq!(sorted_by_bytes, expected);
}

#[test]
fn bool_encoding_is_minimal_and_ordered() {
    let mut scratch_f = [0u8; 8];
    let f = encode_value_into(&json!(false), &mut scratch_f).unwrap();
    let mut scratch_t = [0u8; 8];
    let t = encode_value_into(&json!(true), &mut scratch_t).unwrap();
    assert_eq!(f, &[0u8][..]);
    assert_eq!(t, &[1u8][..]);
    assert!(f < t);
}

#[test]
fn string_encoding_borrows_without_copy() {
    let s = String::from("abc");
    let mut scratch = [0u8; 8];
    let v = json!(s.clone());
    let out = encode_value_into(&v, &mut scratch).unwrap();
    assert_eq!(out, s.as_bytes());
}

#[test]
fn i64_encoding_is_order_preserving() {
    let vals = [i64::MIN, -1, 0, 1, i64::MAX];
    let mut pairs: Vec<(Vec<u8>, i64)> = Vec::with_capacity(vals.len());
    let mut scratch = [0u8; 8];
    for &v in &vals {
        let enc = encode_i64_into(v, &mut scratch).to_vec();
        pairs.push((enc, v));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted: Vec<i64> = pairs.into_iter().map(|(_, v)| v).collect();
    assert_eq!(sorted, vals);
}

#[test]
fn u64_encoding_is_order_preserving() {
    let vals = [0u64, 1, 1u64 << 32, 1u64 << 63, u64::MAX];
    let mut pairs: Vec<(Vec<u8>, u64)> = Vec::with_capacity(vals.len());
    let mut scratch = [0u8; 8];
    for &v in &vals {
        let enc = encode_u64_into(v, &mut scratch).to_vec();
        pairs.push((enc, v));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted: Vec<u64> = pairs.into_iter().map(|(_, v)| v).collect();
    assert_eq!(sorted, vals);
}

#[test]
fn f64_encoding_handles_extremes_and_order() {
    let vals = [f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY];
    let mut pairs: Vec<(Vec<u8>, f64)> = Vec::with_capacity(vals.len());
    let mut scratch = [0u8; 8];
    for &v in &vals {
        let enc = encode_f64_into(v, &mut scratch).to_vec();
        pairs.push((enc, v));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted: Vec<f64> = pairs.into_iter().map(|(_, v)| v).collect();
    assert_eq!(sorted, vals);
}

#[test]
fn wrapper_matches_into_for_various_values() {
    let cases = vec![
        json!("abc"),
        json!(false),
        json!(true),
        json!(0),
        json!(1),
        json!(1u64),
        json!(1.0),
        json!(-1234567890i64),
        json!(std::u64::MAX),
    ];
    for v in cases {
        let mut scratch = [0u8; 8];
        let into = encode_value_into(&v, &mut scratch).unwrap();
        let wrap = encode_value(&v).unwrap();
        assert_eq!(into, &wrap[..]);
    }
}
