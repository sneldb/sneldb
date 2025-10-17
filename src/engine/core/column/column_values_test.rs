use std::sync::Arc;

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::cache::DecompressedBlock;

fn make_block(bytes: Vec<u8>) -> Arc<DecompressedBlock> {
    Arc::new(DecompressedBlock::from_bytes(bytes))
}

#[test]
fn ranges_string_access_and_validation() {
    // Build a simple bytes buffer: "hi" and invalid utf8 [0xFF]
    let mut bytes = Vec::new();
    let s1 = b"hi".to_vec();
    let bad = vec![0xFF];
    let start1 = 0usize;
    bytes.extend_from_slice(&s1);
    let start2 = bytes.len();
    bytes.extend_from_slice(&bad);

    let block = make_block(bytes);
    let ranges = vec![(start1, 2), (start2, 1)];
    let cv = ColumnValues::new(block, ranges);

    assert_eq!(cv.len(), 2);
    assert!(!cv.is_empty());
    assert_eq!(cv.get_str_at(0).unwrap(), "hi");
    assert!(cv.get_str_at(1).is_none(), "invalid utf8 should be None");
    assert!(!cv.validate_utf8(), "column contains invalid utf8");

    // get_str_at_unchecked works only when indexes are valid utf8
    let s = cv.get_str_at_unchecked(0).unwrap();
    assert_eq!(s, "hi");
    assert!(
        cv.get_str_at_unchecked(1).is_some(),
        "unchecked still returns Some, but content may be garbage"
    );
}

#[test]
fn typed_i64_access_and_len() {
    // Payload of three i64 values
    let mut bytes = Vec::new();
    for n in [10i64, -5, 42] {
        bytes.extend_from_slice(&n.to_le_bytes());
    }
    let block = make_block(bytes);
    let cv = ColumnValues::new_typed_i64(Arc::clone(&block), 0, 3, None);

    assert_eq!(cv.len(), 3);
    assert!(cv.is_empty(), "typed views have empty ranges by design");
    assert_eq!(cv.get_i64_at(0), Some(10));
    assert_eq!(cv.get_i64_at(1), Some(-5));
    assert_eq!(cv.get_i64_at(2), Some(42));
    assert_eq!(cv.get_i64_at(3), None);
    assert!(
        cv.get_str_at(0).is_none(),
        "no string view for typed numeric"
    );
}

#[test]
fn typed_u64_with_nulls_bitmap() {
    // Two values with nulls bitmap marking index 1 as null
    // Nulls bitmap: bit1 set -> 0b0000_0010
    let nulls = vec![0b0000_0010u8];
    let mut payload = Vec::new();
    for n in [100u64, 200u64] {
        payload.extend_from_slice(&n.to_le_bytes());
    }
    let mut bytes = nulls.clone();
    bytes.extend_from_slice(&payload);
    let block = make_block(bytes);
    let cv =
        ColumnValues::new_typed_u64(Arc::clone(&block), nulls.len(), 2, Some((0, nulls.len())));

    assert_eq!(cv.len(), 2);
    assert_eq!(cv.get_u64_at(0), Some(100));
    assert_eq!(cv.get_u64_at(1), None, "null row should be None");
}

#[test]
fn typed_f64_access() {
    let mut bytes = Vec::new();
    for f in [1.5f64, -2.25, 0.0] {
        bytes.extend_from_slice(&f.to_le_bytes());
    }
    let block = make_block(bytes);
    let cv = ColumnValues::new_typed_f64(Arc::clone(&block), 0, 3, None);
    assert_eq!(cv.get_f64_at(0), Some(1.5));
    assert_eq!(cv.get_f64_at(1), Some(-2.25));
    assert_eq!(cv.get_f64_at(2), Some(0.0));
}

#[test]
fn typed_bool_access_with_nulls() {
    // Values bitset: 0b0000_1010 -> index 1 and 3 are true
    // Nulls bitset:  0b0000_0100 -> index 2 is null
    let nulls = vec![0b0000_0100u8];
    let values = vec![0b0000_1010u8];
    let mut bytes = nulls.clone();
    bytes.extend_from_slice(&values);
    let block = make_block(bytes);
    let cv =
        ColumnValues::new_typed_bool(Arc::clone(&block), nulls.len(), 4, Some((0, nulls.len())));

    assert_eq!(cv.get_bool_at(0), Some(false));
    assert_eq!(cv.get_bool_at(1), Some(true));
    assert_eq!(cv.get_bool_at(2), None, "null bit set");
    assert_eq!(cv.get_bool_at(3), Some(true));
    assert_eq!(cv.get_bool_at(4), None);
}

#[test]
fn warm_numeric_cache_on_ranges_allows_i64_parsing() {
    // Two numeric strings: "10", "-5"
    let s1 = b"10".to_vec();
    let s2 = b"-5".to_vec();
    let mut bytes = Vec::new();
    let start1 = 0usize;
    bytes.extend_from_slice(&s1);
    let start2 = bytes.len();
    bytes.extend_from_slice(&s2);
    let block = make_block(bytes);
    let ranges = vec![(start1, 2), (start2, 2)];
    let cv = ColumnValues::new(block, ranges);

    // Pre-warm cache and validate
    cv.warm_numeric_cache();
    assert_eq!(cv.get_i64_at(0), Some(10));
    assert_eq!(cv.get_i64_at(1), Some(-5));
}

// (dedup of imports at top intentionally removed)

fn make_block_with_values(values: &[&str]) -> (Arc<DecompressedBlock>, Vec<(usize, usize)>) {
    // Layout per value: [u16 len][bytes]
    let mut bytes: Vec<u8> = Vec::new();
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for v in values {
        let v_bytes = v.as_bytes();
        let len = v_bytes.len() as u16;
        let start = bytes.len();
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(v_bytes);
        ranges.push((start + 2, v_bytes.len()));
    }
    (Arc::new(DecompressedBlock::from_bytes(bytes)), ranges)
}

#[test]
fn column_values_len_and_empty() {
    let (block, ranges) = make_block_with_values(&[]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.len(), 0);
    assert!(cv.is_empty());
}

#[test]
fn get_str_at_returns_expected_strings() {
    let (block, ranges) = make_block_with_values(&["alpha", "beta", "gamma"]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.len(), 3);
    assert_eq!(cv.get_str_at(0), Some("alpha"));
    assert_eq!(cv.get_str_at(1), Some("beta"));
    assert_eq!(cv.get_str_at(2), Some("gamma"));
    assert_eq!(cv.get_str_at(3), None);
}

#[test]
fn get_i64_at_parses_integers_and_handles_invalid() {
    let (block, ranges) = make_block_with_values(&["42", "-7", "x"]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.get_i64_at(0), Some(42));
    assert_eq!(cv.get_i64_at(1), Some(-7));
    assert_eq!(cv.get_i64_at(2), None); // invalid integer
}

#[test]
fn equality_ignores_block_identity_compares_ranges() {
    let (block1, ranges1) = make_block_with_values(&["a", "b"]);
    let cv1 = ColumnValues::new(Arc::clone(&block1), ranges1.clone());
    // Create different backing block with same logical ranges layout
    let (block2, _ranges2) = make_block_with_values(&["xx", "yy"]);
    let cv2 = ColumnValues::new(block2, ranges1);
    assert_eq!(cv1, cv2);
}

#[test]
fn get_i64_at_out_of_bounds_returns_none() {
    let (block, ranges) = make_block_with_values(&["10"]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.get_i64_at(1), None);
}

#[test]
fn get_str_at_invalid_utf8_returns_none() {
    // Manually craft invalid UTF-8 bytes and point ranges to them directly
    let bytes = vec![0xff, 0xfe];
    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    let cv = ColumnValues::new(Arc::clone(&block), vec![(0, 1), (1, 1)]);
    assert_eq!(cv.get_str_at(0), None);
    assert_eq!(cv.get_str_at(1), None);
}

#[test]
fn get_i64_at_handles_extremes_and_formats() {
    let (block, ranges) = make_block_with_values(&[
        "9223372036854775807",  // i64::MAX
        "-9223372036854775808", // i64::MIN
        "9223372036854775808",  // overflow
        "-9223372036854775809", // underflow
        "+7",                   // plus sign
        " 5",                   // leading space
        "4.2",                  // decimal
        "",                     // empty string
        "000",                  // leading zeros
        "0",                    // zero
    ]);
    let cv = ColumnValues::new(block, ranges);

    assert_eq!(cv.get_i64_at(0), Some(9223372036854775807));
    assert_eq!(cv.get_i64_at(1), Some(-9223372036854775808));
    assert_eq!(cv.get_i64_at(2), None);
    assert_eq!(cv.get_i64_at(3), None);
    assert_eq!(cv.get_i64_at(4), Some(7));
    assert_eq!(cv.get_i64_at(5), None);
    assert_eq!(cv.get_i64_at(6), None);
    assert_eq!(cv.get_i64_at(7), None);
    assert_eq!(cv.get_i64_at(8), Some(0));
    assert_eq!(cv.get_i64_at(9), Some(0));

    // Cache should make repeated calls consistent
    assert_eq!(cv.get_i64_at(0), Some(9223372036854775807));
}

#[test]
fn get_str_at_handles_empty_and_unicode() {
    let (block, ranges) = make_block_with_values(&["", "🦀", "naïve"]);
    let cv = ColumnValues::new(block, ranges);
    assert_eq!(cv.get_str_at(0), Some(""));
    assert_eq!(cv.get_str_at(1), Some("🦀"));
    assert_eq!(cv.get_str_at(2), Some("naïve"));
}

#[test]
fn inequality_when_ranges_differ() {
    let (block, ranges_a) = make_block_with_values(&["a", "b"]);
    let (_block_b, mut ranges_b) = make_block_with_values(&["a", "b"]);
    // mutate ranges_b to differ (swap order)
    ranges_b.swap(0, 1);
    let cv_a = ColumnValues::new(Arc::clone(&block), ranges_a);
    let cv_b = ColumnValues::new(block, ranges_b);
    assert_ne!(cv_a, cv_b);
}
