use std::sync::Arc;

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::cache::DecompressedBlock;

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
