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
