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

// Helper function to build typed i64 column with optional nulls
// Ensures payload_start is 8-byte aligned for proper unsafe pointer operations
fn build_typed_i64(values: &[Option<i64>]) -> ColumnValues {
    let row_count = values.len();
    let null_bytes = (row_count + 7) / 8;
    // Align payload_start to 8 bytes for proper i64 alignment
    let payload_start = (null_bytes + 7) & !7; // Round up to nearest 8-byte boundary
    let total_bytes = payload_start + row_count * 8;
    let mut bytes = vec![0u8; total_bytes];
    let mut has_null = false;

    for (idx, value) in values.iter().enumerate() {
        match value {
            Some(v) => {
                let offset = payload_start + idx * 8;
                bytes[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
            }
            None => {
                has_null = true;
                bytes[idx / 8] |= 1 << (idx % 8);
            }
        }
    }

    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    let nulls = has_null.then_some((0, null_bytes));
    ColumnValues::new_typed_i64(block, payload_start, row_count, nulls)
}

#[test]
fn get_i64_slice_with_validity_basic_no_nulls() {
    let cv = build_typed_i64(&[Some(10), Some(20), Some(30), Some(40)]);
    let result = cv.get_i64_slice_with_validity(0, 4).unwrap();
    assert_eq!(result.0, vec![10, 20, 30, 40]);
    assert_eq!(result.1, vec![true, true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_partial_range() {
    let cv = build_typed_i64(&[Some(10), Some(20), Some(30), Some(40), Some(50)]);
    let result = cv.get_i64_slice_with_validity(1, 4).unwrap();
    assert_eq!(result.0, vec![20, 30, 40]);
    assert_eq!(result.1, vec![true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_single_element() {
    let cv = build_typed_i64(&[Some(42)]);
    let result = cv.get_i64_slice_with_validity(0, 1).unwrap();
    assert_eq!(result.0, vec![42]);
    assert_eq!(result.1, vec![true]);
}

#[test]
fn get_i64_slice_with_validity_with_nulls() {
    // Index 1 and 3 are null
    let cv = build_typed_i64(&[Some(10), None, Some(30), None, Some(50)]);
    let result = cv.get_i64_slice_with_validity(0, 5).unwrap();
    // Null values read as 0 (uninitialized payload bytes)
    assert_eq!(result.0.len(), 5);
    assert_eq!(result.0[0], 10);
    assert_eq!(result.0[1], 0); // Null value
    assert_eq!(result.0[2], 30);
    assert_eq!(result.0[3], 0); // Null value
    assert_eq!(result.0[4], 50);
    assert_eq!(result.1, vec![true, false, true, false, true]);
}

#[test]
fn get_i64_slice_with_validity_with_nulls_partial() {
    let cv = build_typed_i64(&[Some(10), None, Some(30), None, Some(50)]);
    let result = cv.get_i64_slice_with_validity(1, 4).unwrap();
    assert_eq!(result.0.len(), 3);
    assert_eq!(result.1, vec![false, true, false]); // Index 1, 2, 3
}

#[test]
fn get_i64_slice_with_validity_empty_range() {
    let cv = build_typed_i64(&[Some(10), Some(20), Some(30)]);
    let result = cv.get_i64_slice_with_validity(1, 1).unwrap();
    assert_eq!(result.0, Vec::<i64>::new());
    assert_eq!(result.1, Vec::<bool>::new());
}

#[test]
fn get_i64_slice_with_validity_start_equals_end() {
    let cv = build_typed_i64(&[Some(10), Some(20)]);
    let result = cv.get_i64_slice_with_validity(2, 2).unwrap();
    assert_eq!(result.0, Vec::<i64>::new());
    assert_eq!(result.1, Vec::<bool>::new());
}

#[test]
fn get_i64_slice_with_validity_start_greater_than_end() {
    let cv = build_typed_i64(&[Some(10), Some(20)]);
    let result = cv.get_i64_slice_with_validity(2, 1).unwrap();
    assert_eq!(result.0, Vec::<i64>::new());
    assert_eq!(result.1, Vec::<bool>::new());
}

#[test]
fn get_i64_slice_with_validity_start_out_of_bounds() {
    let cv = build_typed_i64(&[Some(10), Some(20)]);
    assert_eq!(cv.get_i64_slice_with_validity(3, 4), None);
}

#[test]
fn get_i64_slice_with_validity_end_out_of_bounds() {
    let cv = build_typed_i64(&[Some(10), Some(20)]);
    assert_eq!(cv.get_i64_slice_with_validity(0, 3), None);
}

#[test]
fn get_i64_slice_with_validity_both_out_of_bounds() {
    let cv = build_typed_i64(&[Some(10), Some(20)]);
    assert_eq!(cv.get_i64_slice_with_validity(5, 6), None);
}

#[test]
fn get_i64_slice_with_validity_start_at_boundary() {
    let cv = build_typed_i64(&[Some(10), Some(20), Some(30)]);
    let result = cv.get_i64_slice_with_validity(2, 3).unwrap();
    assert_eq!(result.0, vec![30]);
    assert_eq!(result.1, vec![true]);
}

#[test]
fn get_i64_slice_with_validity_end_at_boundary() {
    let cv = build_typed_i64(&[Some(10), Some(20), Some(30)]);
    let result = cv.get_i64_slice_with_validity(0, 3).unwrap();
    assert_eq!(result.0, vec![10, 20, 30]);
    assert_eq!(result.1, vec![true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_negative_values() {
    let cv = build_typed_i64(&[Some(-10), Some(-20), Some(30)]);
    let result = cv.get_i64_slice_with_validity(0, 3).unwrap();
    assert_eq!(result.0, vec![-10, -20, 30]);
    assert_eq!(result.1, vec![true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_large_values() {
    let cv = build_typed_i64(&[Some(i64::MAX), Some(i64::MIN), Some(0)]);
    let result = cv.get_i64_slice_with_validity(0, 3).unwrap();
    assert_eq!(result.0, vec![i64::MAX, i64::MIN, 0]);
    assert_eq!(result.1, vec![true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_all_nulls() {
    // When all values are null, payload bytes are still allocated but contain zeros
    // The validity bitmap correctly marks them as null
    let cv = build_typed_i64(&[None, None, None]);
    let result = cv.get_i64_slice_with_validity(0, 3).unwrap();
    assert_eq!(result.0.len(), 3);
    // Values will be read as 0 (uninitialized payload), but validity marks them as null
    assert_eq!(result.0, vec![0, 0, 0]);
    assert_eq!(result.1, vec![false, false, false]);
}

#[test]
fn get_i64_slice_with_validity_no_nulls_bitmap() {
    // Create column without nulls bitmap (all values valid)
    let mut bytes = Vec::new();
    for n in [10i64, 20, 30] {
        bytes.extend_from_slice(&n.to_le_bytes());
    }
    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    let cv = ColumnValues::new_typed_i64(block, 0, 3, None);

    let result = cv.get_i64_slice_with_validity(0, 3).unwrap();
    assert_eq!(result.0, vec![10, 20, 30]);
    assert_eq!(result.1, vec![true, true, true]);
}

#[test]
fn get_i64_slice_with_validity_not_typed_i64() {
    // Create a non-typed column
    let (block, ranges) = make_block_with_values(&["10", "20"]);
    let cv = ColumnValues::new(block, ranges);

    assert_eq!(cv.get_i64_slice_with_validity(0, 2), None);
}

#[test]
fn get_i64_slice_with_validity_nulls_bitmap_multiple_bytes() {
    // Create column with 10 values, requiring 2 bytes for nulls bitmap
    let values: Vec<Option<i64>> = (0..10)
        .map(|i| if i % 2 == 0 { Some(i as i64) } else { None })
        .collect();
    let cv = build_typed_i64(&values);

    let result = cv.get_i64_slice_with_validity(0, 10).unwrap();
    assert_eq!(result.0.len(), 10);
    assert_eq!(
        result.1,
        vec![
            true, false, true, false, true, false, true, false, true, false
        ]
    );
}

#[test]
fn get_i64_slice_with_validity_block_bounds_exceeded() {
    // Create a column with insufficient bytes in block
    let mut bytes = vec![0u8; 8]; // Only 1 i64 value worth of bytes
    bytes[0..8].copy_from_slice(&10i64.to_le_bytes());
    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    // Claim we have 2 rows but only 1 row worth of data
    let cv = ColumnValues::new_typed_i64(block, 0, 2, None);

    // Should fail when trying to read beyond available bytes
    assert_eq!(cv.get_i64_slice_with_validity(0, 2), None);
}

#[test]
fn get_i64_slice_with_validity_zero_length_column() {
    let cv = build_typed_i64(&[]);
    let result = cv.get_i64_slice_with_validity(0, 0).unwrap();
    assert_eq!(result.0, Vec::<i64>::new());
    assert_eq!(result.1, Vec::<bool>::new());
}

#[test]
fn get_i64_slice_with_validity_consistency_with_get_i64_at() {
    let cv = build_typed_i64(&[Some(10), None, Some(30), Some(40)]);

    // Get slice
    let (slice_values, slice_valid) = cv.get_i64_slice_with_validity(0, 4).unwrap();

    // Compare with individual get_i64_at calls
    for i in 0..4 {
        let individual = cv.get_i64_at(i);
        if slice_valid[i] {
            assert_eq!(individual, Some(slice_values[i]));
        } else {
            assert_eq!(individual, None);
        }
    }
}
