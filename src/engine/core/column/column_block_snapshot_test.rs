use std::sync::Arc;

use crate::engine::core::column::column_block_snapshot::ColumnBlockSnapshot;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::column::format::PhysicalType;
use crate::engine::core::read::cache::DecompressedBlock;
use serde_json::{Number, Value};

fn build_varbytes(values: &[&str]) -> ColumnValues {
    let mut ranges = Vec::with_capacity(values.len());
    let mut payload = Vec::new();
    let mut cursor = 0usize;
    for value in values {
        let bytes = value.as_bytes();
        ranges.push((cursor, bytes.len()));
        payload.extend_from_slice(bytes);
        cursor += bytes.len();
    }
    let block = Arc::new(DecompressedBlock::from_bytes(payload));
    ColumnValues::new(block, ranges)
}

fn build_i64(values: &[Option<i64>]) -> ColumnValues {
    let row_count = values.len();
    let null_bytes = (row_count + 7) / 8;
    let mut bytes = vec![0u8; null_bytes + row_count * 8];
    let payload_start = null_bytes;
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

fn build_bool(values: &[Option<bool>]) -> ColumnValues {
    let row_count = values.len();
    let bits_len = (row_count + 7) / 8;
    let mut bytes = vec![0u8; bits_len * 2];
    let payload_start = bits_len;
    let mut has_null = false;

    for (idx, value) in values.iter().enumerate() {
        match value {
            Some(true) => bytes[payload_start + idx / 8] |= 1 << (idx % 8),
            Some(false) => {} // leave zero bit
            None => {
                has_null = true;
                bytes[idx / 8] |= 1 << (idx % 8);
            }
        }
    }

    let block = Arc::new(DecompressedBlock::from_bytes(bytes));
    let nulls = has_null.then_some((0, bits_len));
    ColumnValues::new_typed_bool(block, payload_start, row_count, nulls)
}

#[test]
fn varbytes_snapshot_roundtrips_to_strings() {
    let values = build_varbytes(&["alpha", "", "β"]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    assert_eq!(snapshot.physical_type(), PhysicalType::VarBytes);
    assert_eq!(snapshot.len(), 3);
    assert!(!snapshot.is_empty());

    let strings = snapshot.to_strings();
    assert_eq!(strings, vec!["alpha", "", "β"]);
}

#[test]
fn numeric_snapshot_formats_values_and_preserves_nulls() {
    let values = build_i64(&[Some(42), None, Some(-7)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    assert_eq!(snapshot.len(), 3);
    assert_eq!(snapshot.physical_type(), PhysicalType::I64);

    let strings = snapshot.to_strings();
    assert_eq!(strings, vec!["42", "", "-7"]);

    let materialized = snapshot.into_values();
    assert_eq!(materialized.get_i64_at(0), Some(42));
    assert_eq!(materialized.get_i64_at(1), None);
    assert_eq!(materialized.get_i64_at(2), Some(-7));
}

#[test]
fn bool_snapshot_serializes_to_string_values() {
    let values = build_bool(&[Some(true), Some(false), None]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::Bool, values);

    assert_eq!(snapshot.physical_type(), PhysicalType::Bool);
    assert_eq!(snapshot.len(), 3);

    let strings = snapshot.to_strings();
    assert_eq!(strings, vec!["true", "false", ""]);
}

#[test]
fn empty_snapshot_uses_defaults() {
    let snapshot = ColumnBlockSnapshot::empty();
    assert_eq!(snapshot.physical_type(), PhysicalType::VarBytes);
    assert!(snapshot.is_empty());
    assert!(snapshot.to_strings().is_empty());
}

#[test]
fn json_values_preserve_typed_columns() {
    let i64_snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, build_i64(&[Some(42), None]));
    assert_eq!(
        i64_snapshot.to_json_values(),
        vec![Value::Number(Number::from(42)), Value::Null]
    );

    let bool_snapshot =
        ColumnBlockSnapshot::new(PhysicalType::Bool, build_bool(&[Some(true), None]));
    assert_eq!(
        bool_snapshot.to_json_values(),
        vec![Value::Bool(true), Value::Null]
    );

    let var_snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, build_varbytes(&["alpha"]));
    assert_eq!(
        var_snapshot.to_json_values(),
        vec![Value::String("alpha".to_string())]
    );
}
