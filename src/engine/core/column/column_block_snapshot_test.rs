use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, TimestampMillisecondArray,
};

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

fn build_u64(values: &[Option<u64>]) -> ColumnValues {
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
    ColumnValues::new_typed_u64(block, payload_start, row_count, nulls)
}

fn build_f64(values: &[Option<f64>]) -> ColumnValues {
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
    ColumnValues::new_typed_f64(block, payload_start, row_count, nulls)
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

// ==================== Arrow Array Conversion Tests ====================

#[test]
fn to_arrow_array_integer_i64() {
    let values = build_i64(&[Some(42), Some(-7), None, Some(100)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    let array = snapshot.to_arrow_array("Integer");
    assert_eq!(array.len(), 4);
    let int_array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("should be Int64Array");

    assert_eq!(int_array.value(0), 42);
    assert_eq!(int_array.value(1), -7);
    assert!(array.is_null(2));
    assert_eq!(int_array.value(3), 100);
}

#[test]
fn to_arrow_array_timestamp_i64() {
    let values = build_i64(&[Some(1000), Some(2000), None]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    let array = snapshot.to_arrow_array("Timestamp");
    assert_eq!(array.len(), 3);
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("should be TimestampMillisecondArray");

    assert_eq!(ts_array.value(0), 1000);
    assert_eq!(ts_array.value(1), 2000);
    assert!(array.is_null(2));
}

#[test]
fn to_arrow_array_integer_u64() {
    let values = build_u64(&[Some(100), Some(200), None]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::U64, values);

    let array = snapshot.to_arrow_array("Integer");
    assert_eq!(array.len(), 3);
    let int_array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("should be Int64Array");

    assert_eq!(int_array.value(0), 100);
    assert_eq!(int_array.value(1), 200);
    assert!(array.is_null(2));
}

#[test]
fn to_arrow_array_float_f64() {
    let values = build_f64(&[Some(1.5), Some(-2.25), None, Some(0.0)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::F64, values);

    let array = snapshot.to_arrow_array("Float");
    assert_eq!(array.len(), 4);
    let float_array = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("should be Float64Array");

    assert_eq!(float_array.value(0), 1.5);
    assert_eq!(float_array.value(1), -2.25);
    assert!(array.is_null(2));
    assert_eq!(float_array.value(3), 0.0);
}

#[test]
fn to_arrow_array_boolean_bool() {
    let values = build_bool(&[Some(true), Some(false), None, Some(true)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::Bool, values);

    let array = snapshot.to_arrow_array("Boolean");
    assert_eq!(array.len(), 4);
    let bool_array = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("should be BooleanArray");

    assert_eq!(bool_array.value(0), true);
    assert_eq!(bool_array.value(1), false);
    assert!(array.is_null(2));
    assert_eq!(bool_array.value(3), true);
}

#[test]
fn to_arrow_array_string_varbytes() {
    let values = build_varbytes(&["alpha", "beta", ""]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    let array = snapshot.to_arrow_array("String");
    assert_eq!(array.len(), 3);
    let string_array = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("should be LargeStringArray");

    assert_eq!(string_array.value(0), "alpha");
    assert_eq!(string_array.value(1), "beta");
    assert_eq!(string_array.value(2), "");
}

#[test]
fn to_arrow_array_string_with_nulls() {
    // For VarBytes, we can't directly represent nulls in the current build_varbytes helper
    // but we can test that empty strings are handled correctly
    let values = build_varbytes(&["alpha", "", "beta"]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    let array = snapshot.to_arrow_array("String");
    assert_eq!(array.len(), 3);
    let string_array = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("should be LargeStringArray");

    assert_eq!(string_array.value(0), "alpha");
    assert_eq!(string_array.value(1), "");
    assert_eq!(string_array.value(2), "beta");
}

#[test]
fn to_arrow_array_unknown_logical_type_defaults_to_string() {
    let values = build_varbytes(&["test"]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    let array = snapshot.to_arrow_array("UnknownType");
    assert_eq!(array.len(), 1);
    let string_array = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("should default to LargeStringArray");

    assert_eq!(string_array.value(0), "test");
}

#[test]
fn to_arrow_array_number_logical_type() {
    let values = build_i64(&[Some(42), Some(100)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    let array = snapshot.to_arrow_array("Number");
    assert_eq!(array.len(), 2);
    let int_array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Number should map to Int64");

    assert_eq!(int_array.value(0), 42);
    assert_eq!(int_array.value(1), 100);
}

#[test]
fn to_arrow_array_json_logical_type() {
    let values = build_varbytes(&["{\"key\":\"value\"}"]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    let array = snapshot.to_arrow_array("JSON");
    assert_eq!(array.len(), 1);
    let string_array = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("JSON should map to LargeStringArray");

    assert_eq!(string_array.value(0), "{\"key\":\"value\"}");
}

#[test]
fn into_arrow_array_consumes_snapshot() {
    let values = build_i64(&[Some(42), Some(100)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    let array = snapshot.into_arrow_array("Integer");
    assert_eq!(array.len(), 2);
    let int_array = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("should be Int64Array");

    assert_eq!(int_array.value(0), 42);
    assert_eq!(int_array.value(1), 100);
}

#[test]
fn to_arrow_array_preserves_snapshot() {
    let values = build_i64(&[Some(42), Some(100)]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, values);

    let array1 = snapshot.to_arrow_array("Integer");
    let array2 = snapshot.to_arrow_array("Integer"); // Can call again

    assert_eq!(array1.len(), 2);
    assert_eq!(array2.len(), 2);
    let int_array1 = array1
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("should be Int64Array");
    let int_array2 = array2
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("should be Int64Array");

    assert_eq!(int_array1.value(0), int_array2.value(0));
    assert_eq!(int_array1.value(1), int_array2.value(1));
}

#[test]
fn to_arrow_array_empty_snapshot() {
    let snapshot = ColumnBlockSnapshot::empty();

    let array = snapshot.to_arrow_array("String");
    assert_eq!(array.len(), 0);
}

#[test]
fn to_arrow_array_mismatched_physical_type_converts_to_string() {
    // When physical type doesn't match expected Arrow type, it should convert to string
    let values = build_varbytes(&["123"]);
    let snapshot = ColumnBlockSnapshot::new(PhysicalType::VarBytes, values);

    // Requesting Integer from VarBytes should convert to string
    let array = snapshot.to_arrow_array("Integer");
    assert_eq!(array.len(), 1);
    let string_array = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("mismatched types should convert to string");

    assert_eq!(string_array.value(0), "123");
}

#[test]
fn to_arrow_array_all_types_with_nulls() {
    // Test all types with null handling
    let i64_vals = build_i64(&[Some(1), None]);
    let i64_snapshot = ColumnBlockSnapshot::new(PhysicalType::I64, i64_vals);
    let i64_array = i64_snapshot.to_arrow_array("Integer");
    assert_eq!(i64_array.len(), 2);
    let i64_arr = i64_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array");
    assert_eq!(i64_arr.value(0), 1);
    assert!(i64_array.is_null(1));

    let f64_vals = build_f64(&[Some(1.5), None]);
    let f64_snapshot = ColumnBlockSnapshot::new(PhysicalType::F64, f64_vals);
    let f64_array = f64_snapshot.to_arrow_array("Float");
    assert_eq!(f64_array.len(), 2);
    let f64_arr = f64_array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    assert_eq!(f64_arr.value(0), 1.5);
    assert!(f64_array.is_null(1));

    let bool_vals = build_bool(&[Some(true), None]);
    let bool_snapshot = ColumnBlockSnapshot::new(PhysicalType::Bool, bool_vals);
    let bool_array = bool_snapshot.to_arrow_array("Boolean");
    assert_eq!(bool_array.len(), 2);
    let bool_arr = bool_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("BooleanArray");
    assert_eq!(bool_arr.value(0), true);
    assert!(bool_array.is_null(1));
}
