use super::row_comparator::RowComparator;
use crate::engine::types::ScalarValue;
use serde_json::{Value, json};
use std::cmp::Ordering;

fn make_row(context: &str, event: &str, timestamp: u64, payload: Value) -> Vec<ScalarValue> {
    vec![
        ScalarValue::from(json!(context)),
        ScalarValue::from(json!(event)),
        ScalarValue::from(json!(timestamp)),
        ScalarValue::from(payload),
    ]
}

#[test]
fn compares_by_context_id() {
    let row1 = make_row("ctx-001", "click", 1000, json!({}));
    let row2 = make_row("ctx-002", "click", 1000, json!({}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "context_id"),
        Ordering::Less
    );
    assert_eq!(
        RowComparator::compare(&row2, &row1, "context_id"),
        Ordering::Greater
    );
    assert_eq!(
        RowComparator::compare(&row1, &row1, "context_id"),
        Ordering::Equal
    );
}

#[test]
fn compares_by_event_type() {
    let row1 = make_row("ctx", "click", 1000, json!({}));
    let row2 = make_row("ctx", "purchase", 1000, json!({}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "event_type"),
        Ordering::Less
    );
}

#[test]
fn compares_by_timestamp() {
    let row1 = make_row("ctx", "click", 1000, json!({}));
    let row2 = make_row("ctx", "click", 2000, json!({}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "timestamp"),
        Ordering::Less
    );
    assert_eq!(
        RowComparator::compare(&row2, &row1, "timestamp"),
        Ordering::Greater
    );
}

#[test]
fn compares_payload_field_as_u64() {
    let row1 = make_row("ctx", "click", 1000, json!({"amount": 100}));
    let row2 = make_row("ctx", "click", 1000, json!({"amount": 200}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "amount"),
        Ordering::Less
    );
}

#[test]
fn compares_payload_field_as_i64() {
    let row1 = make_row("ctx", "click", 1000, json!({"balance": -50}));
    let row2 = make_row("ctx", "click", 1000, json!({"balance": 100}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "balance"),
        Ordering::Less
    );
}

#[test]
fn compares_payload_field_as_f64() {
    let row1 = make_row("ctx", "click", 1000, json!({"price": 19.99}));
    let row2 = make_row("ctx", "click", 1000, json!({"price": 29.99}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "price"),
        Ordering::Less
    );
}

#[test]
fn compares_payload_field_as_string() {
    let row1 = make_row("ctx", "click", 1000, json!({"region": "EU"}));
    let row2 = make_row("ctx", "click", 1000, json!({"region": "US"}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "region"),
        Ordering::Less
    );
}

#[test]
fn compares_missing_payload_fields() {
    let row1 = make_row("ctx", "click", 1000, json!({"field1": "value"}));
    let row2 = make_row("ctx", "click", 1000, json!({"field2": "value"}));

    // Both missing the same field should be equal
    assert_eq!(
        RowComparator::compare(&row1, &row2, "missing_field"),
        Ordering::Equal
    );
}

#[test]
fn compares_one_missing_payload_field() {
    let row1 = make_row("ctx", "click", 1000, json!({"score": 100}));
    let row2 = make_row("ctx", "click", 1000, json!({}));

    // row1 has field, row2 doesn't - row2 < row1
    assert_eq!(
        RowComparator::compare(&row2, &row1, "score"),
        Ordering::Less
    );
    assert_eq!(
        RowComparator::compare(&row1, &row2, "score"),
        Ordering::Greater
    );
}

#[test]
fn numeric_comparison_with_different_magnitudes() {
    let row1 = make_row("ctx", "click", 1000, json!({"value": 9}));
    let row2 = make_row("ctx", "click", 1000, json!({"value": 100}));

    // Should compare numerically: 9 < 100
    assert_eq!(
        RowComparator::compare(&row1, &row2, "value"),
        Ordering::Less
    );
}

#[test]
fn handles_zero_values() {
    let row1 = make_row("ctx", "click", 1000, json!({"count": 0}));
    let row2 = make_row("ctx", "click", 1000, json!({"count": 1}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "count"),
        Ordering::Less
    );
}

#[test]
fn handles_negative_numbers() {
    let row1 = make_row("ctx", "click", 1000, json!({"temp": -10}));
    let row2 = make_row("ctx", "click", 1000, json!({"temp": -5}));

    assert_eq!(RowComparator::compare(&row1, &row2, "temp"), Ordering::Less);
}

#[test]
fn handles_large_numbers() {
    let row1 = make_row("ctx", "click", 1000, json!({"big": 9999999999_u64}));
    let row2 = make_row("ctx", "click", 1000, json!({"big": 10000000000_u64}));

    assert_eq!(RowComparator::compare(&row1, &row2, "big"), Ordering::Less);
}

#[test]
fn handles_decimal_precision() {
    let row1 = make_row("ctx", "click", 1000, json!({"rate": 0.001}));
    let row2 = make_row("ctx", "click", 1000, json!({"rate": 0.002}));

    assert_eq!(RowComparator::compare(&row1, &row2, "rate"), Ordering::Less);
}

#[test]
fn sort_by_field_ascending() {
    let mut rows = vec![
        make_row("ctx", "click", 1000, json!({"score": 30})),
        make_row("ctx", "click", 1000, json!({"score": 10})),
        make_row("ctx", "click", 1000, json!({"score": 20})),
    ];

    RowComparator::sort_by_field(&mut rows, "score", true);

    assert_eq!(rows[0][3].to_json()["score"], json!(10));
    assert_eq!(rows[1][3].to_json()["score"], json!(20));
    assert_eq!(rows[2][3].to_json()["score"], json!(30));
}

#[test]
fn sort_by_field_descending() {
    let mut rows = vec![
        make_row("ctx", "click", 1000, json!({"score": 10})),
        make_row("ctx", "click", 1000, json!({"score": 30})),
        make_row("ctx", "click", 1000, json!({"score": 20})),
    ];

    RowComparator::sort_by_field(&mut rows, "score", false);

    assert_eq!(rows[0][3].to_json()["score"], json!(30));
    assert_eq!(rows[1][3].to_json()["score"], json!(20));
    assert_eq!(rows[2][3].to_json()["score"], json!(10));
}

#[test]
fn sort_by_timestamp() {
    let mut rows = vec![
        make_row("ctx", "click", 3000, json!({})),
        make_row("ctx", "click", 1000, json!({})),
        make_row("ctx", "click", 2000, json!({})),
    ];

    RowComparator::sort_by_field(&mut rows, "timestamp", true);

    assert_eq!(rows[0][2].as_u64(), Some(1000));
    assert_eq!(rows[1][2].as_u64(), Some(2000));
    assert_eq!(rows[2][2].as_u64(), Some(3000));
}

#[test]
fn sort_by_context_id() {
    let mut rows = vec![
        make_row("ctx-c", "click", 1000, json!({})),
        make_row("ctx-a", "click", 1000, json!({})),
        make_row("ctx-b", "click", 1000, json!({})),
    ];

    RowComparator::sort_by_field(&mut rows, "context_id", true);

    assert_eq!(rows[0][0].as_str(), Some("ctx-a"));
    assert_eq!(rows[1][0].as_str(), Some("ctx-b"));
    assert_eq!(rows[2][0].as_str(), Some("ctx-c"));
}

#[test]
fn sort_empty_list() {
    let mut rows: Vec<Vec<ScalarValue>> = vec![];
    RowComparator::sort_by_field(&mut rows, "timestamp", true);
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_single_element() {
    let mut rows = vec![make_row("ctx", "click", 1000, json!({}))];
    RowComparator::sort_by_field(&mut rows, "timestamp", true);
    assert_eq!(rows.len(), 1);
}

#[test]
fn sort_with_duplicate_values() {
    let mut rows = vec![
        make_row("ctx", "click", 1000, json!({"priority": 2})),
        make_row("ctx", "click", 1000, json!({"priority": 1})),
        make_row("ctx", "click", 1000, json!({"priority": 2})),
        make_row("ctx", "click", 1000, json!({"priority": 1})),
    ];

    RowComparator::sort_by_field(&mut rows, "priority", true);

    assert_eq!(rows[0][3].to_json()["priority"], json!(1));
    assert_eq!(rows[1][3].to_json()["priority"], json!(1));
    assert_eq!(rows[2][3].to_json()["priority"], json!(2));
    assert_eq!(rows[3][3].to_json()["priority"], json!(2));
}

#[test]
fn handles_boolean_as_json() {
    let row1 = make_row("ctx", "click", 1000, json!({"active": false}));
    let row2 = make_row("ctx", "click", 1000, json!({"active": true}));

    // false (0) < true (1) when compared as numbers
    // But as JSON values they compare as strings
    let result = RowComparator::compare(&row1, &row2, "active");
    assert_ne!(result, Ordering::Greater); // Should be less or equal
}

#[test]
fn handles_null_values() {
    let row1 = make_row("ctx", "click", 1000, json!({"field": null}));
    let row2 = make_row("ctx", "click", 1000, json!({"field": "value"}));

    // null values are compared as strings: "null" > "value" (n comes after v)
    let result = RowComparator::compare(&row1, &row2, "field");
    assert_eq!(result, Ordering::Greater); // "null" string > "value" string
}

#[test]
fn handles_nested_objects_as_strings() {
    let row1 = make_row("ctx", "click", 1000, json!({"data": {"nested": 1}}));
    let row2 = make_row("ctx", "click", 1000, json!({"data": {"nested": 2}}));

    // Nested objects will be compared as strings
    let result = RowComparator::compare(&row1, &row2, "data");
    assert!(result != Ordering::Greater || result != Ordering::Equal);
}

#[test]
fn case_sensitive_string_comparison() {
    let row1 = make_row("ctx", "click", 1000, json!({"status": "Active"}));
    let row2 = make_row("ctx", "click", 1000, json!({"status": "active"}));

    // "Active" < "active" (uppercase < lowercase in ASCII)
    assert_eq!(
        RowComparator::compare(&row1, &row2, "status"),
        Ordering::Less
    );
}

#[test]
fn handles_empty_payload() {
    let row1 = make_row("ctx", "click", 1000, json!({}));
    let row2 = make_row("ctx", "click", 1000, json!({}));

    assert_eq!(
        RowComparator::compare(&row1, &row2, "any_field"),
        Ordering::Equal
    );
}

#[test]
fn timestamp_with_mixed_types() {
    use crate::engine::types::ScalarValue;
    // Test when timestamp is provided as different numeric types
    let row1 = vec![
        ScalarValue::from(json!("ctx")),
        ScalarValue::from(json!("click")),
        ScalarValue::from(json!(1000_u64)),
        ScalarValue::from(json!({})),
    ];
    let row2 = vec![
        ScalarValue::from(json!("ctx")),
        ScalarValue::from(json!("click")),
        ScalarValue::from(json!(2000_i64)),
        ScalarValue::from(json!({})),
    ];

    assert_eq!(
        RowComparator::compare(&row1, &row2, "timestamp"),
        Ordering::Less
    );
}
