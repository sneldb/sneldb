use std::collections::HashMap;

use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::sink::aggregate::group_key::GroupKey;
use crate::test_helpers::factories::{DecompressedBlockFactory, EventFactory};

fn make_columns(field_rows: &[(&str, Vec<&str>)]) -> HashMap<String, ColumnValues> {
    let mut map: HashMap<String, ColumnValues> = HashMap::new();
    for (name, rows) in field_rows.iter() {
        let (block, ranges) = DecompressedBlockFactory::create_with_ranges(rows);
        map.insert((*name).to_string(), ColumnValues::new(block, ranges));
    }
    map
}

#[test]
fn group_key_from_row_with_bucket_and_groups() {
    let ts = 3_000_100u64; // month bucket stable
    let cols = make_columns(&[
        ("timestamp", vec![&ts.to_string()]),
        ("country", vec!["US"]),
        ("plan", vec!["pro"]),
    ]);
    let key = GroupKey::from_row(
        Some(&TimeGranularity::Month),
        Some(&["country".to_string(), "plan".to_string()][..]),
        "timestamp",
        &cols,
        0,
    );
    assert!(key.bucket.is_some());
    let groups: Vec<&str> = key.groups.iter().map(|s| s.as_str()).collect();
    assert_eq!(groups, vec!["US", "pro"]);
}

#[test]
fn group_key_from_row_missing_group_field_uses_empty_string() {
    let cols = make_columns(&[("country", vec!["US"])]);
    let key = GroupKey::from_row(
        None,
        Some(&["country".to_string(), "plan".to_string()][..]),
        "timestamp",
        &cols,
        0,
    );
    assert_eq!(key.bucket, None);
    let groups: Vec<&str> = key.groups.iter().map(|s| s.as_str()).collect();
    assert_eq!(groups, vec!["US", ""]);
}

#[test]
fn group_key_from_event_with_custom_time_field() {
    use serde_json::json;
    let e = EventFactory::new()
        .with("timestamp", json!(1))
        .with("payload", json!({"country":"US","created_at": 86_450}))
        .create();
    let key = GroupKey::from_event(
        Some(&TimeGranularity::Day),
        Some(&["country".to_string()][..]),
        "created_at",
        &e,
    );
    assert_eq!(key.groups, vec!["US"]);
    assert_eq!(key.bucket, Some(86_400));
}
