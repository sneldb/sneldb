use std::collections::HashMap;

use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::sink::aggregate::group_key_builder::GroupKeyBuilder;
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
fn group_key_builder_builds_consistently_for_rows() {
    let builder = GroupKeyBuilder::new(
        Some(TimeGranularity::Month),
        Some(vec!["country".to_string()]),
        "timestamp".to_string(),
    );
    let ts = 3_000_000u64;
    let cols = make_columns(&[
        ("timestamp", vec![&ts.to_string()]),
        ("country", vec!["US"]),
    ]);
    let k1 = builder.build_from_row(&cols, 0);
    let k2 = builder.build_from_row(&cols, 0);
    assert_eq!(k1.bucket, k2.bucket);
    assert_eq!(k1.groups, k2.groups);
}

#[test]
fn group_key_builder_builds_from_event_with_same_params() {
    use serde_json::json;
    let builder = GroupKeyBuilder::new(
        Some(TimeGranularity::Day),
        Some(vec!["country".to_string()]),
        "created_at".to_string(),
    );
    let e = EventFactory::new()
        .with("timestamp", json!(1))
        .with("payload", json!({"country":"US","created_at": 86_401}))
        .create();
    let k = builder.build_from_event(&e);
    assert_eq!(k.groups, vec!["US"]);
    assert_eq!(k.bucket, Some(86_400));
}
