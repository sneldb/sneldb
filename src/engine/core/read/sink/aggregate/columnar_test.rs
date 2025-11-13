use super::columnar::ColumnarProcessor;
use super::group_key::GroupKey;
use crate::command::types::TimeGranularity;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use ahash::RandomState as AHashRandomState;
use std::collections::HashMap;
use std::sync::Arc;

fn make_typed_i64_column(values: &[i64]) -> ColumnValues {
    let mut bytes = Vec::new();
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    let block = Arc::new(crate::engine::core::read::cache::DecompressedBlock::from_bytes(bytes));
    ColumnValues::new_typed_i64(block, 0, values.len(), None)
}

fn make_string_column(values: &[&str]) -> ColumnValues {
    use crate::test_helpers::factories::DecompressedBlockFactory;
    let (block, ranges) = DecompressedBlockFactory::create_with_ranges(values);
    ColumnValues::new(block, ranges)
}

// can_use_columnar_processing tests -----------------------------------------

#[test]
fn columnar_can_use_columnar_processing_count_all() {
    let specs = vec![AggregateOpSpec::CountAll];
    let columns = HashMap::new();

    assert!(ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_can_use_columnar_processing_total_with_typed_i64() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20, 30]));

    assert!(ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_can_use_columnar_processing_avg_with_typed_i64() {
    let specs = vec![AggregateOpSpec::Avg {
        field: "amount".into(),
    }];
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20, 30]));

    assert!(ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_cannot_use_columnar_processing_total_with_string_column() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_string_column(&["10", "20"]));

    assert!(!ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_cannot_use_columnar_processing_missing_column() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];
    let columns = HashMap::new();

    assert!(!ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_cannot_use_columnar_processing_count_unique() {
    let specs = vec![AggregateOpSpec::CountUnique {
        field: "user".into(),
    }];
    let mut columns = HashMap::new();
    columns.insert("user".to_string(), make_string_column(&["u1", "u2"]));

    assert!(!ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_cannot_use_columnar_processing_min_max() {
    let specs = vec![AggregateOpSpec::Min {
        field: "name".into(),
    }];
    let mut columns = HashMap::new();
    columns.insert("name".to_string(), make_string_column(&["a", "b"]));

    assert!(!ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_can_use_columnar_processing_mixed_supported_ops() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Avg {
            field: "amount".into(),
        },
    ];
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20, 30]));

    assert!(ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

#[test]
fn columnar_cannot_use_columnar_processing_mixed_unsupported_ops() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Min {
            field: "name".into(),
        },
    ];
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20]));
    columns.insert("name".to_string(), make_string_column(&["a", "b"]));

    assert!(!ColumnarProcessor::can_use_columnar_processing(&specs, &columns));
}

// process_columnar_slice tests (no grouping) ----------------------------------

#[test]
fn columnar_process_columnar_slice_no_grouping() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("dummy".to_string(), make_string_column(&["a", "b", "c"]));

    ColumnarProcessor::process_columnar_slice(&mut groups, &specs, 0, 3, &columns);

    assert_eq!(groups.len(), 1);
    let default_key = GroupKey {
        prehash: 0,
        bucket: None,
        groups: Vec::new(),
        groups_str: Some(Vec::<String>::new()),
    };
    let aggs = groups.get(&default_key).unwrap();
    assert_eq!(aggs.len(), 1);
}

#[test]
fn columnar_process_columnar_slice_with_total() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20, 30]));

    ColumnarProcessor::process_columnar_slice(&mut groups, &specs, 0, 3, &columns);

    assert_eq!(groups.len(), 1);
}

#[test]
fn columnar_process_columnar_slice_partial_range() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("dummy".to_string(), make_string_column(&["a", "b", "c", "d", "e"]));

    ColumnarProcessor::process_columnar_slice(&mut groups, &specs, 1, 4, &columns);

    assert_eq!(groups.len(), 1);
}

// process_columnar_slice_with_grouping tests --------------------------------

#[test]
fn columnar_process_columnar_slice_with_grouping_single_group() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("country".to_string(), make_string_column(&["US", "US", "US"]));

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        Some(&["country".to_string()]),
        "timestamp",
        None,
        0,
        3,
        &columns,
        None,
    );

    assert_eq!(groups.len(), 1);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_multiple_groups() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("country".to_string(), make_string_column(&["US", "DE", "US"]));

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        Some(&["country".to_string()]),
        "timestamp",
        None,
        0,
        3,
        &columns,
        None,
    );

    assert_eq!(groups.len(), 2);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_and_time_bucket() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    let timestamps: Vec<i64> = vec![86400, 86401, 172800];
    columns.insert("timestamp".to_string(), make_typed_i64_column(&timestamps));
    columns.insert("country".to_string(), make_string_column(&["US", "US", "DE"]));

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        Some(&TimeGranularity::Day),
        Some(&["country".to_string()]),
        "timestamp",
        None,
        0,
        3,
        &columns,
        None,
    );

    // Should have multiple groups (different buckets/countries)
    assert!(groups.len() >= 1);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_respects_group_limit() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("country".to_string(), make_string_column(&["US", "DE", "FR", "IT"]));

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        Some(&["country".to_string()]),
        "timestamp",
        None,
        0,
        4,
        &columns,
        Some(2), // Limit to 2 groups
    );

    assert!(groups.len() <= 2);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_uses_column_indices() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("country".to_string(), make_string_column(&["US", "DE"]));

    let mut column_indices = HashMap::new();
    column_indices.insert("country".to_string(), 0);

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        Some(&["country".to_string()]),
        "timestamp",
        Some(&column_indices),
        0,
        2,
        &columns,
        None,
    );

    assert_eq!(groups.len(), 2);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_empty_range() {
    let specs = vec![AggregateOpSpec::CountAll];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let columns = HashMap::new();

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        None,
        "timestamp",
        None,
        0,
        0,
        &columns,
        None,
    );

    assert_eq!(groups.len(), 0);
}

#[test]
fn columnar_process_columnar_slice_with_grouping_contiguous_rows_optimized() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];
    let mut groups: std::collections::HashMap<GroupKey, Vec<AggregatorImpl>, AHashRandomState> =
        std::collections::HashMap::with_hasher(AHashRandomState::new());
    let mut columns = HashMap::new();
    columns.insert("country".to_string(), make_string_column(&["US", "US", "US", "DE", "DE"]));
    columns.insert("amount".to_string(), make_typed_i64_column(&[10, 20, 30, 40, 50]));

    ColumnarProcessor::process_columnar_slice_with_grouping(
        &mut groups,
        &specs,
        None,
        Some(&["country".to_string()]),
        "timestamp",
        None,
        0,
        5,
        &columns,
        None,
    );

    assert_eq!(groups.len(), 2);
}

