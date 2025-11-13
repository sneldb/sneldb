use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::partial::{
    AggPartial, AggState, GroupKey, snapshot_aggregator,
};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::cache::DecompressedBlock;

// Helper functions to create typed columns for testing
fn build_typed_i64(values: &[Option<i64>]) -> ColumnValues {
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

fn build_typed_u64(values: &[Option<u64>]) -> ColumnValues {
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

fn build_typed_f64(values: &[Option<f64>]) -> ColumnValues {
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

fn build_typed_bool(values: &[Option<bool>]) -> ColumnValues {
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

fn make_typed_columns(field_values: &[(&str, ColumnValues)]) -> HashMap<String, ColumnValues> {
    let mut map: HashMap<String, ColumnValues> = HashMap::new();
    for (name, col) in field_values.iter() {
        map.insert((*name).to_string(), (*col).clone());
    }
    map
}

// AggState::merge ---------------------------------------------------------

#[test]
fn agg_state_merge_countall_sums() {
    let mut a = AggState::CountAll { count: 2 };
    let b = AggState::CountAll { count: 3 };
    a.merge(&b);
    assert_eq!(a, AggState::CountAll { count: 5 });
}

#[test]
fn agg_state_merge_countunique_unions() {
    let mut a = AggState::CountUnique {
        values: ["a".into(), "b".into()].into_iter().collect(),
    };
    let b = AggState::CountUnique {
        values: ["b".into(), "c".into()].into_iter().collect(),
    };
    a.merge(&b);
    let mut expected: HashSet<String> = HashSet::new();
    expected.insert("a".into());
    expected.insert("b".into());
    expected.insert("c".into());
    assert_eq!(a, AggState::CountUnique { values: expected });
}

#[test]
fn agg_state_merge_sum_adds() {
    let mut a = AggState::Sum { sum: 7 };
    let b = AggState::Sum { sum: 5 };
    a.merge(&b);
    assert_eq!(a, AggState::Sum { sum: 12 });
}

#[test]
fn agg_state_merge_avg_combines_sum_and_count() {
    let mut a = AggState::Avg { sum: 10, count: 2 };
    let b = AggState::Avg { sum: 5, count: 1 };
    a.merge(&b);
    assert_eq!(a, AggState::Avg { sum: 15, count: 3 });
}

#[test]
fn agg_state_merge_min_prefers_smaller_numeric_and_lexicographic() {
    let mut a = AggState::Min {
        min_num: Some(10),
        min_str: Some("zz".into()),
    };
    let b = AggState::Min {
        min_num: Some(2),
        min_str: Some("aa".into()),
    };
    a.merge(&b);
    assert_eq!(
        a,
        AggState::Min {
            min_num: Some(2),
            min_str: Some("aa".into())
        }
    );

    // when self has none, adopt other's
    let mut a2 = AggState::Min {
        min_num: None,
        min_str: None,
    };
    let b2 = AggState::Min {
        min_num: Some(5),
        min_str: Some("m".into()),
    };
    a2.merge(&b2);
    assert_eq!(a2, b2);
}

#[test]
fn agg_state_merge_max_prefers_larger_numeric_and_lexicographic() {
    let mut a = AggState::Max {
        max_num: Some(3),
        max_str: Some("aa".into()),
    };
    let b = AggState::Max {
        max_num: Some(7),
        max_str: Some("zz".into()),
    };
    a.merge(&b);
    assert_eq!(
        a,
        AggState::Max {
            max_num: Some(7),
            max_str: Some("zz".into())
        }
    );

    // when self has none, adopt other's
    let mut a2 = AggState::Max {
        max_num: None,
        max_str: None,
    };
    let b2 = AggState::Max {
        max_num: Some(1),
        max_str: Some("b".into()),
    };
    a2.merge(&b2);
    assert_eq!(a2, b2);
}

// snapshot_aggregator -----------------------------------------------------

#[test]
fn snapshot_from_aggregators_covers_all_variants() {
    // CountAll
    let mut a = AggregatorImpl::from_spec(&AggregateOpSpec::CountAll);
    // bump once
    if let AggregatorImpl::CountAll(ref mut inner) = a {
        inner.update();
    }
    assert_eq!(snapshot_aggregator(&a), AggState::CountAll { count: 1 });

    // CountField represented as CountAll state
    let mut cf = AggregatorImpl::from_spec(&AggregateOpSpec::CountField { field: "x".into() });
    if let AggregatorImpl::CountField(ref mut inner) = cf {
        inner.update_non_null();
    }
    assert_eq!(snapshot_aggregator(&cf), AggState::CountAll { count: 1 });

    // CountUnique carries set values
    let mut cu = AggregatorImpl::from_spec(&AggregateOpSpec::CountUnique {
        field: "user".into(),
    });
    if let AggregatorImpl::CountUnique(ref mut inner) = cu {
        inner.update_value_str("u1");
        inner.update_value_str("u2");
    }
    if let AggState::CountUnique { values } = snapshot_aggregator(&cu) {
        assert!(values.contains("u1"));
        assert!(values.contains("u2"));
    } else {
        panic!("expected CountUnique state");
    }

    // Sum
    let mut s = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    if let AggregatorImpl::Sum(ref mut inner) = s {
        inner.update_value_i64(7);
    }
    assert_eq!(snapshot_aggregator(&s), AggState::Sum { sum: 7 });

    // Min numeric vs string
    let mut mn = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "name".into(),
    });
    if let AggregatorImpl::Min(ref mut inner) = mn {
        inner.update_value_str("10");
        inner.update_value_str("2");
    }
    assert_eq!(
        snapshot_aggregator(&mn),
        AggState::Min {
            min_num: Some(2),
            min_str: None
        }
    );

    let mut mn2 = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "name".into(),
    });
    if let AggregatorImpl::Min(ref mut inner) = mn2 {
        inner.update_value_str("bob");
    }
    assert_eq!(
        snapshot_aggregator(&mn2),
        AggState::Min {
            min_num: None,
            min_str: Some("bob".into())
        }
    );

    // Max numeric vs string
    let mut mx = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "name".into(),
    });
    if let AggregatorImpl::Max(ref mut inner) = mx {
        inner.update_value_str("10");
        inner.update_value_str("2");
    }
    assert_eq!(
        snapshot_aggregator(&mx),
        AggState::Max {
            max_num: Some(10),
            max_str: None
        }
    );

    let mut mx2 = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "name".into(),
    });
    if let AggregatorImpl::Max(ref mut inner) = mx2 {
        inner.update_value_str("amy");
    }
    assert_eq!(
        snapshot_aggregator(&mx2),
        AggState::Max {
            max_num: None,
            max_str: Some("amy".into())
        }
    );

    // Avg preserves sum and count separately (not finalized) for accurate merging
    // This allows accurate merging of AVG aggregates across shards/segments
    let mut av = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    // Update with multiple values to verify sum and count are preserved
    if let AggregatorImpl::Avg(ref mut inner) = av {
        inner.update_value_i64(5);
        inner.update_value_i64(10);
        inner.update_value_i64(15);
    }
    if let AggState::Avg { sum, count } = snapshot_aggregator(&av) {
        // Sum should be 5 + 10 + 15 = 30, count should be 3
        assert_eq!(sum, 30);
        assert_eq!(count, 3);
    } else {
        panic!("expected Avg state");
    }
}

// AggPartial::merge -------------------------------------------------------

#[test]
fn agg_partial_merge_merges_matching_keys_and_states() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Min {
            field: "name".into(),
        },
    ];

    let key = GroupKey {
        bucket: Some(1),
        groups: vec!["US".into()],
    };

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(
            key.clone(),
            vec![
                AggState::CountAll { count: 1 },
                AggState::Sum { sum: 10 },
                AggState::Min {
                    min_num: None,
                    min_str: Some("zoe".into()),
                },
            ],
        )]),
    };

    let right = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(
            key.clone(),
            vec![
                AggState::CountAll { count: 2 },
                AggState::Sum { sum: 5 },
                AggState::Min {
                    min_num: None,
                    min_str: Some("amy".into()),
                },
            ],
        )]),
    };

    left.merge(&right);

    let merged = left.groups.get(&key).unwrap();
    assert_eq!(merged[0], AggState::CountAll { count: 3 });
    assert_eq!(merged[1], AggState::Sum { sum: 15 });
    assert_eq!(
        merged[2],
        AggState::Min {
            min_num: None,
            min_str: Some("amy".into())
        }
    );
}

#[test]
fn snapshot_aggregator_avg_preserves_sum_and_count() {
    // Test that snapshot_aggregator for Avg uses sum_count() not finalize()
    // This ensures accurate merging across shards/segments
    let mut av = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });

    // Update with values: 10, 20, 30
    if let AggregatorImpl::Avg(ref mut inner) = av {
        inner.update_value_i64(10);
        inner.update_value_i64(20);
        inner.update_value_i64(30);
    }

    let state = snapshot_aggregator(&av);
    if let AggState::Avg { sum, count } = state {
        // Should preserve sum=60, count=3 (not finalized average)
        assert_eq!(sum, 60);
        assert_eq!(count, 3);
        // Verify it's not finalized (average would be 20.0, but we have sum/count)
        assert_ne!(sum as f64 / count as f64, 0.0); // Just verify division works
    } else {
        panic!("expected Avg state with sum and count");
    }
}

#[test]
fn snapshot_aggregator_avg_empty_has_zero_sum_and_count() {
    // Test that empty Avg aggregator has sum=0, count=0
    let av = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });

    let state = snapshot_aggregator(&av);
    if let AggState::Avg { sum, count } = state {
        assert_eq!(sum, 0);
        assert_eq!(count, 0);
    } else {
        panic!("expected Avg state");
    }
}

#[test]
fn agg_state_merge_avg_accurate_merging() {
    // Test that merging Avg states preserves accuracy
    // This is the key benefit of using sum/count instead of finalized average
    let mut a = AggState::Avg { sum: 30, count: 3 }; // avg = 10.0
    let b = AggState::Avg { sum: 20, count: 2 }; // avg = 10.0
    a.merge(&b);
    // Merged: sum = 50, count = 5, avg = 10.0 (accurate!)
    // If we had merged finalized averages (10.0 + 10.0) / 2 = 10.0, but that's only correct
    // because both averages happened to be the same. With sum/count, we get accurate results
    // even when averages differ.
    assert_eq!(a, AggState::Avg { sum: 50, count: 5 });

    // Test with different averages to show accuracy
    let mut c = AggState::Avg { sum: 20, count: 2 }; // avg = 10.0
    let d = AggState::Avg { sum: 30, count: 3 }; // avg = 10.0
    c.merge(&d);
    assert_eq!(c, AggState::Avg { sum: 50, count: 5 });

    // Test with different averages
    let mut e = AggState::Avg { sum: 10, count: 2 }; // avg = 5.0
    let f = AggState::Avg { sum: 30, count: 3 }; // avg = 10.0
    e.merge(&f);
    // Merged: sum = 40, count = 5, avg = 8.0 (accurate!)
    // If we had merged finalized averages: (5.0 + 10.0) / 2 = 7.5 (WRONG!)
    assert_eq!(e, AggState::Avg { sum: 40, count: 5 });
}

#[test]
fn agg_partial_merge_ignores_mismatched_state_lengths() {
    let specs = vec![AggregateOpSpec::CountAll];
    let key = GroupKey {
        bucket: None,
        groups: vec![],
    };

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: None,
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![AggState::CountAll { count: 1 }])]),
    };

    // right has two states (mismatch)
    let right = AggPartial {
        specs,
        group_by: None,
        time_bucket: None,
        groups: HashMap::from([(
            key.clone(),
            vec![AggState::CountAll { count: 2 }, AggState::Sum { sum: 7 }],
        )]),
    };

    left.merge(&right);
    // unchanged
    let merged = left.groups.get(&key).unwrap();
    assert_eq!(merged, &vec![AggState::CountAll { count: 1 }]);
}

// Typed column tests for snapshot_aggregator ------------------------------

#[test]
fn snapshot_countfield_typed_i64_counts_non_null_values() {
    // Test the bug fix: snapshot_aggregator should correctly capture CountField
    // from typed i64 columns
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "order_id".into(),
    });
    let col = build_typed_i64(&[Some(1), Some(2), None, Some(4)]);
    let columns = make_typed_columns(&[("order_id", col)]);

    agg.update(0, &columns); // Some(1) - should count
    agg.update(1, &columns); // Some(2) - should count
    agg.update(2, &columns); // None - should NOT count
    agg.update(3, &columns); // Some(4) - should count

    let state = snapshot_aggregator(&agg);
    assert_eq!(state, AggState::CountAll { count: 3 });
}

#[test]
fn snapshot_countfield_typed_i64_all_null_returns_zero() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "order_id".into(),
    });
    let col = build_typed_i64(&[None, None, None]);
    let columns = make_typed_columns(&[("order_id", col)]);

    agg.update(0, &columns);
    agg.update(1, &columns);
    agg.update(2, &columns);

    let state = snapshot_aggregator(&agg);
    assert_eq!(state, AggState::CountAll { count: 0 });
}

#[test]
fn snapshot_countfield_typed_u64_f64_bool() {
    // Test u64
    let mut agg_u64 = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "user_id".into(),
    });
    let col_u64 = build_typed_u64(&[Some(100), None, Some(200)]);
    let cols_u64 = make_typed_columns(&[("user_id", col_u64)]);
    agg_u64.update(0, &cols_u64);
    agg_u64.update(1, &cols_u64);
    agg_u64.update(2, &cols_u64);
    assert_eq!(
        snapshot_aggregator(&agg_u64),
        AggState::CountAll { count: 2 }
    );

    // Test f64
    let mut agg_f64 = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "price".into(),
    });
    let col_f64 = build_typed_f64(&[Some(10.5), None, Some(20.0)]);
    let cols_f64 = make_typed_columns(&[("price", col_f64)]);
    agg_f64.update(0, &cols_f64);
    agg_f64.update(1, &cols_f64);
    agg_f64.update(2, &cols_f64);
    assert_eq!(
        snapshot_aggregator(&agg_f64),
        AggState::CountAll { count: 2 }
    );

    // Test bool
    let mut agg_bool = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "active".into(),
    });
    let col_bool = build_typed_bool(&[Some(true), None, Some(false)]);
    let cols_bool = make_typed_columns(&[("active", col_bool)]);
    agg_bool.update(0, &cols_bool);
    agg_bool.update(1, &cols_bool);
    agg_bool.update(2, &cols_bool);
    assert_eq!(
        snapshot_aggregator(&agg_bool),
        AggState::CountAll { count: 2 }
    );
}

#[test]
fn snapshot_sum_typed_i64_sums_non_null_values() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    let col = build_typed_i64(&[Some(10), Some(20), None, Some(30)]);
    let columns = make_typed_columns(&[("amount", col)]);

    agg.update(0, &columns);
    agg.update(1, &columns);
    agg.update(2, &columns);
    agg.update(3, &columns);

    let state = snapshot_aggregator(&agg);
    assert_eq!(state, AggState::Sum { sum: 60 });
}

#[test]
fn snapshot_avg_typed_i64_preserves_sum_and_count() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let col = build_typed_i64(&[Some(10), Some(20), None, Some(30)]);
    let columns = make_typed_columns(&[("amount", col)]);

    agg.update(0, &columns);
    agg.update(1, &columns);
    agg.update(2, &columns);
    agg.update(3, &columns);

    let state = snapshot_aggregator(&agg);
    if let AggState::Avg { sum, count } = state {
        assert_eq!(sum, 60); // 10 + 20 + 30
        assert_eq!(count, 3); // 3 non-null values
    } else {
        panic!("expected Avg state");
    }
}

#[test]
fn snapshot_min_max_typed_i64_picks_correct_extremes() {
    // Min
    let mut min_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "value".into(),
    });
    let col = build_typed_i64(&[Some(10), Some(5), None, Some(20)]);
    let columns = make_typed_columns(&[("value", col)]);
    min_agg.update(0, &columns);
    min_agg.update(1, &columns);
    min_agg.update(2, &columns);
    min_agg.update(3, &columns);

    let min_state = snapshot_aggregator(&min_agg);
    assert_eq!(
        min_state,
        AggState::Min {
            min_num: Some(5),
            min_str: None
        }
    );

    // Max
    let mut max_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "value".into(),
    });
    let col2 = build_typed_i64(&[Some(10), Some(5), None, Some(20)]);
    let columns2 = make_typed_columns(&[("value", col2)]);
    max_agg.update(0, &columns2);
    max_agg.update(1, &columns2);
    max_agg.update(2, &columns2);
    max_agg.update(3, &columns2);

    let max_state = snapshot_aggregator(&max_agg);
    assert_eq!(
        max_state,
        AggState::Max {
            max_num: Some(20),
            max_str: None
        }
    );
}

#[test]
fn snapshot_countfield_typed_i64_real_world_scenario() {
    // Simulates the actual bug scenario: COUNT order_id BY country
    // where order_id is int | null, then snapshot for merging
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "order_id".into(),
    });

    // NL country: 2 orders with order_id
    let col_nl = build_typed_i64(&[Some(1), Some(2)]);
    let cols_nl = make_typed_columns(&[("order_id", col_nl)]);
    agg.update(0, &cols_nl);
    agg.update(1, &cols_nl);

    // DE country: 0 orders with order_id, 1 without
    let col_de = build_typed_i64(&[None]);
    let cols_de = make_typed_columns(&[("order_id", col_de)]);
    agg.update(0, &cols_de);

    // Snapshot should capture count of 2 (from NL), not 0
    let state = snapshot_aggregator(&agg);
    assert_eq!(state, AggState::CountAll { count: 2 });
}

// Typed column tests for AggPartial merge ----------------------------------

#[test]
fn agg_partial_merge_countfield_typed_columns() {
    let specs = vec![AggregateOpSpec::CountField {
        field: "order_id".into(),
    }];

    let key = GroupKey {
        bucket: None,
        groups: vec!["NL".into()],
    };

    // Left partial: 2 non-null values
    let mut left_agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "order_id".into(),
    });
    let col1 = build_typed_i64(&[Some(1), Some(2)]);
    let cols1 = make_typed_columns(&[("order_id", col1)]);
    left_agg.update(0, &cols1);
    left_agg.update(1, &cols1);
    let left_state = snapshot_aggregator(&left_agg);

    // Right partial: 1 non-null value, 1 null
    let mut right_agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "order_id".into(),
    });
    let col2 = build_typed_i64(&[Some(3), None]);
    let cols2 = make_typed_columns(&[("order_id", col2)]);
    right_agg.update(0, &cols2);
    right_agg.update(1, &cols2);
    let right_state = snapshot_aggregator(&right_agg);

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![left_state])]),
    };

    let right = AggPartial {
        specs,
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![right_state])]),
    };

    left.merge(&right);

    let merged = left.groups.get(&key).unwrap();
    // Should be 2 + 1 = 3 (nulls don't count)
    assert_eq!(merged[0], AggState::CountAll { count: 3 });
}

#[test]
fn agg_partial_merge_sum_typed_columns() {
    let specs = vec![AggregateOpSpec::Total {
        field: "amount".into(),
    }];

    let key = GroupKey {
        bucket: None,
        groups: vec!["US".into()],
    };

    let mut left_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    let col1 = build_typed_i64(&[Some(10), Some(20)]);
    let cols1 = make_typed_columns(&[("amount", col1)]);
    left_agg.update(0, &cols1);
    left_agg.update(1, &cols1);
    let left_state = snapshot_aggregator(&left_agg);

    let mut right_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    let col2 = build_typed_i64(&[Some(30), None]);
    let cols2 = make_typed_columns(&[("amount", col2)]);
    right_agg.update(0, &cols2);
    right_agg.update(1, &cols2);
    let right_state = snapshot_aggregator(&right_agg);

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![left_state])]),
    };

    let right = AggPartial {
        specs,
        group_by: Some(vec!["country".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![right_state])]),
    };

    left.merge(&right);

    let merged = left.groups.get(&key).unwrap();
    assert_eq!(merged[0], AggState::Sum { sum: 60 }); // 10 + 20 + 30
}

#[test]
fn agg_partial_merge_avg_typed_columns() {
    let specs = vec![AggregateOpSpec::Avg {
        field: "amount".into(),
    }];

    let key = GroupKey {
        bucket: None,
        groups: vec!["EU".into()],
    };

    let mut left_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let col1 = build_typed_i64(&[Some(10), Some(20)]);
    let cols1 = make_typed_columns(&[("amount", col1)]);
    left_agg.update(0, &cols1);
    left_agg.update(1, &cols1);
    let left_state = snapshot_aggregator(&left_agg);

    let mut right_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let col2 = build_typed_i64(&[Some(30), None]);
    let cols2 = make_typed_columns(&[("amount", col2)]);
    right_agg.update(0, &cols2);
    right_agg.update(1, &cols2);
    let right_state = snapshot_aggregator(&right_agg);

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["region".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![left_state])]),
    };

    let right = AggPartial {
        specs,
        group_by: Some(vec!["region".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![right_state])]),
    };

    left.merge(&right);

    let merged = left.groups.get(&key).unwrap();
    if let AggState::Avg { sum, count } = merged[0] {
        assert_eq!(sum, 60); // 10 + 20 + 30
        assert_eq!(count, 3); // 3 non-null values
    } else {
        panic!("expected Avg state");
    }
}

#[test]
fn agg_partial_merge_min_max_typed_columns() {
    let specs = vec![
        AggregateOpSpec::Min {
            field: "value".into(),
        },
        AggregateOpSpec::Max {
            field: "value".into(),
        },
    ];

    let key = GroupKey {
        bucket: None,
        groups: vec!["A".into()],
    };

    // Left: min=5, max=10
    let mut left_min = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "value".into(),
    });
    let mut left_max = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "value".into(),
    });
    let col1 = build_typed_i64(&[Some(5), Some(10)]);
    let cols1 = make_typed_columns(&[("value", col1)]);
    left_min.update(0, &cols1);
    left_min.update(1, &cols1);
    left_max.update(0, &cols1);
    left_max.update(1, &cols1);
    let left_min_state = snapshot_aggregator(&left_min);
    let left_max_state = snapshot_aggregator(&left_max);

    // Right: min=3, max=15
    let mut right_min = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "value".into(),
    });
    let mut right_max = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "value".into(),
    });
    let col2 = build_typed_i64(&[Some(3), Some(15)]);
    let cols2 = make_typed_columns(&[("value", col2)]);
    right_min.update(0, &cols2);
    right_min.update(1, &cols2);
    right_max.update(0, &cols2);
    right_max.update(1, &cols2);
    let right_min_state = snapshot_aggregator(&right_min);
    let right_max_state = snapshot_aggregator(&right_max);

    let mut left = AggPartial {
        specs: specs.clone(),
        group_by: Some(vec!["category".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![left_min_state, left_max_state])]),
    };

    let right = AggPartial {
        specs,
        group_by: Some(vec!["category".into()]),
        time_bucket: None,
        groups: HashMap::from([(key.clone(), vec![right_min_state, right_max_state])]),
    };

    left.merge(&right);

    let merged = left.groups.get(&key).unwrap();
    // Min should be 3 (smallest of 5, 10, 3, 15)
    assert_eq!(
        merged[0],
        AggState::Min {
            min_num: Some(3),
            min_str: None
        }
    );
    // Max should be 15 (largest of 5, 10, 3, 15)
    assert_eq!(
        merged[1],
        AggState::Max {
            max_num: Some(15),
            max_str: None
        }
    );
}
