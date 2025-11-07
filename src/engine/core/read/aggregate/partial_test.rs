use std::collections::{HashMap, HashSet};

use crate::engine::core::read::aggregate::ops::AggregatorImpl;
use crate::engine::core::read::aggregate::partial::{
    AggPartial, AggState, GroupKey, snapshot_aggregator,
};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;

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
