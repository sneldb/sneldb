use std::collections::{HashMap, HashSet};

use crate::engine::core::read::aggregate::ops::{AggOutput, AggregatorImpl};
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

    // Avg approximates as (sum=avg as i64, count=1)
    let mut av = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    // simulate avg 5.0
    if let AggregatorImpl::Avg(ref mut inner) = av {
        inner.update_value_i64(5);
    }
    if let AggState::Avg { sum, count } = snapshot_aggregator(&av) {
        assert_eq!(sum, 5);
        assert_eq!(count, 1);
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
