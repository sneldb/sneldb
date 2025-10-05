use std::collections::HashMap;

use serde_json::json;

use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::aggregate::ops::{
    AggOutput, AggregatorImpl, Avg, CountAll, CountField, CountUnique, Max, Min, Sum,
};
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::test_helpers::factories::{DecompressedBlockFactory, EventFactory};

fn make_columns(field_rows: &[(&str, Vec<&str>)]) -> HashMap<String, ColumnValues> {
    let mut map: HashMap<String, ColumnValues> = HashMap::new();
    for (name, rows) in field_rows.iter() {
        let (block, ranges) = DecompressedBlockFactory::create_with_ranges(rows);
        map.insert((*name).to_string(), ColumnValues::new(block, ranges));
    }
    map
}

// from_spec + field -------------------------------------------------------

#[test]
fn aggregator_from_spec_and_field_mapping() {
    let specs = vec![
        AggregateOpSpec::CountAll,
        AggregateOpSpec::CountField {
            field: "visits".into(),
        },
        AggregateOpSpec::CountUnique {
            field: "user".into(),
        },
        AggregateOpSpec::Total {
            field: "amount".into(),
        },
        AggregateOpSpec::Avg {
            field: "amount".into(),
        },
        AggregateOpSpec::Min {
            field: "name".into(),
        },
        AggregateOpSpec::Max {
            field: "name".into(),
        },
    ];
    let aggs: Vec<AggregatorImpl> = specs.iter().map(|s| AggregatorImpl::from_spec(s)).collect();

    assert!(matches!(aggs[0], AggregatorImpl::CountAll(_)));
    assert_eq!(aggs[0].field(), None);

    assert!(matches!(aggs[1], AggregatorImpl::CountField(_)));
    assert_eq!(aggs[1].field(), Some("visits"));

    assert!(matches!(aggs[2], AggregatorImpl::CountUnique(_)));
    assert_eq!(aggs[2].field(), Some("user"));

    assert!(matches!(aggs[3], AggregatorImpl::Sum(_)));
    assert_eq!(aggs[3].field(), Some("amount"));

    assert!(matches!(aggs[4], AggregatorImpl::Avg(_)));
    assert_eq!(aggs[4].field(), Some("amount"));

    assert!(matches!(aggs[5], AggregatorImpl::Min(_)));
    assert_eq!(aggs[5].field(), Some("name"));

    assert!(matches!(aggs[6], AggregatorImpl::Max(_)));
    assert_eq!(aggs[6].field(), Some("name"));
}

// Row-path update/finalize ------------------------------------------------

#[test]
fn count_all_row_updates_and_finalize() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountAll);
    let columns = make_columns(&[("x", vec!["1", "2"])]);
    agg.update(0, &columns);
    agg.update(1, &columns);
    assert_eq!(agg.finalize(), AggOutput::Count(2));
}

#[test]
fn count_field_counts_non_null_and_missing_zero() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "visits".into(),
    });
    // present column: empty string still counts (Some("") is non-null)
    let cols = make_columns(&[("visits", vec!["", "x"])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    assert_eq!(agg.finalize(), AggOutput::Count(2));

    // missing column: does not increment
    let mut agg2 = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "visits".into(),
    });
    let cols2 = make_columns(&[("other", vec!["1"])]);
    agg2.update(0, &cols2);
    assert_eq!(agg2.finalize(), AggOutput::Count(0));
}

#[test]
fn count_unique_collects_unique_values() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountUnique {
        field: "user".into(),
    });
    let cols = make_columns(&[("user", vec!["u1", "u2", "u1", ""])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    agg.update(2, &cols);
    agg.update(3, &cols);
    assert_eq!(agg.finalize(), AggOutput::CountUnique(3));
}

#[test]
fn sum_ignores_non_numeric_and_sums_numeric() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    let cols = make_columns(&[("amount", vec!["10", "x", "-3"])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    agg.update(2, &cols);
    assert_eq!(agg.finalize(), AggOutput::Sum(7));
}

#[test]
fn avg_computes_mean_and_zero_when_no_numeric() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let cols = make_columns(&[("amount", vec!["10", "20"])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    match agg.finalize() {
        AggOutput::Avg(v) => assert_eq!(v, 15.0),
        other => panic!("expected Avg, got {:?}", other),
    }

    let mut agg2 = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let cols2 = make_columns(&[("amount", vec!["x", "y"])]);
    agg2.update(0, &cols2);
    agg2.update(1, &cols2);
    match agg2.finalize() {
        AggOutput::Avg(v) => assert_eq!(v, 0.0),
        other => panic!("expected Avg, got {:?}", other),
    }
}

#[test]
fn min_prefers_numeric_and_picks_smallest() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "name".into(),
    });
    let cols = make_columns(&[("name", vec!["10", "2", "bob"])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    agg.update(2, &cols);
    assert_eq!(agg.finalize(), AggOutput::Min("2".into()));
}

#[test]
fn max_prefers_numeric_and_picks_largest() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "name".into(),
    });
    let cols = make_columns(&[("name", vec!["10", "2", "bob"])]);
    agg.update(0, &cols);
    agg.update(1, &cols);
    agg.update(2, &cols);
    assert_eq!(agg.finalize(), AggOutput::Max("10".into()));
}

// Event-path update_from_event -------------------------------------------

#[test]
fn sum_from_event_handles_timestamp_special_case() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "timestamp".into(),
    });
    let event = EventFactory::new().with("timestamp", json!(42)).create();
    agg.update_from_event(&event);
    assert_eq!(agg.finalize(), AggOutput::Sum(42));
}

#[test]
fn count_field_from_event_counts_non_null() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::CountField {
        field: "visits".into(),
    });
    let e1 = EventFactory::new()
        .with("payload", json!({"visits": null}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"visits": "x"}))
        .create();
    agg.update_from_event(&e1);
    agg.update_from_event(&e2);
    assert_eq!(agg.finalize(), AggOutput::Count(1));
}

#[test]
fn count_unique_from_event_special_fields() {
    let mut by_ctx = AggregatorImpl::from_spec(&AggregateOpSpec::CountUnique {
        field: "context_id".into(),
    });
    let mut by_evt = AggregatorImpl::from_spec(&AggregateOpSpec::CountUnique {
        field: "event_type".into(),
    });
    let mut by_ts = AggregatorImpl::from_spec(&AggregateOpSpec::CountUnique {
        field: "timestamp".into(),
    });

    let event = EventFactory::new()
        .with("event_type", json!("evt"))
        .with("context_id", json!("c1"))
        .with("timestamp", json!(100))
        .create();

    by_ctx.update_from_event(&event);
    by_evt.update_from_event(&event);
    by_ts.update_from_event(&event);

    assert_eq!(by_ctx.finalize(), AggOutput::CountUnique(1));
    assert_eq!(by_evt.finalize(), AggOutput::CountUnique(1));
    assert_eq!(by_ts.finalize(), AggOutput::CountUnique(1));
}

// Merge behavior ----------------------------------------------------------

#[test]
fn merge_count_all_sums_counts() {
    let mut a = CountAll::new();
    a.update();
    a.update();
    let mut b = CountAll::new();
    b.update();
    let mut ai = AggregatorImpl::CountAll(a);
    let bi = AggregatorImpl::CountAll(b);
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::Count(3));
}

#[test]
fn merge_count_unique_unions_sets() {
    let mut a = CountUnique::new("user".into());
    let cols1 = make_columns(&[("user", vec!["u1", "u2"])]);
    a.update(0, &cols1);
    a.update(1, &cols1);

    let mut b = CountUnique::new("user".into());
    let cols2 = make_columns(&[("user", vec!["u2", "u3"])]);
    b.update(0, &cols2);
    b.update(1, &cols2);

    let mut ai = AggregatorImpl::CountUnique(a);
    let bi = AggregatorImpl::CountUnique(b);
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::CountUnique(3));
}

#[test]
fn merge_sum_adds_values() {
    let mut a = Sum::new("amount".into());
    let cols1 = make_columns(&[("amount", vec!["10"])]);
    a.update(0, &cols1);
    let mut b = Sum::new("amount".into());
    let cols2 = make_columns(&[("amount", vec!["3"])]);
    b.update(0, &cols2);
    let mut ai = AggregatorImpl::Sum(a);
    let bi = AggregatorImpl::Sum(b);
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::Sum(13));
}

#[test]
fn merge_min_and_max_pick_correct_extremes() {
    // Min
    let mut a = Min::new("x".into());
    let cols1 = make_columns(&[("x", vec!["5"])]);
    a.update(0, &cols1);
    let mut b = Min::new("x".into());
    let cols2 = make_columns(&[("x", vec!["2"])]);
    b.update(0, &cols2);
    let mut ai = AggregatorImpl::Min(a);
    let bi = AggregatorImpl::Min(b);
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::Min("2".into()));

    // Max
    let mut c = Max::new("y".into());
    let cols3 = make_columns(&[("y", vec!["5"])]);
    c.update(0, &cols3);
    let mut d = Max::new("y".into());
    let cols4 = make_columns(&[("y", vec!["9"])]);
    d.update(0, &cols4);
    let mut ci = AggregatorImpl::Max(c);
    let di = AggregatorImpl::Max(d);
    ci.merge(&di);
    assert_eq!(ci.finalize(), AggOutput::Max("9".into()));
}

#[test]
fn merge_avg_combines_sum_and_count() {
    let mut a = Avg::new("amount".into());
    let cols1 = make_columns(&[("amount", vec!["10", "20"])]);
    a.update(0, &cols1);
    a.update(1, &cols1);

    let mut b = Avg::new("amount".into());
    let cols2 = make_columns(&[("amount", vec!["30"])]);
    b.update(0, &cols2);

    let mut ai = AggregatorImpl::Avg(a);
    let bi = AggregatorImpl::Avg(b);
    ai.merge(&bi);
    match ai.finalize() {
        AggOutput::Avg(v) => assert_eq!(v, 20.0),
        other => panic!("expected Avg, got {:?}", other),
    }
}

#[test]
fn min_max_finalize_empty_when_no_updates() {
    let agg_min = AggregatorImpl::from_spec(&AggregateOpSpec::Min { field: "x".into() });
    let agg_max = AggregatorImpl::from_spec(&AggregateOpSpec::Max { field: "x".into() });
    match agg_min.finalize() {
        AggOutput::Min(s) => assert_eq!(s, ""),
        other => panic!("expected Min, got {:?}", other),
    }
    match agg_max.finalize() {
        AggOutput::Max(s) => assert_eq!(s, ""),
        other => panic!("expected Max, got {:?}", other),
    }
}

#[test]
fn sum_from_event_ignores_non_numeric_payload() {
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Total {
        field: "amount".into(),
    });
    let e = EventFactory::new()
        .with("payload", json!({"amount":"x"}))
        .create();
    agg.update_from_event(&e);
    assert_eq!(agg.finalize(), AggOutput::Sum(0));
}

#[test]
fn avg_from_event_numeric_and_timestamp_and_ignores_non_numeric() {
    // numeric payload averaging
    let mut agg = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "amount".into(),
    });
    let e1 = EventFactory::new()
        .with("payload", json!({"amount": 10}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"amount": 20}))
        .create();
    let e_bad = EventFactory::new()
        .with("payload", json!({"amount": "x"}))
        .create();
    agg.update_from_event(&e1);
    agg.update_from_event(&e2);
    agg.update_from_event(&e_bad); // ignored
    match agg.finalize() {
        AggOutput::Avg(v) => assert_eq!(v, 15.0),
        other => panic!("expected Avg, got {:?}", other),
    }

    // timestamp special-case averaging
    let mut agg_ts = AggregatorImpl::from_spec(&AggregateOpSpec::Avg {
        field: "timestamp".into(),
    });
    let e3 = EventFactory::new().with("timestamp", json!(100)).create();
    let e4 = EventFactory::new().with("timestamp", json!(200)).create();
    agg_ts.update_from_event(&e3);
    agg_ts.update_from_event(&e4);
    match agg_ts.finalize() {
        AggOutput::Avg(v) => assert_eq!(v, 150.0),
        other => panic!("expected Avg, got {:?}", other),
    }
}

#[test]
fn min_max_from_event_numeric_vs_string_behavior() {
    // Min prefers numeric when parseable
    let mut min_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Min {
        field: "name".into(),
    });
    let e1 = EventFactory::new()
        .with("payload", json!({"name": "10"}))
        .create();
    let e2 = EventFactory::new()
        .with("payload", json!({"name": "2"}))
        .create();
    let e3 = EventFactory::new()
        .with("payload", json!({"name": "bob"}))
        .create();
    min_agg.update_from_event(&e1);
    min_agg.update_from_event(&e2);
    min_agg.update_from_event(&e3);
    assert_eq!(min_agg.finalize(), AggOutput::Min("2".into()));

    // Max prefers numeric when parseable
    let mut max_agg = AggregatorImpl::from_spec(&AggregateOpSpec::Max {
        field: "name".into(),
    });
    max_agg.update_from_event(&e1);
    max_agg.update_from_event(&e2);
    max_agg.update_from_event(&e3);
    assert_eq!(max_agg.finalize(), AggOutput::Max("10".into()));
}

#[test]
fn merge_count_field_adds_counts() {
    let mut a = CountField::new("visits".into());
    a.update_non_null();
    a.update_non_null();
    let mut b = CountField::new("visits".into());
    b.update_non_null();
    let mut ai = AggregatorImpl::CountField(a);
    let bi = AggregatorImpl::CountField(b);
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::Count(3));
}

#[test]
fn merge_mismatched_variants_is_noop() {
    let mut ai = AggregatorImpl::CountAll(CountAll::new());
    // bump count to 1
    if let AggregatorImpl::CountAll(ref mut inner) = ai {
        inner.update();
    }
    let bi = AggregatorImpl::Sum(Sum::new("x".into()));
    ai.merge(&bi);
    assert_eq!(ai.finalize(), AggOutput::Count(1));
}
