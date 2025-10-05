use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::partial::AggState;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::engine::core::read::result::QueryResult;
use crate::test_helpers::factories::{
    AggregateResultFactory, ColumnSpecFactory, GroupKeyFactory, SelectionResultFactory,
};
use serde_json::json;
use std::collections::HashSet;

#[test]
fn selection_merge_and_finalize_appends_rows_and_keeps_columns() {
    let columns = vec![
        ColumnSpecFactory::string("a"),
        ColumnSpecFactory::integer("b"),
    ];
    let mut s1 = SelectionResultFactory::new()
        .with_columns(columns.clone())
        .with_row(vec![json!("x"), json!(1)])
        .create();
    let s2 = SelectionResultFactory::new()
        .with_columns(columns.clone())
        .with_row(vec![json!("y"), json!(2)])
        .create();

    s1.merge(s2);
    let table = s1.finalize();

    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[0].name, "a");
    assert_eq!(table.columns[1].logical_type, "Integer");
    assert_eq!(table.rows.len(), 2);
    assert_eq!(table.rows[0][0], json!("x"));
    assert_eq!(table.rows[1][1], json!(2));
}

#[test]
fn aggregate_finalize_builds_correct_columns_for_all_specs_and_headers() {
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
    let agg = AggregateResultFactory::new()
        .with_group_by(vec!["country"])
        .with_time_bucket(TimeGranularity::Month)
        .with_specs(specs.clone())
        .create();

    let table = agg.finalize();

    let headers: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(headers[0], "bucket");
    assert!(headers.contains(&"country".into()));
    assert!(headers.contains(&"count".into()));
    assert!(headers.contains(&"count_visits".into()));
    assert!(headers.contains(&"count_unique_user".into()));
    assert!(headers.contains(&"total_amount".into()));
    assert!(headers.contains(&"avg_amount".into()));
    assert!(headers.contains(&"min_name".into()));
    assert!(headers.contains(&"max_name".into()));
}

#[test]
fn aggregate_finalize_outputs_rows_matching_group_keys_and_states() {
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

    let mut set: HashSet<String> = HashSet::new();
    set.insert("u1".into());
    set.insert("u2".into());

    // Use GroupKeyFactory to build group keys and feed their fields to the builder
    let gk_us = GroupKeyFactory::new()
        .with_bucket(1725148800)
        .with_groups(&["US"])
        .create();
    let gk_us_groups: Vec<&str> = gk_us.groups.iter().map(|s| s.as_str()).collect();

    let gk_de = GroupKeyFactory::new()
        .with_bucket(1725148800)
        .with_groups(&["DE"])
        .create();
    let gk_de_groups: Vec<&str> = gk_de.groups.iter().map(|s| s.as_str()).collect();

    let agg = AggregateResultFactory::new()
        .with_group_by(vec!["country"])
        .with_time_bucket(TimeGranularity::Month)
        .with_specs(specs.clone())
        .add_group_key(
            gk_us.bucket,
            &gk_us_groups,
            vec![
                AggState::CountAll { count: 3 },
                AggState::CountAll { count: 3 },
                AggState::CountUnique {
                    values: set.clone(),
                },
                AggState::Sum { sum: 60 },
                AggState::Avg { sum: 60, count: 3 },
                AggState::Min {
                    min_num: None,
                    min_str: Some("amy".into()),
                },
                AggState::Max {
                    max_num: None,
                    max_str: Some("zoe".into()),
                },
            ],
        )
        .add_group_key(
            gk_de.bucket,
            &gk_de_groups,
            vec![
                AggState::CountAll { count: 2 },
                AggState::CountAll { count: 2 },
                AggState::CountUnique {
                    values: HashSet::from(["u3".into()]),
                },
                AggState::Sum { sum: 15 },
                AggState::Avg { sum: 15, count: 2 },
                AggState::Min {
                    min_num: Some(1),
                    min_str: None,
                },
                AggState::Max {
                    max_num: Some(9),
                    max_str: None,
                },
            ],
        )
        .create();

    let table = agg.finalize();
    assert_eq!(table.rows.len(), 2);

    // Validate one row's values mapping across specs
    let headers: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let idx = |h: &str| headers.iter().position(|c| c == h).unwrap();

    let mut seen_us = false;
    let mut seen_de = false;
    for row in table.rows {
        let country = row[idx("country")].as_str().unwrap();
        if country == "US" {
            seen_us = true;
            assert_eq!(row[idx("bucket")], json!(1725148800i64));
            assert_eq!(row[idx("count")], json!(3));
            assert_eq!(row[idx("count_visits")], json!(3));
            assert_eq!(row[idx("count_unique_user")], json!(2));
            assert_eq!(row[idx("total_amount")], json!(60));
            assert_eq!(row[idx("avg_amount")].as_f64().unwrap(), 20.0);
            assert_eq!(row[idx("min_name")], json!("amy"));
            assert_eq!(row[idx("max_name")], json!("zoe"));
        } else if country == "DE" {
            seen_de = true;
            assert_eq!(row[idx("bucket")], json!(1725148800i64));
            assert_eq!(row[idx("count")], json!(2));
            assert_eq!(row[idx("count_visits")], json!(2));
            assert_eq!(row[idx("count_unique_user")], json!(1));
            assert_eq!(row[idx("total_amount")], json!(15));
            assert_eq!(row[idx("avg_amount")].as_f64().unwrap(), 7.5);
            assert_eq!(row[idx("min_name")], json!(1));
            assert_eq!(row[idx("max_name")], json!(9));
        }
    }
    assert!(seen_us && seen_de);
}

#[test]
fn aggregate_merge_combines_groups_and_merges_states() {
    let specs = vec![
        AggregateOpSpec::CountAll,
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

    let mut left = AggregateResultFactory::new()
        .with_group_by(vec!["country"])
        .with_specs(specs.clone())
        .add_group_key(
            None,
            &["US"],
            vec![
                AggState::CountAll { count: 2 },
                AggState::Avg { sum: 10, count: 2 },
                AggState::Min {
                    min_num: None,
                    min_str: Some("bob".into()),
                },
                AggState::Max {
                    max_num: None,
                    max_str: Some("max".into()),
                },
            ],
        )
        .create();
    let right = AggregateResultFactory::new()
        .with_group_by(vec!["country"])
        .with_specs(specs.clone())
        .add_group_key(
            None,
            &["US"],
            vec![
                AggState::CountAll { count: 3 },
                AggState::Avg { sum: 30, count: 3 },
                AggState::Min {
                    min_num: None,
                    min_str: Some("adam".into()),
                },
                AggState::Max {
                    max_num: None,
                    max_str: Some("zoe".into()),
                },
            ],
        )
        .create();

    left.merge(right);
    let table = left.finalize();

    let headers: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    let idx = |h: &str| headers.iter().position(|c| c == h).unwrap();
    assert_eq!(table.rows.len(), 1);
    let row = &table.rows[0];
    // CountAll merged: 2 + 3 => 5
    assert_eq!(row[idx("count")], json!(5));
    // Avg merged: (10/2) and (30/3) => combined (40/5) => 8.0
    assert!((row[idx("avg_amount")].as_f64().unwrap() - 8.0).abs() < 1e-9);
    // Min merged: min("adam", "bob") => "adam"; Max merged: max("max","zoe") => "zoe"
    assert_eq!(row[idx("min_name")], json!("adam"));
    assert_eq!(row[idx("max_name")], json!("zoe"));
}

#[test]
fn query_result_merge_and_finalize_routes_variants() {
    // Selection path
    let cols = vec![ColumnSpecFactory::string("a")];
    let mut sel_a = QueryResult::Selection(
        SelectionResultFactory::new()
            .with_columns(cols.clone())
            .with_row(vec![json!("x")])
            .create(),
    );
    let sel_b = QueryResult::Selection(
        SelectionResultFactory::new()
            .with_columns(cols.clone())
            .with_row(vec![json!("y")])
            .create(),
    );
    sel_a.merge(sel_b);
    let table = sel_a.finalize_table();
    assert_eq!(table.rows.len(), 2);

    // Aggregation path
    let specs = vec![AggregateOpSpec::CountAll];
    let mut agga = QueryResult::Aggregation(
        AggregateResultFactory::new()
            .with_specs(specs.clone())
            .add_group_key(None, &[], vec![AggState::CountAll { count: 1 }])
            .create(),
    );
    let aggb = QueryResult::Aggregation(
        AggregateResultFactory::new()
            .with_specs(specs)
            .add_group_key(None, &[], vec![AggState::CountAll { count: 1 }])
            .create(),
    );
    agga.merge(aggb);
    let t2 = agga.finalize_table();
    assert_eq!(t2.rows.len(), 1);
    assert_eq!(t2.rows[0][0], json!(2));
}
