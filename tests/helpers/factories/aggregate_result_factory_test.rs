use crate::command::types::TimeGranularity;
use crate::engine::core::read::aggregate::partial::AggState;
use crate::engine::core::read::aggregate::plan::AggregateOpSpec;
use crate::test_helpers::factories::AggregateResultFactory;

#[test]
fn builds_aggregate_result_and_finalizes_headers_and_rows() {
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

    let agg = AggregateResultFactory::new()
        .with_group_by(vec!["country"])
        .with_time_bucket(TimeGranularity::Month)
        .with_specs(specs.clone())
        .add_group_key(
            Some(1725148800),
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
                    max_str: Some("zoe".into()),
                },
            ],
        )
        .add_group_key(
            Some(1725148800),
            &["DE"],
            vec![
                AggState::CountAll { count: 1 },
                AggState::Avg { sum: 5, count: 1 },
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
    let headers: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    assert!(headers.contains(&"bucket".to_string()));
    assert!(headers.contains(&"country".to_string()));
    assert!(headers.contains(&"count".to_string()));
    assert!(headers.contains(&"avg_amount".to_string()));
    assert!(headers.contains(&"min_name".to_string()));
    assert!(headers.contains(&"max_name".to_string()));
    assert_eq!(table.rows.len(), 2);
}
