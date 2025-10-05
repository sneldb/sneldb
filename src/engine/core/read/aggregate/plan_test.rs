use crate::command::types::{AggSpec, TimeGranularity};
use crate::engine::core::read::aggregate::plan::{AggregateOpSpec, AggregatePlan};
use crate::test_helpers::factories::CommandFactory;

#[test]
fn from_command_none_when_no_aggs() {
    let cmd = CommandFactory::query().with_event_type("evt").create();
    let plan = AggregatePlan::from_command(&cmd);
    assert!(plan.is_none());
}

#[test]
fn from_command_builds_ops_for_basic_aggs() {
    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .with_aggs(vec![
            AggSpec::Count { unique_field: None },
            AggSpec::CountField {
                field: "visits".into(),
            },
            AggSpec::Count {
                unique_field: Some("user".into()),
            },
            AggSpec::Total {
                field: "amount".into(),
            },
            AggSpec::Avg {
                field: "amount".into(),
            },
            AggSpec::Min {
                field: "name".into(),
            },
            AggSpec::Max {
                field: "name".into(),
            },
        ])
        .create();
    let plan = AggregatePlan::from_command(&cmd).expect("expected plan");

    assert_eq!(plan.ops.len(), 7);
    assert_eq!(plan.ops[0], AggregateOpSpec::CountAll);
    assert_eq!(
        plan.ops[1],
        AggregateOpSpec::CountField {
            field: "visits".into()
        }
    );
    assert_eq!(
        plan.ops[2],
        AggregateOpSpec::CountUnique {
            field: "user".into()
        }
    );
    assert_eq!(
        plan.ops[3],
        AggregateOpSpec::Total {
            field: "amount".into()
        }
    );
    assert_eq!(
        plan.ops[4],
        AggregateOpSpec::Avg {
            field: "amount".into()
        }
    );
    assert_eq!(
        plan.ops[5],
        AggregateOpSpec::Min {
            field: "name".into()
        }
    );
    assert_eq!(
        plan.ops[6],
        AggregateOpSpec::Max {
            field: "name".into()
        }
    );

    assert!(plan.group_by.is_none());
    assert!(plan.time_bucket.is_none());
}

#[test]
fn from_command_carries_group_by_and_time_bucket() {
    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .with_time_bucket(TimeGranularity::Day)
        .with_group_by(vec!["country", "plan"])
        .create();
    let plan = AggregatePlan::from_command(&cmd).expect("expected plan");

    assert_eq!(plan.ops, vec![AggregateOpSpec::CountAll]);
    assert_eq!(plan.group_by, Some(vec!["country".into(), "plan".into()]));
    assert_eq!(plan.time_bucket, Some(TimeGranularity::Day));
}
