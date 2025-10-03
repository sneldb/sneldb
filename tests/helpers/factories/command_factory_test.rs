use crate::command::types::{AggSpec, Command, CompareOp, TimeGranularity};
use crate::test_helpers::factories::{CommandFactory, ExprFactory};
use serde_json::json;

#[test]
fn test_command_store_with_custom_payload() {
    let cmd = CommandFactory::store()
        .with_payload(json!({"foo": "bar"}))
        .create();

    if let Command::Store { payload, .. } = cmd {
        assert_eq!(payload["foo"], "bar");
    } else {
        panic!("Expected store command");
    }
}

#[test]
fn test_command_query_with_where_clause() {
    let expr = ExprFactory::new()
        .with_field("age")
        .with_op(CompareOp::Gt)
        .with_value(json!(18))
        .create();

    let cmd = CommandFactory::query()
        .with_where_clause(expr.clone())
        .create();

    if let Command::Query {
        where_clause: Some(w),
        ..
    } = cmd
    {
        assert_eq!(&w, &expr);
    } else {
        panic!("Expected query command with where clause");
    }
}
#[test]
fn test_command_replay_with_custom_values() {
    let cmd = CommandFactory::replay()
        .with_event_type("login")
        .with_context_id("ctx99")
        .with_since("2024-01-01T12:00:00Z")
        .create();

    if let Command::Replay {
        event_type,
        context_id,
        since,
        ..
    } = cmd
    {
        assert_eq!(event_type, Some("login".into()));
        assert_eq!(context_id, "ctx99");
        assert_eq!(since, Some("2024-01-01T12:00:00Z".into()));
    } else {
        panic!("Expected Command::Replay variant");
    }
}

#[test]
fn test_command_query_with_return_fields() {
    let cmd = CommandFactory::query()
        .with_return_fields(vec!["context_id", "country", "plan"])
        .create();

    if let Command::Query {
        return_fields: Some(f),
        ..
    } = cmd
    {
        assert_eq!(
            f,
            vec![
                "context_id".to_string(),
                "country".to_string(),
                "plan".to_string()
            ]
        );
    } else {
        panic!("Expected query command with return_fields");
    }
}

#[test]
fn test_command_replay_with_return_fields() {
    let cmd = CommandFactory::replay()
        .with_return_fields(vec!["event_type", "timestamp"])
        .create();

    if let Command::Replay {
        return_fields: Some(f),
        ..
    } = cmd
    {
        assert_eq!(f, vec!["event_type".to_string(), "timestamp".to_string()]);
    } else {
        panic!("Expected replay command with return_fields");
    }
}

#[test]
fn test_command_query_with_link_field() {
    let cmd = CommandFactory::query().with_link_field("user_id").create();

    if let Command::Query { link_field, .. } = cmd {
        assert_eq!(link_field, Some("user_id".to_string()));
    } else {
        panic!("Expected query command with link_field");
    }
}

#[test]
fn test_command_query_with_aggs_helpers() {
    let cmd = CommandFactory::query()
        .add_count()
        .add_count_unique("ctx")
        .add_total("amount")
        .add_avg("amount")
        .add_min("amount")
        .add_max("amount")
        .create();

    if let Command::Query { aggs: Some(a), .. } = cmd {
        assert_eq!(a.len(), 6);
        assert!(matches!(a[0], AggSpec::Count { unique_field: None }));
        match &a[1] {
            AggSpec::Count { unique_field } => assert_eq!(unique_field, &Some("ctx".to_string())),
            _ => panic!("Expected Count UNIQUE"),
        }
        assert!(matches!(&a[2], AggSpec::Total { field } if field == "amount"));
        assert!(matches!(&a[3], AggSpec::Avg { field } if field == "amount"));
        assert!(matches!(&a[4], AggSpec::Min { field } if field == "amount"));
        assert!(matches!(&a[5], AggSpec::Max { field } if field == "amount"));
    } else {
        panic!("Expected query command with aggs");
    }
}

#[test]
fn test_command_query_with_time_bucket_and_group_by() {
    let cmd = CommandFactory::query()
        .with_time_bucket(TimeGranularity::Day)
        .with_group_by(vec!["country", "plan"])
        .create();

    if let Command::Query {
        time_bucket: Some(tb),
        group_by: Some(g),
        ..
    } = cmd
    {
        assert!(matches!(tb, TimeGranularity::Day));
        assert_eq!(g, vec!["country".to_string(), "plan".to_string()]);
    } else {
        panic!("Expected query command with time_bucket and group_by");
    }
}

#[test]
fn test_command_query_with_aggs_vec() {
    let aggs = vec![
        AggSpec::Count { unique_field: None },
        AggSpec::Avg {
            field: "amount".to_string(),
        },
    ];

    let cmd = CommandFactory::query().with_aggs(aggs.clone()).create();

    if let Command::Query { aggs: Some(a), .. } = cmd {
        assert_eq!(a, aggs);
    } else {
        panic!("Expected query command with aggs via with_aggs");
    }
}
