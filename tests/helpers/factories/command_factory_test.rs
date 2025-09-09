use crate::command::types::Command;
use crate::command::types::CompareOp;
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
    } = cmd
    {
        assert_eq!(event_type, Some("login".into()));
        assert_eq!(context_id, "ctx99");
        assert_eq!(since, Some("2024-01-01T12:00:00Z".into()));
    } else {
        panic!("Expected Command::Replay variant");
    }
}
