use super::remember;
use crate::command::parser::error::ParseError;
use crate::command::types::Command;

#[test]
fn parse_remember_basic() {
    let cmd =
        remember::parse("REMEMBER QUERY events WHERE context_id = \"foo\" LIMIT 10 AS hot_events")
            .expect("failed to parse remember command");

    match cmd {
        Command::RememberQuery { spec } => {
            assert_eq!(spec.name, "hot_events");
            match *spec.query {
                Command::Query {
                    ref event_type,
                    ref limit,
                    ..
                } => {
                    assert_eq!(event_type, "events");
                    assert_eq!(limit, &Some(10));
                }
                other => panic!("expected inner query, got {:?}", other),
            }
        }
        other => panic!("expected remember command, got {:?}", other),
    }
}

#[test]
fn parse_remember_missing_alias() {
    let err = remember::parse("REMEMBER QUERY events WHERE id = 1").unwrap_err();
    assert!(matches!(err, ParseError::MissingArgument(_)));
}
