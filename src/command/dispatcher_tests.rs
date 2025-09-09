use crate::command::parser::command::parse_command;
use crate::command::parser::error::ParseError;
use crate::command::types::{Command, FieldSpec};
use serde_json::json;

#[cfg(test)]
mod dispatcher_tests {
    use super::*;

    #[test]
    fn test_parse_command_store_basic() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9 }"#;
        let command = parse_command(input).expect("Failed to parse STORE command");

        assert_eq!(
            command,
            Command::Store {
                event_type: "order_created".to_string(),
                context_id: "user-9".to_string(),
                payload: json!({ "id": 9 }),
            }
        );
    }

    #[test]
    fn test_parse_command_store_with_complex_payload_should_fail() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { "id": 9, "details": { "product": "book" } }"#;
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::NestedJsonNotAllowed)),
            "Expected NestedJsonNotAllowed error"
        );
    }

    #[test]
    fn test_parse_command_query_basic() {
        let input = r#"QUERY orders WHERE status = "pending""#;
        let command = parse_command(input).expect("Failed to parse QUERY command");

        if let Command::Query { event_type, .. } = command {
            assert_eq!(event_type, "orders");
        } else {
            panic!("Expected Command::Query");
        }
    }

    #[test]
    fn test_parse_command_query_with_limit() {
        let input = r#"QUERY login_events WHERE success = true LIMIT 100"#;
        let command = parse_command(input).expect("Failed to parse QUERY with LIMIT");

        if let Command::Query {
            event_type, limit, ..
        } = command
        {
            assert_eq!(event_type, "login_events");
            assert_eq!(limit, Some(100));
        } else {
            panic!("Expected Command::Query");
        }
    }

    #[test]
    fn test_parse_command_replay_with_full_arguments() {
        let input = r#"REPLAY orders FOR user-123 SINCE "2024-01-01T00:00:00Z""#;
        let command = parse_command(input).expect("Failed to parse REPLAY command");

        if let Command::Replay {
            event_type,
            context_id,
            since,
        } = command
        {
            assert_eq!(event_type.unwrap(), "orders");
            assert_eq!(context_id, "user-123");
            assert_eq!(since.unwrap(), "2024-01-01T00:00:00Z");
        } else {
            panic!("Expected Command::Replay");
        }
    }

    #[test]
    fn test_parse_command_replay_without_event_type() {
        let input = r#"REPLAY FOR user-123"#;
        let command = parse_command(input).expect("Failed to parse REPLAY without event_type");

        if let Command::Replay {
            event_type,
            context_id,
            since,
        } = command
        {
            assert_eq!(event_type, None);
            assert_eq!(context_id, "user-123");
            assert_eq!(since, None);
        } else {
            panic!("Expected Command::Replay");
        }
    }

    #[test]
    fn test_parse_command_define_basic() {
        let input = r#"DEFINE order_created FIELDS { "id": "number", "status": "string" }"#;
        let command = parse_command(input).expect("Failed to parse DEFINE command");

        if let Command::Define {
            event_type,
            version: _,
            schema,
        } = command
        {
            assert_eq!(event_type, "order_created");
            assert_eq!(
                schema.fields["id"],
                FieldSpec::Primitive("number".to_string())
            );
            assert_eq!(
                schema.fields["status"],
                FieldSpec::Primitive("string".to_string())
            );
        } else {
            panic!("Expected Command::Define");
        }
    }

    #[test]
    fn test_parse_command_define_with_more_fields() {
        let input = r#"DEFINE user_signed_up FIELDS { "user_id": "string", "email": "string", "age": "number" }"#;
        let command = parse_command(input).expect("Failed to parse DEFINE with extra fields");

        if let Command::Define {
            event_type,
            version: _,
            schema,
        } = command
        {
            assert_eq!(event_type, "user_signed_up");
            assert_eq!(
                schema.fields["user_id"],
                FieldSpec::Primitive("string".to_string())
            );
            assert_eq!(
                schema.fields["email"],
                FieldSpec::Primitive("string".to_string())
            );
            assert_eq!(
                schema.fields["age"],
                FieldSpec::Primitive("number".to_string())
            );
        } else {
            panic!("Expected Command::Define");
        }
    }
    #[test]
    fn test_parse_command_ping() {
        let input = "PING";
        let command = parse_command(input).expect("Failed to parse PING command");

        assert_eq!(command, Command::Ping);
    }

    #[test]
    fn test_parse_command_flush() {
        let input = "FLUSH";
        let command = parse_command(input).expect("Failed to parse FLUSH command");

        assert_eq!(command, Command::Flush);
    }

    #[test]
    fn test_parse_command_invalid_token_should_fail() {
        let input = r#"STORE order_created FOR user-9 PAYLOAD { [ "bad" ] }"#;
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::UnexpectedToken(_))),
            "Expected UnexpectedToken error for invalid JSON"
        );
    }

    #[test]
    fn test_parse_command_define_with_enum_field() {
        let input = r#"DEFINE subscription FIELDS { "plan": ["pro", "basic"] }"#;
        let command = parse_command(input).expect("Failed to parse DEFINE with enum field");

        if let Command::Define {
            event_type, schema, ..
        } = command
        {
            assert_eq!(event_type, "subscription");
            assert_eq!(
                schema.fields["plan"],
                FieldSpec::Enum(vec!["pro".to_string(), "basic".to_string()])
            );
        } else {
            panic!("Expected Command::Define");
        }
    }

    #[test]
    fn test_parse_command_define_with_invalid_enum_should_fail() {
        let input = r#"DEFINE subscription FIELDS { "plan": ["pro", 1] }"#;
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::InvalidJson(_))),
            "Expected InvalidJson for non-string enum variant"
        );
    }

    #[test]
    fn test_parse_command_define_with_nullable_field() {
        let input = r#"DEFINE user_created FIELDS { "email": "string | null" }"#;
        let command = parse_command(input).expect("Failed to parse DEFINE with nullable field");

        if let Command::Define {
            event_type, schema, ..
        } = command
        {
            assert_eq!(event_type, "user_created");
            assert_eq!(
                schema.fields["email"],
                FieldSpec::Primitive("string | null".to_string())
            );
        } else {
            panic!("Expected Command::Define");
        }
    }

    #[test]
    fn test_parse_command_unknown_command_should_fail() {
        let input = "INVALIDCOMMAND something";
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::UnknownCommand(_))),
            "Expected UnknownCommand error"
        );
    }

    #[test]
    fn test_parse_command_query_invalid_syntax_should_fail() {
        let input = r#"QUERY orders WHERE"#;
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::MissingArgument(_))),
            "Expected MissingArgument error for incomplete WHERE clause"
        );
    }

    #[test]
    fn test_parse_command_store_missing_payload_should_fail() {
        let input = r#"STORE order_created FOR user-9"#;
        let result = parse_command(input);

        assert!(
            matches!(result, Err(ParseError::MissingArgument(_))),
            "Expected MissingArgument error for missing PAYLOAD"
        );
    }
}
