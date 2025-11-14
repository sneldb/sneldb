use crate::command::parser::commands::grant_permission;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod grant_permission_tests {
    use super::*;

    // ─────────────────────────────
    // Valid Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_grant_permission_read_single_event_type() {
        let input = "GRANT READ ON order_created TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT READ permission");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_write_single_event_type() {
        let input = "GRANT WRITE ON payment_succeeded TO user456";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT WRITE permission");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["write".to_string()],
                event_types: vec!["payment_succeeded".to_string()],
                user_id: "user456".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_read_write_single_event_type() {
        let input = "GRANT READ,WRITE ON order_created TO user789";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT READ,WRITE permission");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string(), "write".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user789".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_write_read_single_event_type() {
        let input = "GRANT WRITE,READ ON order_created TO user789";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT WRITE,READ permission");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["write".to_string(), "read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user789".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_read_multiple_event_types() {
        let input = "GRANT READ ON order_created,payment_succeeded TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT READ with multiple event types");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string(), "payment_succeeded".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_read_write_multiple_event_types() {
        let input = "GRANT READ,WRITE ON order_created,payment_succeeded,order_cancelled TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT READ,WRITE with multiple event types");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string(), "write".to_string()],
                event_types: vec![
                    "order_created".to_string(),
                    "payment_succeeded".to_string(),
                    "order_cancelled".to_string(),
                ],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_case_insensitive() {
        let input = "grant read on order_created to user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse lowercase GRANT command");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_mixed_case() {
        let input = "GrAnT ReAd On order_created To user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse mixed case GRANT command");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_with_string_literal_event_type() {
        let input = r#"GRANT READ ON "order-created" TO user123"#;
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with string literal event type");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order-created".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_with_string_literal_user_id() {
        let input = r#"GRANT READ ON order_created TO "user-123""#;
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with string literal user_id");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user-123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_with_string_literal_both() {
        let input = r#"GRANT READ ON "order-created" TO "user-123""#;
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with string literals");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order-created".to_string()],
                user_id: "user-123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_user_id_with_underscore() {
        let input = "GRANT READ ON order_created TO user_123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with underscore in user_id");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user_123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_user_id_with_hyphen() {
        let input = "GRANT READ ON order_created TO user-456";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with hyphen in user_id");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user-456".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_event_type_with_numbers() {
        let input = "GRANT READ ON event123 TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with numbers in event_type");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["event123".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    // ─────────────────────────────
    // Error Cases - Missing Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_grant_permission_missing_grant_keyword() {
        let input = "READ ON order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing GRANT keyword");
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_empty_input() {
        let input = "";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to empty input");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "GRANT");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_missing_permissions() {
        let input = "GRANT ON order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing permissions");
        // The parser sees "ON" and treats it as an invalid permission name
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(
                    msg.contains("Invalid permission") || msg.contains("ON"),
                    "Error message should mention invalid permission: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected UnexpectedToken error, got: {:?}", e),
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_missing_on_keyword() {
        let input = "GRANT READ order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing ON keyword");
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "ON");
            }
            Err(ParseError::UnexpectedToken(_)) => {} // Could also be this
            _ => panic!("Expected ExpectedKeyword or UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_missing_event_types() {
        let input = "GRANT READ ON TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing event types");
        // The parser will consume "TO" as an event_type, then fail when trying to match "TO" keyword
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "TO");
            }
            Err(ParseError::MissingArgument(arg)) => {
                assert!(arg.contains("event_type"));
            }
            Err(e) => panic!("Expected ExpectedKeyword or MissingArgument error, got: {:?}", e),
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_missing_to_keyword() {
        let input = "GRANT READ ON order_created user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing TO keyword");
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "TO");
            }
            Err(ParseError::UnexpectedToken(_)) => {} // Could also be this
            _ => panic!("Expected ExpectedKeyword or UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_missing_user_id() {
        let input = "GRANT READ ON order_created TO";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing user_id");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "user_id");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    // ─────────────────────────────
    // Error Cases - Invalid Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_grant_permission_invalid_permission() {
        let input = "GRANT EXECUTE ON order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to invalid permission");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Invalid permission") || msg.contains("EXECUTE"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_invalid_permission_mixed() {
        let input = "GRANT READ,EXECUTE ON order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to invalid permission in list");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Invalid permission") || msg.contains("EXECUTE"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_wrong_keyword_after_grant() {
        let input = "GRANT FROM order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to wrong keyword after GRANT");
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            Err(ParseError::ExpectedKeyword(_, _)) => {}
            _ => panic!("Expected UnexpectedToken or ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_wrong_keyword_instead_of_on() {
        let input = "GRANT READ FROM order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to wrong keyword instead of ON");
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "ON");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_wrong_keyword_instead_of_to() {
        let input = "GRANT READ ON order_created FROM user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to wrong keyword instead of TO");
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "TO");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_invalid_token_as_event_type() {
        let input = "GRANT READ ON 123 TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to number as event_type");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("event_type") || msg.contains("Expected"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_invalid_token_as_user_id() {
        let input = "GRANT READ ON order_created TO 123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to number as user_id");
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    // ─────────────────────────────
    // Error Cases - Extra Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_grant_permission_extra_tokens_at_end() {
        let input = "GRANT READ ON order_created TO user123 extra_token";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens") || msg.contains("extra_token"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_grant_permission_extra_tokens_multiple() {
        let input = "GRANT READ ON order_created TO user123 extra token1 token2";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to multiple extra tokens");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens") || msg.contains("extra"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    // ─────────────────────────────
    // Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_grant_permission_permissions_with_spaces() {
        let input = "GRANT READ , WRITE ON order_created TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with spaces around comma");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string(), "write".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_event_types_with_spaces() {
        let input = "GRANT READ ON order_created , payment_succeeded TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with spaces around event type comma");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string(), "payment_succeeded".to_string()],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_trailing_comma_should_fail() {
        let input = "GRANT READ, ON order_created TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to trailing comma in permissions");
    }

    #[test]
    fn test_parse_grant_permission_trailing_comma_event_types_should_fail() {
        let input = "GRANT READ ON order_created, TO user123";
        let tokens = tokenize(input);

        let result = grant_permission::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to trailing comma in event types");
    }

    #[test]
    fn test_parse_grant_permission_many_event_types() {
        let input = "GRANT READ ON e1,e2,e3,e4,e5 TO user123";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with many event types");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec![
                    "e1".to_string(),
                    "e2".to_string(),
                    "e3".to_string(),
                    "e4".to_string(),
                    "e5".to_string(),
                ],
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_grant_permission_long_user_id() {
        let input = "GRANT READ ON order_created TO very_long_user_id_with_many_parts_12345";
        let tokens = tokenize(input);

        let command = grant_permission::parse(&tokens)
            .expect("Failed to parse GRANT with long user_id");

        assert_eq!(
            command,
            Command::GrantPermission {
                permissions: vec!["read".to_string()],
                event_types: vec!["order_created".to_string()],
                user_id: "very_long_user_id_with_many_parts_12345".to_string(),
            }
        );
    }
}

