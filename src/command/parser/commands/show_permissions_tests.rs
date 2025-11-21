use crate::command::parser::commands::show_permissions;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod show_permissions_tests {
    use super::*;

    // ─────────────────────────────
    // Valid Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_show_permissions_simple() {
        let input = "SHOW PERMISSIONS FOR user123";
        let tokens = tokenize(input);

        let command =
            show_permissions::parse(&tokens).expect("Failed to parse SHOW PERMISSIONS command");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_case_insensitive() {
        let input = "show permissions for user123";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse lowercase SHOW PERMISSIONS command");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_mixed_case() {
        let input = "ShOw PeRmIsSiOnS fOr user123";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse mixed case SHOW PERMISSIONS command");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_with_string_literal_user_id() {
        let input = r#"SHOW PERMISSIONS FOR "user-123""#;
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with string literal user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user-123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_with_underscore() {
        let input = "SHOW PERMISSIONS FOR user_123";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with underscore in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user_123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_with_hyphen() {
        let input = "SHOW PERMISSIONS FOR user-456";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with hyphen in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user-456".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_with_numbers() {
        let input = "SHOW PERMISSIONS FOR user123";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with numbers in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_long_user_id() {
        let input = "SHOW PERMISSIONS FOR very_long_user_id_with_many_parts_12345";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with long user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "very_long_user_id_with_many_parts_12345".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_with_special_chars_in_string() {
        let input = r#"SHOW PERMISSIONS FOR "user@example.com""#;
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with special chars in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user@example.com".to_string(),
            }
        );
    }

    // ─────────────────────────────
    // Error Cases - Missing Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_show_permissions_missing_show_keyword() {
        let input = "PERMISSIONS FOR user123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing SHOW keyword"
        );
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_empty_input() {
        let input = "";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to empty input");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "SHOW");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_missing_permissions_keyword() {
        let input = "SHOW FOR user123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing PERMISSIONS keyword"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "PERMISSIONS");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_missing_for_keyword() {
        let input = "SHOW PERMISSIONS user123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing FOR keyword"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "FOR");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_missing_user_id() {
        let input = "SHOW PERMISSIONS FOR";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing user_id");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "user_id");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_only_show() {
        let input = "SHOW";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to only SHOW keyword");
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "PERMISSIONS");
            }
            Err(ParseError::MissingArgument(_)) => {}
            _ => panic!("Expected ExpectedKeyword or MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_show_permissions_only() {
        let input = "SHOW PERMISSIONS";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing FOR keyword"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "FOR");
            }
            Err(ParseError::MissingArgument(_)) => {}
            _ => panic!("Expected ExpectedKeyword or MissingArgument error"),
        }
    }

    // ─────────────────────────────
    // Error Cases - Invalid Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_show_permissions_wrong_keyword_after_show() {
        let input = "SHOW TABLES FOR user123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to wrong keyword after SHOW"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "PERMISSIONS");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_wrong_keyword_instead_of_for() {
        let input = "SHOW PERMISSIONS TO user123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to wrong keyword instead of FOR"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "FOR");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_invalid_token_as_user_id() {
        let input = "SHOW PERMISSIONS FOR 123";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to number as user_id");
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_symbol_as_user_id() {
        let input = "SHOW PERMISSIONS FOR =";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to symbol as user_id");
        match result {
            Err(ParseError::UnexpectedToken(_)) => {}
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    // ─────────────────────────────
    // Error Cases - Extra Tokens
    // ─────────────────────────────

    #[test]
    fn test_parse_show_permissions_extra_tokens_at_end() {
        let input = "SHOW PERMISSIONS FOR user123 extra_token";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens") || msg.contains("extra_token"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_extra_tokens_multiple() {
        let input = "SHOW PERMISSIONS FOR user123 extra token1 token2";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to multiple extra tokens"
        );
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens") || msg.contains("extra"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_show_permissions_extra_tokens_with_keywords() {
        let input = "SHOW PERMISSIONS FOR user123 ON order_created";
        let tokens = tokenize(input);

        let result = show_permissions::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens with keywords"
        );
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens") || msg.contains("ON"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    // ─────────────────────────────
    // Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_show_permissions_user_id_with_spaces_in_string() {
        let input = r#"SHOW PERMISSIONS FOR "user 123""#;
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with spaces in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user 123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_empty_string() {
        let input = r#"SHOW PERMISSIONS FOR """#;
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with empty string user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_single_char() {
        let input = "SHOW PERMISSIONS FOR a";
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with single char user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "a".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_user_id_unicode() {
        let input = r#"SHOW PERMISSIONS FOR "user_ñ_123""#;
        let tokens = tokenize(input);

        let command = show_permissions::parse(&tokens)
            .expect("Failed to parse SHOW PERMISSIONS with unicode in user_id");

        assert_eq!(
            command,
            Command::ShowPermissions {
                user_id: "user_ñ_123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_show_permissions_case_insensitive_all_keywords() {
        let inputs = vec![
            "show permissions for user123",
            "SHOW permissions FOR user123",
            "SHOW PERMISSIONS for user123",
            "Show Permissions For user123",
        ];

        for input in inputs {
            let tokens = tokenize(input);
            let command =
                show_permissions::parse(&tokens).expect(&format!("Failed to parse: {}", input));

            assert_eq!(
                command,
                Command::ShowPermissions {
                    user_id: "user123".to_string(),
                }
            );
        }
    }
}

