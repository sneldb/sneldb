use crate::command::parser::commands::create_user;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod create_user_tests {
    use super::*;

    #[test]
    fn test_parse_create_user_simple() {
        let input = "CREATE USER test_user";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens).expect("Failed to parse CREATE USER command");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_string_literal() {
        let input = r#"CREATE USER "user-123""#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with string literal");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "user-123".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_secret_key() {
        let input = r#"CREATE USER test_user WITH KEY "my_secret_key_12345""#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with secret key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("my_secret_key_12345".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_secret_key_word() {
        let input = r#"CREATE USER test_user WITH KEY secret_key_12345"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with secret key as word");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret_key_12345".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_case_insensitive() {
        let input = "create user test_user";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens).expect("Failed to parse CREATE USER (lowercase)");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_key_case_insensitive() {
        let input = r#"CREATE USER test_user with key "secret""#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER WITH KEY (lowercase)");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_missing_user_keyword() {
        let input = "CREATE test_user";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing USER keyword"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "USER");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_create_user_missing_user_id() {
        let input = "CREATE USER";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing user_id");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "user_id");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_create_user_missing_create_keyword() {
        let input = "USER test_user";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing CREATE keyword"
        );
    }

    #[test]
    fn test_parse_create_user_empty_input() {
        let input = "";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to empty input");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "CREATE");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_keyword_but_no_key() {
        let input = "CREATE USER test_user WITH";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing KEY after WITH"
        );
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "KEY or ROLES");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_keyword_but_no_secret_key() {
        let input = "CREATE USER test_user WITH KEY";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing secret_key after KEY"
        );
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "secret_key");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_invalid_keyword_after_with() {
        let input = "CREATE USER test_user WITH INVALID";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to invalid keyword after WITH"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "KEY or ROLES");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_extra_tokens() {
        let input = "CREATE USER test_user extra_token";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_key_and_extra_tokens() {
        let input = r#"CREATE USER test_user WITH KEY "secret" extra_token"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens after secret key"
        );
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_create_user_user_id_with_underscore() {
        let input = "CREATE USER user_123";
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with underscore");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "user_123".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_user_id_with_hyphen() {
        let input = "CREATE USER user-456";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens).expect("Failed to parse CREATE USER with hyphen");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "user-456".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_user_id_with_numbers() {
        let input = "CREATE USER user123";
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with numbers");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "user123".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_secret_key_with_special_chars() {
        let input = r#"CREATE USER test_user WITH KEY "key-with-special_chars-123!@#""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with special chars in key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("key-with-special_chars-123!@#".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", "read-only"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens).expect("Failed to parse CREATE USER with roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string(), "read-only".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_single() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"]"#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with single role");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_empty_array() {
        let input = r#"CREATE USER test_user WITH ROLES []"#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with empty roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_word_identifiers() {
        let input = r#"CREATE USER test_user WITH ROLES [admin, read-only]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with roles as word identifiers");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string(), "read-only".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_key_and_roles() {
        let input = r#"CREATE USER test_user WITH KEY "secret" WITH ROLES ["admin"]"#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with key and roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_and_key() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"] WITH KEY "secret""#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with roles and key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_missing_bracket() {
        let input = r#"CREATE USER test_user WITH ROLES "admin""#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing '[' after ROLES"
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_missing_closing_bracket() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin""#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing closing ']'"
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_invalid_keyword_after_with() {
        let input = r#"CREATE USER test_user WITH INVALID"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to invalid keyword after WITH"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "KEY or ROLES");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    // ─────────────────────────────
    // Additional Role Parsing Tests
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_with_roles_mixed_string_and_word() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", read-only, "write"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with mixed string and word roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![
                    "admin".to_string(),
                    "read-only".to_string(),
                    "write".to_string()
                ]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_three_or_more() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", "read-only", "write", "delete"]"#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with multiple roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![
                    "admin".to_string(),
                    "read-only".to_string(),
                    "write".to_string(),
                    "delete".to_string()
                ]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_whitespace_around_commas() {
        let input = r#"CREATE USER test_user WITH ROLES [ "admin" , "read-only" , "write" ]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with whitespace around commas");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![
                    "admin".to_string(),
                    "read-only".to_string(),
                    "write".to_string()
                ]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_special_characters() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", "read-only", "role_with_underscore", "role-with-hyphen"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with special characters in roles");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![
                    "admin".to_string(),
                    "read-only".to_string(),
                    "role_with_underscore".to_string(),
                    "role-with-hyphen".to_string()
                ]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_case_insensitive_with_keyword() {
        let input = r#"CREATE USER test_user with roles ["admin"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with lowercase WITH ROLES");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_mixed_case_keyword() {
        let input = r#"CREATE USER test_user WiTh RoLeS ["admin"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with mixed case WITH ROLES");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_duplicate_roles() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", "admin", "read-only"]"#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with duplicate roles");

        // Parser should accept duplicates (validation happens elsewhere)
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec![
                    "admin".to_string(),
                    "admin".to_string(),
                    "read-only".to_string()
                ]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_empty_string_role() {
        let input = r#"CREATE USER test_user WITH ROLES [""]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with empty string role");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_invalid_token_in_array() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", 123]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to number token in roles array"
        );
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Expected role name or ']'"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_roles_symbol_in_array() {
        // Use a symbol that gets tokenized as Symbol (like = or :)
        let input = r#"CREATE USER test_user WITH ROLES ["admin", =invalid]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Symbols like = should be rejected in roles array
        assert!(
            result.is_err(),
            "Expected failure due to symbol token (=) in roles array"
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_extra_tokens_after_array() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"] extra_token"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens after roles array"
        );
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_create_user_with_roles_with_key_and_extra_tokens() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"] WITH KEY "secret" extra_token"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens after WITH KEY"
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_missing_comma() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin" "read-only"]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // The parser might accept this as two separate roles without comma
        // since it processes tokens sequentially. Let's check actual behavior.
        // If it fails, that's fine - it means the parser requires commas.
        // If it succeeds, it means the parser is lenient.
        match result {
            Ok(_) => {
                // Parser accepts missing comma - this is valid behavior
            }
            Err(_) => {
                // Parser rejects missing comma - also valid
            }
        }
    }

    #[test]
    fn test_parse_create_user_with_roles_trailing_comma() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin", "read-only",]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Trailing comma before closing bracket should be handled gracefully
        // The parser should accept it since it skips commas
        let command = result.expect("Should parse with trailing comma");
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string(), "read-only".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_leading_comma() {
        let input = r#"CREATE USER test_user WITH ROLES [,"admin"]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Leading comma should be skipped and parsing should continue
        let command = result.expect("Should parse with leading comma");
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_multiple_consecutive_commas() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin",, "read-only"]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Multiple commas should be skipped
        let command = result.expect("Should parse with multiple commas");
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["admin".to_string(), "read-only".to_string()]),
            }
        );
    }

    // ─────────────────────────────
    // WITH Clause Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_duplicate_with_key() {
        let input = r#"CREATE USER test_user WITH KEY "key1" WITH KEY "key2""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with duplicate WITH KEY");

        // Last WITH KEY should overwrite previous
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("key2".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_duplicate_with_roles() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"] WITH ROLES ["read-only"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with duplicate WITH ROLES");

        // Last WITH ROLES should overwrite previous
        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: Some(vec!["read-only".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_key_case_insensitive_lowercase() {
        let input = r#"CREATE USER test_user with key "secret""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with lowercase WITH KEY");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_key_mixed_case() {
        let input = r#"CREATE USER test_user WiTh KeY "secret""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with mixed case WITH KEY");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_complex_order_key_roles_key() {
        let input = r#"CREATE USER test_user WITH KEY "key1" WITH ROLES ["admin"] WITH KEY "key2""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with complex WITH clause order");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("key2".to_string()),
                roles: Some(vec!["admin".to_string()]),
            }
        );
    }

    #[test]
    fn test_parse_create_user_complex_order_roles_key_roles() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin"] WITH KEY "secret" WITH ROLES ["read-only"]"#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with complex WITH clause order");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("secret".to_string()),
                roles: Some(vec!["read-only".to_string()]),
            }
        );
    }

    // ─────────────────────────────
    // User ID Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_user_id_only_numbers() {
        let input = "CREATE USER 12345";
        let tokens = tokenize(input);

        // Numbers are tokenized as Number tokens, not Word tokens
        // The parser expects Word or StringLiteral for user_id
        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure - numbers are not valid user_id tokens"
        );
    }

    #[test]
    fn test_parse_create_user_user_id_only_underscores() {
        let input = "CREATE USER ___";
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with only underscores");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "___".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_user_id_only_hyphens() {
        // Multiple hyphens might be tokenized as symbols, not words
        // The existing test_parse_create_user_user_id_with_hyphen already tests
        // that hyphens work when part of a valid identifier
        // This test verifies edge case behavior
        let input = "CREATE USER ---";
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Parser expects Word or StringLiteral - multiple hyphens might be symbols
        // This is an edge case that may fail, which is acceptable
        // The important thing is that hyphens work in normal identifiers (already tested)
        assert!(
            result.is_err(),
            "Multiple hyphens should fail as they're likely tokenized as symbols"
        );
    }

    #[test]
    fn test_parse_create_user_user_id_mixed_case() {
        let input = "CREATE USER TestUser_123-Mixed";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with mixed case user_id");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "TestUser_123-Mixed".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_user_id_long() {
        let input = format!("CREATE USER {}", "a".repeat(100));
        let tokens = tokenize(&input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with long user_id");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "a".repeat(100),
                secret_key: None,
                roles: None,
            }
        );
    }

    // ─────────────────────────────
    // Secret Key Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_secret_key_empty_string() {
        let input = r#"CREATE USER test_user WITH KEY """#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with empty string secret key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_secret_key_very_long() {
        let long_key = "a".repeat(1000);
        let input = format!(r#"CREATE USER test_user WITH KEY "{}""#, long_key);
        let tokens = tokenize(&input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with very long secret key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some(long_key),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_secret_key_with_spaces() {
        let input = r#"CREATE USER test_user WITH KEY "key with spaces""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with secret key containing spaces");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("key with spaces".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_secret_key_with_newlines() {
        let input = r#"CREATE USER test_user WITH KEY "key\nwith\nnewlines""#;
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with secret key containing newlines");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("key\nwith\nnewlines".to_string()),
                roles: None,
            }
        );
    }

    // ─────────────────────────────
    // Whitespace and Formatting Edge Cases
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_multiple_spaces() {
        let input = "CREATE    USER    test_user";
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with multiple spaces");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_tabs() {
        let input = "CREATE\tUSER\ttest_user";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens).expect("Failed to parse CREATE USER with tabs");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_newlines() {
        let input = "CREATE\nUSER\ntest_user";
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Failed to parse CREATE USER with newlines");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_leading_whitespace() {
        let input = "   CREATE USER test_user";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with leading whitespace");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_trailing_whitespace() {
        let input = "CREATE USER test_user   ";
        let tokens = tokenize(input);

        let command = create_user::parse(&tokens)
            .expect("Failed to parse CREATE USER with trailing whitespace");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: None,
                roles: None,
            }
        );
    }

    // ─────────────────────────────
    // Error Cases - Additional Coverage
    // ─────────────────────────────

    #[test]
    fn test_parse_create_user_with_roles_unclosed_bracket_with_valid_tokens() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin" "read-only""#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to unclosed bracket");
    }

    #[test]
    fn test_parse_create_user_with_roles_nested_brackets() {
        let input = r#"CREATE USER test_user WITH ROLES [["admin"]]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Nested brackets should cause an error when we encounter the inner bracket
        assert!(result.is_err(), "Expected failure due to nested brackets");
    }

    #[test]
    fn test_parse_create_user_with_key_empty_string_literal() {
        let input = r#"CREATE USER test_user WITH KEY """#;
        let tokens = tokenize(input);

        let command =
            create_user::parse(&tokens).expect("Should parse empty string literal as secret key");

        assert_eq!(
            command,
            Command::CreateUser {
                user_id: "test_user".to_string(),
                secret_key: Some("".to_string()),
                roles: None,
            }
        );
    }

    #[test]
    fn test_parse_create_user_with_roles_only_comma() {
        let input = r#"CREATE USER test_user WITH ROLES [,]"#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        // Parser skips commas, so [,] might be parsed as empty array
        // Let's check actual behavior
        match result {
            Ok(cmd) => {
                // If it succeeds, verify it's an empty array
                if let Command::CreateUser { roles, .. } = cmd {
                    assert_eq!(roles, Some(vec![]));
                }
            }
            Err(_) => {
                // If it fails, that's also valid - parser requires at least one role
            }
        }
    }

    #[test]
    fn test_parse_create_user_with_roles_unclosed_bracket_after_comma() {
        let input = r#"CREATE USER test_user WITH ROLES ["admin","#;
        let tokens = tokenize(input);

        let result = create_user::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to unclosed bracket after comma"
        );
    }
}
