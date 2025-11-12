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
                assert_eq!(arg, "KEY");
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
                assert_eq!(expected, "KEY");
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
            }
        );
    }
}
