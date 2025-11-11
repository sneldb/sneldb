use crate::command::parser::commands::revoke_key;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod revoke_key_tests {
    use super::*;

    #[test]
    fn test_parse_revoke_key_simple() {
        let input = "REVOKE KEY test_user";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY command");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "test_user".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_with_string_literal() {
        let input = r#"REVOKE KEY "user-123""#;
        let tokens = tokenize(input);

        let command =
            revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY with string literal");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "user-123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_case_insensitive() {
        let input = "revoke key test_user";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY (lowercase)");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "test_user".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_mixed_case() {
        let input = "Revoke Key test_user";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY (mixed case)");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "test_user".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_missing_key_keyword() {
        let input = "REVOKE test_user";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing KEY keyword"
        );
        match result {
            Err(crate::command::parser::error::ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "KEY");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_revoke_key_missing_user_id() {
        let input = "REVOKE KEY";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to missing user_id");
        match result {
            Err(crate::command::parser::error::ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "user_id");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_revoke_key_missing_revoke_keyword() {
        let input = "KEY test_user";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing REVOKE keyword"
        );
    }

    #[test]
    fn test_parse_revoke_key_empty_input() {
        let input = "";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to empty input");
        match result {
            Err(crate::command::parser::error::ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "REVOKE");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_revoke_key_with_extra_tokens() {
        let input = "REVOKE KEY test_user extra_token";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
        match result {
            Err(crate::command::parser::error::ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_revoke_key_with_multiple_extra_tokens() {
        let input = "REVOKE KEY test_user token1 token2";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
    }

    #[test]
    fn test_parse_revoke_key_wrong_keyword_after_revoke() {
        let input = "REVOKE INVALID test_user";
        let tokens = tokenize(input);

        let result = revoke_key::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to wrong keyword after REVOKE"
        );
        match result {
            Err(crate::command::parser::error::ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "KEY");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_revoke_key_user_id_with_underscore() {
        let input = "REVOKE KEY user_123";
        let tokens = tokenize(input);

        let command =
            revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY with underscore");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "user_123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_user_id_with_hyphen() {
        let input = "REVOKE KEY user-456";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY with hyphen");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "user-456".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_user_id_with_numbers() {
        let input = "REVOKE KEY user123";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY with numbers");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "user123".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_user_id_with_string_literal_special_chars() {
        let input = r#"REVOKE KEY "user@domain.com""#;
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens)
            .expect("Failed to parse REVOKE KEY with special chars in string literal");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "user@domain.com".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_revoke_key_uppercase() {
        let input = "REVOKE KEY TEST_USER";
        let tokens = tokenize(input);

        let command = revoke_key::parse(&tokens).expect("Failed to parse REVOKE KEY (uppercase)");

        assert_eq!(
            command,
            Command::RevokeKey {
                user_id: "TEST_USER".to_string(),
            }
        );
    }
}
