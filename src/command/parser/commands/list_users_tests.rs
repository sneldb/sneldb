use crate::command::parser::commands::list_users;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod list_users_tests {
    use super::*;

    #[test]
    fn test_parse_list_users_simple() {
        let input = "LIST USERS";
        let tokens = tokenize(input);

        let command = list_users::parse(&tokens).expect("Failed to parse LIST USERS command");

        assert_eq!(command, Command::ListUsers);
    }

    #[test]
    fn test_parse_list_users_case_insensitive() {
        let input = "list users";
        let tokens = tokenize(input);

        let command = list_users::parse(&tokens).expect("Failed to parse LIST USERS (lowercase)");

        assert_eq!(command, Command::ListUsers);
    }

    #[test]
    fn test_parse_list_users_mixed_case() {
        let input = "List Users";
        let tokens = tokenize(input);

        let command = list_users::parse(&tokens).expect("Failed to parse LIST USERS (mixed case)");

        assert_eq!(command, Command::ListUsers);
    }

    #[test]
    fn test_parse_list_users_missing_users_keyword() {
        let input = "LIST";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing USERS keyword"
        );
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "USERS");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_list_users_missing_list_keyword() {
        let input = "USERS";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to missing LIST keyword"
        );
    }

    #[test]
    fn test_parse_list_users_empty_input() {
        let input = "";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to empty input");
        match result {
            Err(ParseError::MissingArgument(arg)) => {
                assert_eq!(arg, "LIST");
            }
            _ => panic!("Expected MissingArgument error"),
        }
    }

    #[test]
    fn test_parse_list_users_with_extra_tokens() {
        let input = "LIST USERS extra_token";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
        match result {
            Err(ParseError::UnexpectedToken(msg)) => {
                assert!(msg.contains("Extra tokens"));
            }
            _ => panic!("Expected UnexpectedToken error"),
        }
    }

    #[test]
    fn test_parse_list_users_with_multiple_extra_tokens() {
        let input = "LIST USERS token1 token2 token3";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(result.is_err(), "Expected failure due to extra tokens");
    }

    #[test]
    fn test_parse_list_users_wrong_keyword_after_list() {
        let input = "LIST INVALID";
        let tokens = tokenize(input);

        let result = list_users::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to wrong keyword after LIST"
        );
        match result {
            Err(ParseError::ExpectedKeyword(expected, _)) => {
                assert_eq!(expected, "USERS");
            }
            _ => panic!("Expected ExpectedKeyword error"),
        }
    }

    #[test]
    fn test_parse_list_users_uppercase() {
        let input = "LIST USERS";
        let tokens = tokenize(input);

        let command = list_users::parse(&tokens).expect("Failed to parse LIST USERS (uppercase)");

        assert_eq!(command, Command::ListUsers);
    }
}
