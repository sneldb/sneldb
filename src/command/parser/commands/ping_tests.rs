use crate::command::parser::commands::ping;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod ping_tests {
    use super::*;

    #[test]
    fn test_parse_ping_simple() {
        let input = "PING";
        let tokens = tokenize(input);

        let command = ping::parse(&tokens).expect("Failed to parse PING command");

        assert_eq!(command, Command::Ping);
    }

    #[test]
    fn test_parse_ping_with_trailing_tokens_should_fail() {
        let input = "PING junk";
        let tokens = tokenize(input);

        let result = ping::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens after PING"
        );
    }
}
