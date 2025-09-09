use crate::command::parser::commands::flush;
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;

#[cfg(test)]
mod flush_tests {
    use super::*;

    #[test]
    fn test_parse_flush_simple() {
        let input = "FLUSH";
        let tokens = tokenize(input);

        let command = flush::parse(&tokens).expect("Failed to parse FLUSH command");

        assert_eq!(command, Command::Flush);
    }

    #[test]
    fn test_parse_flush_with_trailing_tokens_should_fail() {
        let input = "FLUSH garbage";
        let tokens = tokenize(input);

        let result = flush::parse(&tokens);

        assert!(
            result.is_err(),
            "Expected failure due to extra tokens after FLUSH"
        );
    }
}
