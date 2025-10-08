use crate::command::parser::commands;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::{Token, tokenize};
use crate::command::types::Command;
use tracing::{debug, warn};

pub fn parse_command(input: &str) -> Result<Command, ParseError> {
    let input = input.trim();
    debug!(target: "sneldb::parse", raw = input, "Parsing input command");

    let tokens = tokenize(input);

    // Validate before parsing
    if let Err(err) = validate_tokens(&tokens) {
        warn!(target: "sneldb::parse", ?err, "Token validation failed");
        return Err(err);
    }

    match tokens.first() {
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("DEFINE") => {
            commands::define::parse(&tokens)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("STORE") => {
            commands::store::parse(&tokens)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("QUERY") => {
            commands::query::parse(input)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("FIND") => commands::query::parse(input),
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("REPLAY") => {
            commands::replay::parse(&tokens)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("BATCH") => {
            commands::batch::parse(&tokens)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("PING") => {
            commands::ping::parse(&tokens)
        }
        Some(Token::Word(cmd)) if cmd.eq_ignore_ascii_case("FLUSH") => {
            commands::flush::parse(&tokens)
        }
        _ => {
            warn!(target: "sneldb::parse", input, "Unknown command keyword");
            Err(ParseError::UnknownCommand(input.into()))
        }
    }
}

/// Validates that there are no invalid tokens (e.g., `<INVALID>`) after tokenization.
fn validate_tokens(tokens: &[Token]) -> Result<(), ParseError> {
    for token in tokens {
        if let Token::Word(word) = token {
            if word == "<INVALID>" {
                return Err(ParseError::UnexpectedToken(
                    "Found invalid character during tokenization.".to_string(),
                ));
            }
        }
    }
    Ok(())
}
