use crate::command::parser::commands::query;
use crate::command::parser::error::ParseError;
use crate::command::types::{Command, MaterializedQuerySpec};

pub fn parse(input: &str) -> Result<Command, ParseError> {
    let trimmed = input.trim();

    let remainder = strip_prefix_ci(trimmed, "REMEMBER")
        .ok_or_else(|| ParseError::UnexpectedToken("REMEMBER".to_string()))?
        .trim_start();

    if remainder.is_empty() {
        return Err(ParseError::MissingArgument(
            "QUERY specification before AS".to_string(),
        ));
    }

    let upper = remainder.to_ascii_uppercase();
    let as_idx = upper
        .rfind(" AS ")
        .ok_or_else(|| ParseError::MissingArgument("AS <name> clause".to_string()))?;

    let query_part = remainder[..as_idx].trim();
    if query_part.is_empty() {
        return Err(ParseError::MissingArgument("Query expression".to_string()));
    }

    if !query_part.to_ascii_uppercase().starts_with("QUERY") {
        return Err(ParseError::ExpectedKeyword(
            "QUERY".to_string(),
            query_part
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_string(),
        ));
    }

    let alias_part = remainder[as_idx + 4..].trim();
    if alias_part.is_empty() {
        return Err(ParseError::MissingArgument(
            "Materialization name".to_string(),
        ));
    }

    if !is_valid_alias(alias_part) {
        return Err(ParseError::UnexpectedToken(alias_part.to_string()));
    }

    let query_command = query::parse(query_part)?;

    if let Command::Query { .. } = query_command {
        let spec = MaterializedQuerySpec {
            name: alias_part.to_string(),
            query: Box::new(query_command),
        };
        Ok(Command::RememberQuery { spec })
    } else {
        Err(ParseError::UnexpectedToken(
            "REMEMBER expects a QUERY command".to_string(),
        ))
    }
}

fn strip_prefix_ci<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.len() < prefix.len() {
        return None;
    }
    let (head, tail) = input.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail)
    } else {
        None
    }
}

pub(super) fn is_valid_alias(alias: &str) -> bool {
    alias
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
