use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;
use serde_json::Value;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("STORE") => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("STORE".into())),
    };

    let event_type = match iter.next() {
        Some(Word(word)) => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("event_type".into())),
    };

    match iter.next() {
        Some(Word(for_kw)) if for_kw.eq_ignore_ascii_case("FOR") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "FOR".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("FOR context_id".into())),
    }

    let context_id = match iter.next() {
        Some(Word(ctx)) => ctx.clone(),
        Some(StringLiteral(ctx)) => ctx.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("context_id".into())),
    };

    match iter.next() {
        Some(Word(payload_kw)) if payload_kw.eq_ignore_ascii_case("PAYLOAD") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "PAYLOAD".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("PAYLOAD {json}".into())),
    }

    let json_value = parse_json_block(&mut iter)?;

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after STORE command".to_string(),
        ));
    }

    Ok(Command::Store {
        event_type,
        context_id,
        payload: json_value,
    })
}

/// Parses a JSON block `{ ... }` from a token stream.
///
/// - Only builds the raw JSON string.
/// - Does **not** do any schema validation.
/// - Returns `ParseError::InvalidJson` if the JSON is invalid or braces don't match.
pub fn parse_json_block<'a, I>(tokens: &mut std::iter::Peekable<I>) -> Result<Value, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    let mut json_string = String::new();
    let mut brace_level = 0;
    let mut started = false;

    while let Some(tok) = tokens.next() {
        match tok {
            Token::LeftBrace => {
                if started {
                    return Err(ParseError::NestedJsonNotAllowed);
                }
                brace_level += 1;
                started = true;
                json_string.push('{');
            }
            Token::RightBrace => {
                brace_level -= 1;
                json_string.push('}');
                if brace_level == 0 {
                    break;
                }
            }
            Token::Word(word) => {
                json_string.push_str(word);
            }
            Token::StringLiteral(word) => {
                json_string.push('"');
                json_string.push_str(word);
                json_string.push('"');
            }
            Token::Symbol(c) => {
                json_string.push(*c);
            }
            Token::Number(n) => {
                json_string.push_str(&n.to_string());
            }
            _ => {
                return Err(ParseError::UnexpectedToken(format!(
                    "Unexpected token inside JSON: {:?}",
                    tok
                )));
            }
        }
    }

    if !started {
        return Err(ParseError::ExpectedJsonBlock);
    }

    serde_json::from_str(&json_string).map_err(|_| ParseError::InvalidJson(json_string))
}
