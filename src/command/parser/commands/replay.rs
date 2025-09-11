use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;
    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("REPLAY") => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("REPLAY".into())),
    };

    let mut event_type = None;

    // First token: either "FOR" or <event_type>
    match iter.peek() {
        Some(Word(word)) if word.eq_ignore_ascii_case("FOR") => {
            // No event type
            iter.next(); // consume FOR
        }
        Some(Word(word)) => {
            // Event type present
            event_type = Some(word.clone());
            iter.next(); // consume event type

            match iter.next() {
                Some(Word(for_kw)) if for_kw.eq_ignore_ascii_case("FOR") => {
                    // OK, expect context_id next
                }
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected FOR after event type, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected FOR after event type".to_string(),
                    ));
                }
            }
        }
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Unexpected token at start of REPLAY: {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected FOR or event_type after REPLAY".to_string(),
            ));
        }
    }

    // Now: expect context_id
    let context_id = match iter.next() {
        Some(Word(id)) => id.clone(),
        Some(StringLiteral(id)) => id.clone(),
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected context_id after FOR, found {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected context_id after FOR".to_string(),
            ));
        }
    };

    // Optional: SINCE "timestamp"
    let mut since = None;
    if let Some(Word(since_kw)) = iter.peek() {
        if since_kw.eq_ignore_ascii_case("SINCE") {
            iter.next(); // consume SINCE

            match iter.next() {
                Some(StringLiteral(ts)) => {
                    since = Some(ts.clone());
                }
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected timestamp string after SINCE, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected timestamp after SINCE".to_string(),
                    ));
                }
            }
        }
    }

    // Optional: RETURN [fields] — parse and ignore
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("RETURN") {
            iter.next();

            // Expect '['
            match iter.next() {
                Some(LeftSquareBracket) => {}
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected '[' after RETURN, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected '[' after RETURN".to_string(),
                    ));
                }
            }

            loop {
                match iter.peek() {
                    Some(RightSquareBracket) => {
                        iter.next();
                        break;
                    }
                    Some(Word(_)) | Some(StringLiteral(_)) => {
                        iter.next();
                    }
                    Some(Symbol(',')) => {
                        iter.next();
                        continue;
                    }
                    Some(other) => {
                        return Err(ParseError::UnexpectedToken(format!(
                            "Unexpected token in RETURN list: {:?}",
                            other
                        )));
                    }
                    None => {
                        return Err(ParseError::MissingArgument(
                            "Unterminated RETURN list, expected ']'".to_string(),
                        ));
                    }
                }
            }
        }
    }

    // Check there are no extra tokens
    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(format!(
            "Unexpected trailing token: {:?}",
            iter.peek().unwrap()
        )));
    }

    Ok(Command::Replay {
        event_type,
        context_id,
        since,
        return_fields: None,
    })
}
