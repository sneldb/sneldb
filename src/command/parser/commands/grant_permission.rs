use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    // GRANT
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("GRANT") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("GRANT".into())),
    }

    // Parse permissions: READ, WRITE or READ,WRITE
    let mut permissions = Vec::new();
    loop {
        match iter.peek() {
            Some(Word(word)) if word.eq_ignore_ascii_case("READ") => {
                iter.next();
                permissions.push("read".to_string());
            }
            Some(Word(word)) if word.eq_ignore_ascii_case("WRITE") => {
                iter.next();
                permissions.push("write".to_string());
            }
            Some(Word(word)) => {
                // Invalid permission name
                return Err(ParseError::UnexpectedToken(format!(
                    "Invalid permission: '{}'. Must be 'READ' or 'WRITE'",
                    word
                )));
            }
            _ => break,
        }

        // Check for comma
        if let Some(Symbol(',')) = iter.peek() {
            iter.next();
            continue;
        }
        break;
    }

    if permissions.is_empty() {
        return Err(ParseError::MissingArgument(
            "At least one permission (READ or WRITE) required".into(),
        ));
    }

    // ON
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("ON") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "ON".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("ON".into())),
    }

    // Parse event types: event_type1, event_type2, ...
    let mut event_types = Vec::new();
    loop {
        let event_type = match iter.next() {
            Some(Word(word)) => word.clone(),
            Some(StringLiteral(word)) => word.clone(),
            Some(tok) => {
                return Err(ParseError::UnexpectedToken(format!(
                    "Expected event_type, found {:?}",
                    tok
                )));
            }
            None => break,
        };
        event_types.push(event_type);

        // Check for comma
        if let Some(Symbol(',')) = iter.peek() {
            iter.next();
            continue;
        }
        break;
    }

    if event_types.is_empty() {
        return Err(ParseError::MissingArgument(
            "At least one event_type required".into(),
        ));
    }

    // TO
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("TO") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "TO".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("TO".into())),
    }

    // user_id
    let user_id = match iter.next() {
        Some(Word(word)) => word.clone(),
        Some(StringLiteral(word)) => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("user_id".into())),
    };

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after GRANT command".to_string(),
        ));
    }

    Ok(Command::GrantPermission {
        permissions,
        event_types,
        user_id,
    })
}
