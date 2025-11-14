use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    // REVOKE
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("REVOKE") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("REVOKE".into())),
    }

    // Note: REVOKE KEY is handled by revoke_key parser, so we won't see it here

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

    // Permissions are optional for REVOKE (can revoke all permissions for event types)
    // But we'll require at least one for clarity

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

    // FROM
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("FROM") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "FROM".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("FROM".into())),
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
            "Extra tokens after REVOKE command".to_string(),
        ));
    }

    Ok(Command::RevokePermission {
        permissions,
        event_types,
        user_id,
    })
}
