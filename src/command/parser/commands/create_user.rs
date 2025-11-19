use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("CREATE") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("CREATE".into())),
    }

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("USER") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "USER".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("USER".into())),
    }

    let user_id = match iter.next() {
        Some(Word(word)) => word.clone(),
        Some(StringLiteral(word)) => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("user_id".into())),
    };

    // Optional: WITH KEY "secret_key" and/or WITH ROLES ["admin", "read-only"]
    let mut secret_key = None;
    let mut roles = None;

    while let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("WITH") {
            iter.next(); // consume WITH
            match iter.next() {
                Some(Word(kw)) if kw.eq_ignore_ascii_case("KEY") => match iter.next() {
                    Some(StringLiteral(key)) => secret_key = Some(key.clone()),
                    Some(Word(key)) => secret_key = Some(key.clone()),
                    Some(tok) => {
                        return Err(ParseError::UnexpectedToken(format!("{:?}", tok)));
                    }
                    None => return Err(ParseError::MissingArgument("secret_key".into())),
                },
                Some(Word(kw)) if kw.eq_ignore_ascii_case("ROLES") => {
                    // Parse array: ["admin", "read-only"]
                    match iter.next() {
                        Some(LeftSquareBracket) => {
                            let mut role_list = Vec::new();
                            loop {
                                match iter.next() {
                                    Some(StringLiteral(role)) => {
                                        role_list.push(role.clone());
                                    }
                                    Some(Word(role)) => {
                                        role_list.push(role.clone());
                                    }
                                    Some(RightSquareBracket) => {
                                        break;
                                    }
                                    Some(Symbol(',')) => {
                                        // Skip comma, continue to next role
                                        continue;
                                    }
                                    Some(tok) => {
                                        return Err(ParseError::UnexpectedToken(format!(
                                            "Expected role name or ']', found {:?}",
                                            tok
                                        )));
                                    }
                                    None => {
                                        return Err(ParseError::MissingArgument(
                                            "Missing closing ']' for ROLES array".into(),
                                        ));
                                    }
                                }
                            }
                            roles = Some(role_list);
                        }
                        Some(tok) => {
                            return Err(ParseError::ExpectedKeyword(
                                "Expected '[' after ROLES".into(),
                                format!("{:?}", tok),
                            ));
                        }
                        None => {
                            return Err(ParseError::MissingArgument(
                                "Expected '[' after ROLES".into(),
                            ));
                        }
                    }
                }
                Some(tok) => {
                    return Err(ParseError::ExpectedKeyword(
                        "KEY or ROLES".into(),
                        format!("{:?}", tok),
                    ));
                }
                None => {
                    return Err(ParseError::MissingArgument("KEY or ROLES".into()));
                }
            }
        } else {
            break;
        }
    }

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after CREATE USER command".to_string(),
        ));
    }

    Ok(Command::CreateUser {
        user_id,
        secret_key,
        roles,
    })
}
