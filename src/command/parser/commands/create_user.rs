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

    // Optional: WITH KEY "secret_key"
    let secret_key = if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("WITH") {
            iter.next(); // consume WITH
            match iter.next() {
                Some(Word(kw)) if kw.eq_ignore_ascii_case("KEY") => {}
                Some(tok) => {
                    return Err(ParseError::ExpectedKeyword(
                        "KEY".into(),
                        format!("{:?}", tok),
                    ));
                }
                None => return Err(ParseError::MissingArgument("KEY".into())),
            }

            match iter.next() {
                Some(StringLiteral(key)) => Some(key.clone()),
                Some(Word(key)) => Some(key.clone()),
                Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
                None => return Err(ParseError::MissingArgument("secret_key".into())),
            }
        } else {
            None
        }
    } else {
        None
    };

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after CREATE USER command".to_string(),
        ));
    }

    Ok(Command::CreateUser {
        user_id,
        secret_key,
    })
}
