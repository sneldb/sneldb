use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("REVOKE") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("REVOKE".into())),
    }

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("KEY") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "KEY".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("KEY".into())),
    }

    let user_id = match iter.next() {
        Some(Word(word)) => word.clone(),
        Some(StringLiteral(word)) => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("user_id".into())),
    };

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after REVOKE KEY command".to_string(),
        ));
    }

    Ok(Command::RevokeKey { user_id })
}
