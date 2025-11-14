use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    // SHOW
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("SHOW") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("SHOW".into())),
    }

    // PERMISSIONS
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("PERMISSIONS") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "PERMISSIONS".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("PERMISSIONS".into())),
    }

    // FOR
    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("FOR") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "FOR".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("FOR".into())),
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
            "Extra tokens after SHOW PERMISSIONS command".to_string(),
        ));
    }

    Ok(Command::ShowPermissions { user_id })
}
