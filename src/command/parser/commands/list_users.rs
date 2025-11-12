use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;

    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("LIST") => {}
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("LIST".into())),
    }

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("USERS") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "USERS".into(),
                format!("{:?}", tok),
            ));
        }
        None => return Err(ParseError::MissingArgument("USERS".into())),
    }

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(
            "Extra tokens after LIST USERS command".to_string(),
        ));
    }

    Ok(Command::ListUsers)
}
