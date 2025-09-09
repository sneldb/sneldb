use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Token::Word(word)) if word.eq_ignore_ascii_case("FLUSH") => {
            if iter.peek().is_some() {
                return Err(ParseError::UnexpectedToken(
                    "Extra tokens after FLUSH command".to_string(),
                ));
            }
            Ok(Command::Flush)
        }
        Some(tok) => Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => Err(ParseError::MissingArgument("FLUSH".to_string())),
    }
}
