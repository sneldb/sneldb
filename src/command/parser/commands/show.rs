use crate::command::parser::commands::remember::is_valid_alias;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    if tokens.len() < 2 {
        return Err(ParseError::MissingArgument(
            "Materialization name".to_string(),
        ));
    }

    if tokens.len() > 2 {
        return Err(ParseError::UnexpectedToken(
            "SHOW expects exactly one argument".to_string(),
        ));
    }

    let alias = match &tokens[1] {
        Token::Word(word) => word.clone(),
        Token::StringLiteral(value) => value.clone(),
        other => {
            return Err(ParseError::UnexpectedToken(format!(
                "Invalid SHOW argument: {:?}",
                other
            )));
        }
    };

    if alias.is_empty() {
        return Err(ParseError::MissingArgument(
            "Materialization name".to_string(),
        ));
    }

    if !is_valid_alias(&alias) {
        return Err(ParseError::UnexpectedToken(alias));
    }

    Ok(Command::ShowMaterialized { name: alias })
}
