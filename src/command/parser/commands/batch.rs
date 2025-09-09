use crate::command::parser::command::parse_command;
use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::Command;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;
    let mut tokens = tokens.iter().peekable();

    match tokens.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("BATCH") => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("BATCH".into())),
    };

    match tokens.next() {
        Some(Token::LeftSquareBracket) => {}
        _ => {
            return Err(ParseError::UnexpectedToken(
                "Expected '[' to start BATCH block".to_string(),
            ));
        }
    }

    let mut buffer = String::new();
    let mut brace_depth = 0;
    let mut found_right_bracket = false;

    while let Some(token) = tokens.next() {
        match token {
            Token::LeftBrace => {
                brace_depth += 1;
                buffer.push('{');
            }
            Token::RightBrace => {
                if brace_depth == 0 {
                    return Err(ParseError::UnexpectedToken("Unexpected '}'".to_string()));
                }
                brace_depth -= 1;
                buffer.push('}');
            }
            Token::RightSquareBracket => {
                if brace_depth != 0 {
                    return Err(ParseError::UnexpectedToken(
                        "Unbalanced { } inside BATCH".to_string(),
                    ));
                }
                found_right_bracket = true;
                break;
            }
            Token::Word(w) => {
                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push_str(w);
            }
            Token::StringLiteral(s) => {
                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push('"');
                buffer.push_str(s);
                buffer.push('"');
            }
            Token::Number(n) => {
                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push_str(&n.to_string());
            }
            Token::Symbol(c) => {
                buffer.push(*c);
            }
            Token::Semicolon => {
                buffer.push(';');
            }
            _ => {}
        }
    }

    if !found_right_bracket {
        return Err(ParseError::UnexpectedToken(
            "Missing closing ']' for BATCH".to_string(),
        ));
    }

    // Now split safely
    let parts = buffer.split(';').map(str::trim).filter(|p| !p.is_empty());

    let mut commands = Vec::new();
    for part in parts {
        let cmd = parse_command(part)?;
        commands.push(cmd);
    }

    if commands.is_empty() {
        return Err(ParseError::MissingArgument(
            "BATCH must contain at least one command".to_string(),
        ));
    }

    Ok(Command::Batch(commands))
}
