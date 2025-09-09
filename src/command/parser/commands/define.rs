use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::{Command, FieldSpec, MiniSchema};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

use Token::*;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    let mut iter = tokens.iter().peekable();

    let _define_word = match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("DEFINE") => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("DEFINE".into())),
    };

    let event_type = match iter.next() {
        Some(Word(word)) => {
            validate_event_type(word)?;
            word.clone()
        }
        Some(tok) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected event_type, found {:?}",
                tok
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected event_type after DEFINE".into(),
            ));
        }
    };

    // Optional: AS <version>
    let mut version = None;
    if let Some(Word(as_kw)) = iter.peek() {
        if as_kw.eq_ignore_ascii_case("AS") {
            iter.next(); // consume AS

            match iter.next() {
                Some(Number(n)) if *n >= 0.0 => {
                    version = Some(*n as u32);
                }
                Some(Word(num_str)) => {
                    version = Some(num_str.parse::<u32>().map_err(|_| {
                        ParseError::UnexpectedToken(format!(
                            "Invalid version number: {:?}",
                            num_str
                        ))
                    })?);
                }
                Some(tok) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected version number after AS, found {:?}",
                        tok
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected version number after AS".into(),
                    ));
                }
            }
        }
    }

    match iter.next() {
        Some(Word(fields_kw)) if fields_kw.eq_ignore_ascii_case("FIELDS") => {}
        Some(tok) => {
            return Err(ParseError::ExpectedKeyword(
                "FIELDS".into(),
                format!("{:?}", tok),
            ));
        }
        None => {
            return Err(ParseError::MissingArgument("FIELDS { ... }".into()));
        }
    }

    let fields = parse_fields_block(&mut iter)?;

    if fields.is_empty() {
        return Err(ParseError::EmptySchema);
    }

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(format!(
            "Unexpected token after FIELDS block: {:?}",
            iter.peek().unwrap()
        )));
    }

    Ok(Command::Define {
        event_type,
        version,
        schema: MiniSchema { fields },
    })
}

fn parse_fields_block<'a, I>(
    tokens: &mut std::iter::Peekable<I>,
) -> Result<HashMap<String, FieldSpec>, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    use Token::*;

    let mut json_string = String::new();
    let mut brace_level = 0;
    let _started = false;

    match tokens.next() {
        Some(LeftBrace) => {
            brace_level += 1;
            json_string.push('{');
        }
        _ => return Err(ParseError::ExpectedJsonBlock),
    }

    while let Some(tok) = tokens.next() {
        match tok {
            LeftBrace => {
                return Err(ParseError::NestedJsonNotAllowed);
            }
            LeftSquareBracket => {
                json_string.push('[');
            }
            RightSquareBracket => {
                json_string.push(']');
            }
            RightBrace => {
                brace_level -= 1;
                json_string.push('}');
                if brace_level == 0 {
                    break;
                }
            }
            Word(word) => {
                json_string.push('"');
                json_string.push_str(word);
                json_string.push('"');
            }
            StringLiteral(word) => {
                json_string.push('"');
                json_string.push_str(word);
                json_string.push('"');
            }
            Symbol(c) => {
                json_string.push(*c);
            }
            Number(n) => {
                json_string.push_str(&n.to_string());
            }
            _ => {}
        }
    }

    if brace_level != 0 {
        return Err(ParseError::ExpectedJsonBlock);
    }

    let parsed: Value = serde_json::from_str(&json_string)
        .map_err(|_| ParseError::InvalidJson(json_string.clone()))?;

    match parsed {
        Value::Object(map) => {
            let mut fields = HashMap::new();
            for (key, val) in map {
                match val {
                    Value::String(s) => {
                        fields.insert(key, FieldSpec::Primitive(s));
                    }
                    Value::Array(arr) => {
                        let mut variants = Vec::with_capacity(arr.len());
                        for v in arr {
                            if let Value::String(s) = v {
                                variants.push(s);
                            } else {
                                return Err(ParseError::InvalidJson(
                                    "Enum variants must be strings".to_string(),
                                ));
                            }
                        }
                        if variants.is_empty() {
                            return Err(ParseError::InvalidJson(
                                "Enum must have at least one variant".to_string(),
                            ));
                        }
                        fields.insert(key, FieldSpec::Enum(variants));
                    }
                    _ => {
                        return Err(ParseError::InvalidJson(
                            "Field type must be a string or array".to_string(),
                        ));
                    }
                }
            }
            Ok(fields)
        }
        _ => Err(ParseError::InvalidJson(
            "FIELDS must be a JSON object".to_string(),
        )),
    }
}

fn validate_event_type(name: &str) -> Result<(), ParseError> {
    static EVENT_TYPE_REGEX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9_]{0,99}$").unwrap());

    if EVENT_TYPE_REGEX.is_match(name) {
        Ok(())
    } else {
        Err(ParseError::InvalidEventType(name.to_string()))
    }
}
