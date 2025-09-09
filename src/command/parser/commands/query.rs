use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::{Command, CompareOp, Expr};
use serde_json::Value;
use std::iter::Peekable;

pub fn parse(tokens: &[Token]) -> Result<Command, ParseError> {
    use Token::*;
    let mut iter = tokens.iter().peekable();

    match iter.next() {
        Some(Word(word)) if word.eq_ignore_ascii_case("QUERY") => word.clone(),
        Some(tok) => return Err(ParseError::UnexpectedToken(format!("{:?}", tok))),
        None => return Err(ParseError::MissingArgument("QUERY".into())),
    };

    // 1. event_type
    let event_type = match iter.next() {
        Some(Word(word)) => word.clone(),
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected event_type, found {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected event_type".to_string(),
            ));
        }
    };

    // 2. optional FOR context_id
    let mut context_id = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("FOR") {
            iter.next();
            match iter.next() {
                Some(Word(id)) | Some(StringLiteral(id)) => context_id = Some(id.clone()),
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected context_id after FOR, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected context_id after FOR".to_string(),
                    ));
                }
            }
        }
    }

    // 3. optional SINCE "timestamp"
    let mut since = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("SINCE") {
            iter.next();
            match iter.next() {
                Some(StringLiteral(ts)) => since = Some(ts.clone()),
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected timestamp after SINCE, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected timestamp after SINCE".to_string(),
                    ));
                }
            }
        }
    }

    // 4. optional WHERE clause
    let mut where_clause = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("WHERE") {
            iter.next();
            where_clause = Some(parse_where_expr(&mut iter)?);
        }
    }

    // 5. optional LIMIT
    let mut limit = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("LIMIT") {
            iter.next();
            match iter.next() {
                Some(Token::Number(n)) => limit = Some(*n as u32),
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected number after LIMIT, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected number after LIMIT".to_string(),
                    ));
                }
            }
        }
    }

    if iter.peek().is_some() {
        return Err(ParseError::UnexpectedToken(format!(
            "Unexpected trailing token: {:?}",
            iter.peek().unwrap()
        )));
    }

    Ok(Command::Query {
        event_type,
        context_id,
        since,
        where_clause,
        limit,
    })
}

fn parse_where_expr<'a, I>(iter: &mut Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    parse_or_expr(iter)
}

fn parse_or_expr<'a, I>(iter: &mut Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    let mut node = parse_and_expr(iter)?;

    while let Some(Token::Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("OR") {
            iter.next();
            let right = parse_and_expr(iter)?;
            node = Expr::Or(Box::new(node), Box::new(right));
        } else {
            break;
        }
    }

    Ok(node)
}

fn parse_and_expr<'a, I>(iter: &mut Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    let mut node = parse_factor(iter)?;

    while let Some(Token::Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("AND") {
            iter.next();
            let right = parse_factor(iter)?;
            node = Expr::And(Box::new(node), Box::new(right));
        } else {
            break;
        }
    }

    Ok(node)
}

fn parse_factor<'a, I>(iter: &mut Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    if let Some(Token::Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("NOT") {
            iter.next();
            let expr = parse_factor(iter)?;
            return Ok(Expr::Not(Box::new(expr)));
        }
    }

    parse_comparison(iter)
}

fn parse_comparison<'a, I>(iter: &mut Peekable<I>) -> Result<Expr, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    use Token::*;

    let field = match iter.next() {
        Some(Word(w)) => w.clone(),
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected field name, found {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected field name".to_string(),
            ));
        }
    };

    let op = parse_compare_op(iter)?;

    let value = match iter.next() {
        Some(StringLiteral(s)) => Value::String(s.clone()),
        Some(Number(n)) => Value::Number(serde_json::Number::from_f64(*n).unwrap()),
        Some(Word(w)) => Value::String(w.clone()),
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected value, found {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected value after comparison".to_string(),
            ));
        }
    };

    Ok(Expr::Compare { field, op, value })
}

fn parse_compare_op<'a, I>(iter: &mut Peekable<I>) -> Result<CompareOp, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    use Token::*;
    match iter.next() {
        Some(Symbol('=')) => Ok(CompareOp::Eq),
        Some(Symbol('!')) => {
            if let Some(Symbol('=')) = iter.next() {
                Ok(CompareOp::Neq)
            } else {
                Err(ParseError::UnexpectedToken(
                    "Expected '=' after '!'".to_string(),
                ))
            }
        }
        Some(Symbol('>')) => {
            if let Some(Symbol('=')) = iter.peek() {
                iter.next();
                Ok(CompareOp::Gte)
            } else {
                Ok(CompareOp::Gt)
            }
        }
        Some(Symbol('<')) => {
            if let Some(Symbol('=')) = iter.peek() {
                iter.next();
                Ok(CompareOp::Lte)
            } else {
                Ok(CompareOp::Lt)
            }
        }
        Some(other) => Err(ParseError::UnexpectedToken(format!(
            "Expected comparison operator, found {:?}",
            other
        ))),
        None => Err(ParseError::MissingArgument(
            "Expected comparison operator".to_string(),
        )),
    }
}
