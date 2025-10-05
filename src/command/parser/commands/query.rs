use crate::command::parser::error::ParseError;
use crate::command::parser::tokenizer::Token;
use crate::command::types::{
    AggSpec, Command, CompareOp, EventSequence, EventTarget, Expr, SequenceLink, TimeGranularity,
};
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

    // 1. event_type or EventSeq head
    let (event_type, event_sequence): (String, Option<EventSequence>) = match iter.next() {
        Some(Word(word)) => {
            let head = parse_event_target_from_word(word.clone())?;
            // Check for sequence links (FOLLOWED BY | PRECEDED BY)
            let mut links: Vec<(SequenceLink, EventTarget)> = Vec::new();
            loop {
                match iter.peek() {
                    Some(Word(w)) if w.eq_ignore_ascii_case("FOLLOWED") => {
                        iter.next();
                        match iter.next() {
                            Some(Word(by)) if by.eq_ignore_ascii_case("BY") => {}
                            Some(other) => {
                                return Err(ParseError::UnexpectedToken(format!(
                                    "Expected BY after FOLLOWED, found {:?}",
                                    other
                                )));
                            }
                            None => {
                                return Err(ParseError::MissingArgument(
                                    "Expected BY after FOLLOWED".to_string(),
                                ));
                            }
                        }
                        let target = parse_event_target(&mut iter)?;
                        links.push((SequenceLink::FollowedBy, target));
                    }
                    Some(Word(w)) if w.eq_ignore_ascii_case("PRECEDED") => {
                        iter.next();
                        match iter.next() {
                            Some(Word(by)) if by.eq_ignore_ascii_case("BY") => {}
                            Some(other) => {
                                return Err(ParseError::UnexpectedToken(format!(
                                    "Expected BY after PRECEDED, found {:?}",
                                    other
                                )));
                            }
                            None => {
                                return Err(ParseError::MissingArgument(
                                    "Expected BY after PRECEDED".to_string(),
                                ));
                            }
                        }
                        let target = parse_event_target(&mut iter)?;
                        links.push((SequenceLink::PrecededBy, target));
                    }
                    _ => break,
                }
            }

            if links.is_empty() {
                (head.event.clone(), None)
            } else {
                (head.event.clone(), Some(EventSequence { head, links }))
            }
        }
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

    // 4. optional RETURN [fields]
    let mut return_fields: Option<Vec<String>> = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("RETURN") {
            iter.next();

            // Expect '['
            match iter.next() {
                Some(LeftSquareBracket) => {}
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected '[' after RETURN, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected '[' after RETURN".to_string(),
                    ));
                }
            }

            // Parse zero or more field names until ']'
            let mut fields: Vec<String> = Vec::new();
            loop {
                match iter.peek() {
                    Some(RightSquareBracket) => {
                        iter.next();
                        break;
                    }
                    Some(Word(_)) => {
                        let f = parse_field(&mut iter)?;
                        fields.push(f);
                        if let Some(Token::Symbol(',')) = iter.peek() {
                            iter.next();
                        }
                        continue;
                    }
                    Some(StringLiteral(s)) => {
                        let s = s.clone();
                        iter.next();
                        fields.push(s);
                        if let Some(Token::Symbol(',')) = iter.peek() {
                            iter.next();
                        }
                        continue;
                    }
                    Some(other) => {
                        return Err(ParseError::UnexpectedToken(format!(
                            "Unexpected token in RETURN list: {:?}",
                            other
                        )));
                    }
                    None => {
                        return Err(ParseError::MissingArgument(
                            "Unterminated RETURN list, expected ']'".to_string(),
                        ));
                    }
                }
            }

            // Empty list => Some(vec![]) meaning "all payload" is handled downstream
            return_fields = Some(fields);
        }
    }

    // New optional clauses for extended FIND/QUERY grammar
    // LINKED BY <ident>
    let mut link_field: Option<String> = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("LINKED") {
            iter.next();
            match iter.next() {
                Some(Word(by)) if by.eq_ignore_ascii_case("BY") => {}
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected BY after LINKED, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected BY after LINKED".to_string(),
                    ));
                }
            }
            match iter.next() {
                Some(Word(id)) => link_field = Some(id.clone()),
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected identifier after LINKED BY, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected identifier after LINKED BY".to_string(),
                    ));
                }
            }
        }
    }

    // 5. optional WHERE clause
    let mut where_clause = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("WHERE") {
            iter.next();
            where_clause = Some(parse_where_expr(&mut iter)?);
        }
    }

    // 6. optional AggClause
    let mut aggs: Option<Vec<AggSpec>> = None;
    if let Some(Word(word)) = iter.peek() {
        if is_agg_start(word) {
            aggs = Some(parse_aggs(&mut iter)?);
        }
    }

    // 7. optional TimeClause: PER day|week|hour|month
    let mut time_bucket: Option<TimeGranularity> = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("PER") {
            iter.next();
            match iter.next() {
                Some(Word(u)) if u.eq_ignore_ascii_case("hour") => {
                    time_bucket = Some(TimeGranularity::Hour)
                }
                Some(Word(u)) if u.eq_ignore_ascii_case("day") => {
                    time_bucket = Some(TimeGranularity::Day)
                }
                Some(Word(u)) if u.eq_ignore_ascii_case("week") => {
                    time_bucket = Some(TimeGranularity::Week)
                }
                Some(Word(u)) if u.eq_ignore_ascii_case("month") => {
                    time_bucket = Some(TimeGranularity::Month)
                }
                Some(other) => {
                    return Err(ParseError::UnexpectedToken(format!(
                        "Expected time unit after PER, found {:?}",
                        other
                    )));
                }
                None => {
                    return Err(ParseError::MissingArgument(
                        "Expected time unit after PER".to_string(),
                    ));
                }
            }
        }
    }

    // 8. optional GroupClause: BY Field { , Field }
    let mut group_by: Option<Vec<String>> = None;
    if let Some(Word(word)) = iter.peek() {
        if word.eq_ignore_ascii_case("BY") {
            iter.next();
            let mut fields: Vec<String> = Vec::new();
            loop {
                match iter.peek() {
                    Some(Word(_)) => {
                        let field = parse_field(&mut iter)?;
                        fields.push(field);
                        if let Some(Token::Symbol(',')) = iter.peek() {
                            iter.next();
                            continue;
                        }
                        break;
                    }
                    Some(other) => {
                        return Err(ParseError::UnexpectedToken(format!(
                            "Unexpected token in GROUP BY list: {:?}",
                            other
                        )));
                    }
                    None => break,
                }
            }
            if fields.is_empty() {
                return Err(ParseError::MissingArgument(
                    "Expected field after BY".to_string(),
                ));
            }
            group_by = Some(fields);
        }
    }

    // 9. optional LIMIT (kept for compatibility at the end)
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
        return_fields,
        link_field,
        aggs,
        time_bucket,
        group_by,
        event_sequence,
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

    let field = parse_field(iter)?;

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

fn is_agg_start(word: &str) -> bool {
    word.eq_ignore_ascii_case("COUNT")
        || word.eq_ignore_ascii_case("TOTAL")
        || word.eq_ignore_ascii_case("AVG")
        || word.eq_ignore_ascii_case("MIN")
        || word.eq_ignore_ascii_case("MAX")
}

fn parse_aggs<'a, I>(iter: &mut Peekable<I>) -> Result<Vec<AggSpec>, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    use Token::*;
    let mut result: Vec<AggSpec> = Vec::new();
    loop {
        match iter.next() {
            Some(Word(w)) if w.eq_ignore_ascii_case("COUNT") => {
                // COUNT [ UNIQUE field ] | COUNT <field>
                // First check UNIQUE
                if let Some(Word(u)) = iter.peek() {
                    if u.eq_ignore_ascii_case("UNIQUE") {
                        iter.next();
                        let f = expect_field(iter, "UNIQUE")?;
                        result.push(AggSpec::Count {
                            unique_field: Some(f),
                        });

                        // Optional comma will be handled by loop footer
                        // Continue to next agg or break below
                    } else {
                        // If the next word starts a new clause, treat as COUNT ALL
                        let clause = u.eq_ignore_ascii_case("PER")
                            || u.eq_ignore_ascii_case("BY")
                            || u.eq_ignore_ascii_case("LIMIT")
                            || u.eq_ignore_ascii_case("WHERE")
                            || u.eq_ignore_ascii_case("LINKED")
                            || u.eq_ignore_ascii_case("RETURN")
                            || u.eq_ignore_ascii_case("SINCE")
                            || u.eq_ignore_ascii_case("FOR")
                            || u.eq_ignore_ascii_case("FOLLOWED")
                            || u.eq_ignore_ascii_case("PRECEDED");
                        if clause {
                            result.push(AggSpec::Count { unique_field: None });
                        } else {
                            // COUNT <field>
                            let f = expect_field(iter, "COUNT")?;
                            result.push(AggSpec::CountField { field: f });
                        }
                    }
                } else {
                    // Bare COUNT
                    result.push(AggSpec::Count { unique_field: None });
                }
            }
            Some(Word(w)) if w.eq_ignore_ascii_case("TOTAL") => {
                let field = expect_field(iter, "TOTAL")?;
                result.push(AggSpec::Total { field });
            }
            Some(Word(w)) if w.eq_ignore_ascii_case("AVG") => {
                let field = expect_field(iter, "AVG")?;
                result.push(AggSpec::Avg { field });
            }
            Some(Word(w)) if w.eq_ignore_ascii_case("MIN") => {
                let field = expect_field(iter, "MIN")?;
                result.push(AggSpec::Min { field });
            }
            Some(Word(w)) if w.eq_ignore_ascii_case("MAX") => {
                let field = expect_field(iter, "MAX")?;
                result.push(AggSpec::Max { field });
            }
            Some(other) => {
                return Err(ParseError::UnexpectedToken(format!(
                    "Expected aggregate function, found {:?}",
                    other
                )));
            }
            None => break,
        }

        // Optional comma to chain more aggs
        if let Some(Symbol(',')) = iter.peek() {
            iter.next();
            continue;
        } else {
            break;
        }
    }
    Ok(result)
}

fn expect_field<'a, I>(iter: &mut Peekable<I>, ctx: &str) -> Result<String, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    parse_field(iter).map_err(|e| match e {
        ParseError::MissingArgument(_) => {
            ParseError::MissingArgument(format!("Expected field after {}", ctx))
        }
        ParseError::UnexpectedToken(_) => {
            ParseError::UnexpectedToken(format!("Expected field after {}", ctx))
        }
        other => other,
    })
}

fn parse_event_target_from_word(w: String) -> Result<EventTarget, ParseError> {
    // Tokenizer already allows dotted identifiers in a single Word
    let parts: Vec<&str> = w.split('.').collect();
    if parts.len() == 1 {
        Ok(EventTarget {
            event: w,
            field: None,
        })
    } else if parts.len() == 2 {
        // Per new EBNF: EventType must be unqualified in EventSeq.
        // Reject dotted form here.
        Err(ParseError::UnexpectedToken(
            "EventType in sequence must not be qualified".to_string(),
        ))
    } else {
        Err(ParseError::UnexpectedToken(
            "Too many dots in event target".to_string(),
        ))
    }
}

fn parse_event_target<'a, I>(iter: &mut Peekable<I>) -> Result<EventTarget, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    match iter.next() {
        Some(Token::Word(w)) => parse_event_target_from_word(w.clone()),
        Some(other) => Err(ParseError::UnexpectedToken(format!(
            "Expected event identifier, found {:?}",
            other
        ))),
        None => Err(ParseError::MissingArgument(
            "Expected event after sequence link".to_string(),
        )),
    }
}

// Field = [ EventType "." ] Ident
fn parse_field<'a, I>(iter: &mut Peekable<I>) -> Result<String, ParseError>
where
    I: Iterator<Item = &'a Token>,
{
    use Token::*;
    // First must be Word
    let first = match iter.next() {
        Some(Word(w)) => w.clone(),
        Some(other) => {
            return Err(ParseError::UnexpectedToken(format!(
                "Expected field identifier, found {:?}",
                other
            )));
        }
        None => {
            return Err(ParseError::MissingArgument(
                "Expected field identifier".to_string(),
            ));
        }
    };

    // If next is a dot and then a Word, consume and build qualified field
    if let Some(Symbol('.')) = iter.peek() {
        iter.next();
        match iter.next() {
            Some(Word(w2)) => Ok(format!("{}.{}", first, w2)),
            Some(other) => Err(ParseError::UnexpectedToken(format!(
                "Expected identifier after '.', found {:?}",
                other
            ))),
            None => Err(ParseError::MissingArgument(
                "Expected identifier after '.'".to_string(),
            )),
        }
    } else {
        Ok(first)
    }
}
