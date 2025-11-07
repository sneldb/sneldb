use serde::Deserialize;
use serde_json::Value;

use crate::command::types::{Command, CompareOp, Expr, MiniSchema};

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum JsonCommand {
    Define {
        event_type: String,
        version: Option<u32>,
        schema: MiniSchema,
    },
    Store {
        event_type: String,
        context_id: String,
        payload: Value,
    },
    Query {
        event_type: String,
        context_id: Option<String>,
        since: Option<String>,
        time_field: Option<String>,
        #[serde(default)]
        #[serde(alias = "where")]
        where_clause: Option<JsonExpr>,
        limit: Option<u32>,
        offset: Option<u32>,
        order_by: Option<crate::command::types::OrderSpec>,
    },
    Replay {
        event_type: Option<String>,
        context_id: String,
        since: Option<String>,
        time_field: Option<String>,
    },
    Ping,
    Flush,
    Batch(Vec<JsonCommand>),
}

impl From<JsonCommand> for Command {
    fn from(j: JsonCommand) -> Self {
        match j {
            JsonCommand::Define {
                event_type,
                version,
                schema,
            } => Command::Define {
                event_type,
                version,
                schema,
            },
            JsonCommand::Store {
                event_type,
                context_id,
                payload,
            } => Command::Store {
                event_type,
                context_id,
                payload,
            },
            JsonCommand::Query {
                event_type,
                context_id,
                since,
                time_field,
                where_clause,
                limit,
                offset,
                order_by,
            } => Command::Query {
                event_type,
                context_id,
                since,
                time_field,
                sequence_time_field: None,
                where_clause: where_clause.map(Into::into),
                limit,
                offset,
                order_by,
                picked_zones: None,
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            },
            JsonCommand::Replay {
                event_type,
                context_id,
                since,
                time_field,
            } => Command::Replay {
                event_type,
                context_id,
                since,
                time_field,
                return_fields: None,
            },
            JsonCommand::Ping => Command::Ping,
            JsonCommand::Flush => Command::Flush,
            JsonCommand::Batch(cmds) => Command::Batch(cmds.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum JsonExpr {
    Compare(JsonCompare),
    Logical(JsonLogical),
}

#[derive(Deserialize)]
pub struct JsonCompare {
    pub field: String,
    pub op: String,
    pub value: Value,
}

#[derive(Deserialize)]
pub struct JsonLogical {
    #[serde(default)]
    pub and: Vec<JsonExpr>,
    #[serde(default)]
    pub or: Vec<JsonExpr>,
    #[serde(default)]
    pub not: Option<Box<JsonExpr>>,
}

impl From<JsonExpr> for Expr {
    fn from(j: JsonExpr) -> Self {
        match j {
            JsonExpr::Compare(JsonCompare { field, op, value }) => {
                let compare_op = match op.as_str() {
                    "eq" | "==" | "=" => CompareOp::Eq,
                    "neq" | "!=" | "<>" => CompareOp::Neq,
                    "gt" | ">" => CompareOp::Gt,
                    "gte" | ">=" => CompareOp::Gte,
                    "lt" | "<" => CompareOp::Lt,
                    "lte" | "<=" => CompareOp::Lte,
                    _ => {
                        eprintln!("Unknown comparison op: {}", op);
                        CompareOp::Eq
                    }
                };
                Expr::Compare {
                    field,
                    op: compare_op,
                    value,
                }
            }
            JsonExpr::Logical(JsonLogical { and, or, not }) => {
                if !and.is_empty() {
                    and.into_iter()
                        .map(Into::into)
                        .reduce(|a, b| Expr::And(Box::new(a), Box::new(b)))
                        .unwrap()
                } else if !or.is_empty() {
                    or.into_iter()
                        .map(Into::into)
                        .reduce(|a, b| Expr::Or(Box::new(a), Box::new(b)))
                        .unwrap()
                } else if let Some(inner) = not {
                    Expr::Not(Box::new((*inner).into()))
                } else {
                    // default fallback: always-false
                    Expr::Compare {
                        field: "".into(),
                        op: CompareOp::Eq,
                        value: Value::Bool(false),
                    }
                }
            }
        }
    }
}
