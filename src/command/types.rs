use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
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
        where_clause: Option<Expr>,
        limit: Option<u32>,
        return_fields: Option<Vec<String>>,
    },
    Replay {
        event_type: Option<String>,
        context_id: String,
        since: Option<String>,
        return_fields: Option<Vec<String>>,
    },
    Ping,
    Flush,
    Batch(Vec<Command>),
}

impl Command {
    pub fn to_query_command(&self) -> Option<Command> {
        if let Command::Replay {
            event_type,
            context_id,
            since,
            return_fields,
        } = self
        {
            Some(Command::Query {
                event_type: event_type.clone().unwrap_or_else(|| "*".to_string()),
                context_id: Some(context_id.clone()),
                since: since.clone(),
                where_clause: None,
                limit: None,
                return_fields: return_fields.clone(),
            })
        } else {
            None
        }
    }

    pub fn event_type(&self) -> &str {
        match self {
            Command::Query { event_type, .. } => event_type,
            Command::Replay { event_type, .. } => event_type.as_deref().unwrap_or("*"),
            _ => panic!("Command is not a query"),
        }
    }

    pub fn context_id(&self) -> Option<&str> {
        match self {
            Command::Query { context_id, .. } => context_id.as_deref(),
            Command::Replay { context_id, .. } => Some(context_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MiniSchema {
    pub fields: HashMap<String, FieldSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldSpec {
    Primitive(String),
    Enum(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}
