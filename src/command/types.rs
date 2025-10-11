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
        time_field: Option<String>,
        where_clause: Option<Expr>,
        limit: Option<u32>,
        offset: Option<u32>,
        order_by: Option<OrderSpec>,
        picked_zones: Option<PickedZones>,
        return_fields: Option<Vec<String>>,
        link_field: Option<String>,
        aggs: Option<Vec<AggSpec>>,
        time_bucket: Option<TimeGranularity>,
        group_by: Option<Vec<String>>,
        event_sequence: Option<EventSequence>,
    },
    Replay {
        event_type: Option<String>,
        context_id: String,
        since: Option<String>,
        time_field: Option<String>,
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
            time_field,
            return_fields,
        } = self
        {
            Some(Command::Query {
                event_type: event_type.clone().unwrap_or_else(|| "*".to_string()),
                context_id: Some(context_id.clone()),
                since: since.clone(),
                time_field: time_field.clone(),
                where_clause: None,
                limit: None,
                offset: None,
                order_by: None,
                picked_zones: None,
                return_fields: return_fields.clone(),
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggSpec {
    Count { unique_field: Option<String> },
    CountField { field: String },
    Total { field: String },
    Avg { field: String },
    Min { field: String },
    Max { field: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderSpec {
    pub field: String,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PickedZones {
    pub uid: String,
    pub field: String,
    pub asc: bool,
    pub cutoff: String,
    pub k: usize,
    pub zones: Vec<(String, u32)>, // (segment_id, zone_id)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeGranularity {
    Hour,
    Day,
    Week,
    Month,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SequenceLink {
    FollowedBy,
    PrecededBy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventTarget {
    pub event: String,
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventSequence {
    pub head: EventTarget,
    pub links: Vec<(SequenceLink, EventTarget)>,
}
