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
        sequence_time_field: Option<String>,
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
    RememberQuery {
        spec: MaterializedQuerySpec,
    },
    ShowMaterialized {
        name: String,
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
    Compare {
        queries: Vec<QueryCommand>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaterializedQuerySpec {
    pub name: String,
    pub query: Box<Command>,
}

/// Represents a single query command, used both in Command::Query and Command::Compare
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryCommand {
    pub event_type: String,
    pub context_id: Option<String>,
    pub since: Option<String>,
    pub time_field: Option<String>,
    pub sequence_time_field: Option<String>,
    pub where_clause: Option<Expr>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order_by: Option<OrderSpec>,
    pub picked_zones: Option<PickedZones>,
    pub return_fields: Option<Vec<String>>,
    pub link_field: Option<String>,
    pub aggs: Option<Vec<AggSpec>>,
    pub time_bucket: Option<TimeGranularity>,
    pub group_by: Option<Vec<String>>,
    pub event_sequence: Option<EventSequence>,
}

impl From<&Command> for QueryCommand {
    fn from(cmd: &Command) -> Self {
        match cmd {
            Command::Query {
                event_type,
                context_id,
                since,
                time_field,
                sequence_time_field,
                where_clause,
                limit,
                offset,
                order_by,
                picked_zones,
                return_fields,
                link_field,
                aggs,
                time_bucket,
                group_by,
                event_sequence,
            } => QueryCommand {
                event_type: event_type.clone(),
                context_id: context_id.clone(),
                since: since.clone(),
                time_field: time_field.clone(),
                sequence_time_field: sequence_time_field.clone(),
                where_clause: where_clause.clone(),
                limit: *limit,
                offset: *offset,
                order_by: order_by.clone(),
                picked_zones: picked_zones.clone(),
                return_fields: return_fields.clone(),
                link_field: link_field.clone(),
                aggs: aggs.clone(),
                time_bucket: time_bucket.clone(),
                group_by: group_by.clone(),
                event_sequence: event_sequence.clone(),
            },
            _ => panic!("Command is not a Query"),
        }
    }
}

impl From<QueryCommand> for Command {
    fn from(qc: QueryCommand) -> Self {
        Command::Query {
            event_type: qc.event_type,
            context_id: qc.context_id,
            since: qc.since,
            time_field: qc.time_field,
            sequence_time_field: qc.sequence_time_field,
            where_clause: qc.where_clause,
            limit: qc.limit,
            offset: qc.offset,
            order_by: qc.order_by,
            picked_zones: qc.picked_zones,
            return_fields: qc.return_fields,
            link_field: qc.link_field,
            aggs: qc.aggs,
            time_bucket: qc.time_bucket,
            group_by: qc.group_by,
            event_sequence: qc.event_sequence,
        }
    }
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
                sequence_time_field: None,
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

    pub fn is_comparison_query(&self) -> bool {
        matches!(self, Command::Compare { .. })
    }

    pub fn to_query_commands(&self) -> Option<Vec<QueryCommand>> {
        match self {
            Command::Compare { queries } => Some(queries.clone()),
            Command::Query { .. } => Some(vec![QueryCommand::from(self)]),
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
    In {
        field: String,
        values: Vec<Value>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
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
    Year,
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
