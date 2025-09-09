use crate::command::types::{Command, Expr, FieldSpec, MiniSchema};
use serde_json::{Value, json};

pub struct CommandFactory {
    inner: Command,
}

impl CommandFactory {
    pub fn define() -> Self {
        let schema = MiniSchema {
            fields: [("id".into(), FieldSpec::Primitive("int".into()))].into(),
        };
        Self {
            inner: Command::Define {
                event_type: "test_event".into(),
                version: Some(1),
                schema,
            },
        }
    }

    pub fn store() -> Self {
        Self {
            inner: Command::Store {
                event_type: "test_event".into(),
                context_id: "ctx1".into(),
                payload: json!({"key": "value"}),
            },
        }
    }

    pub fn query() -> Self {
        Self {
            inner: Command::Query {
                event_type: "test_event".into(),
                context_id: None,
                since: None,
                where_clause: None,
                limit: Some(10),
            },
        }
    }

    pub fn replay() -> Self {
        Self {
            inner: Command::Replay {
                event_type: Some("test_event".into()),
                context_id: "ctx1".into(),
                since: Some("2023-01-01T00:00:00Z".into()),
            },
        }
    }

    pub fn with_event_type(mut self, value: &str) -> Self {
        match &mut self.inner {
            Command::Define { event_type, .. }
            | Command::Store { event_type, .. }
            | Command::Query { event_type, .. }
            | Command::Replay {
                event_type: Some(event_type),
                ..
            } => {
                *event_type = value.to_string();
            }
            Command::Replay { event_type: et, .. } => {
                *et = Some(value.to_string());
            }
            _ => {}
        }
        self
    }

    pub fn with_context_id(mut self, value: &str) -> Self {
        match &mut self.inner {
            Command::Store { context_id, .. } | Command::Replay { context_id, .. } => {
                *context_id = value.to_string();
            }
            Command::Query { context_id, .. } => {
                *context_id = Some(value.to_string());
            }
            _ => {}
        }
        self
    }

    pub fn with_payload(mut self, value: Value) -> Self {
        if let Command::Store { payload, .. } = &mut self.inner {
            *payload = value;
        }
        self
    }

    pub fn with_where_clause(mut self, expr: Expr) -> Self {
        if let Command::Query { where_clause, .. } = &mut self.inner {
            *where_clause = Some(expr);
        }
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        if let Command::Query { limit: l, .. } = &mut self.inner {
            *l = Some(limit);
        }
        self
    }

    pub fn with_since(mut self, since: &str) -> Self {
        match &mut self.inner {
            Command::Query { since: s, .. } => {
                *s = Some(since.to_string());
            }
            Command::Replay { since: s, .. } => {
                *s = Some(since.to_string());
            }
            _ => {}
        }
        self
    }

    pub fn create(self) -> Command {
        self.inner
    }
}
