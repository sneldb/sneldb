use crate::command::types::{AggSpec, Command, Expr, FieldSpec, MiniSchema, TimeGranularity};
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
                return_fields: None,
                link_field: None,
                aggs: None,
                time_bucket: None,
                group_by: None,
                event_sequence: None,
            },
        }
    }

    pub fn replay() -> Self {
        Self {
            inner: Command::Replay {
                event_type: Some("test_event".into()),
                context_id: "ctx1".into(),
                since: Some("2023-01-01T00:00:00Z".into()),
                return_fields: None,
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

    pub fn with_return_fields(mut self, fields: Vec<&str>) -> Self {
        let values: Vec<String> = fields.into_iter().map(|s| s.to_string()).collect();
        match &mut self.inner {
            Command::Query { return_fields, .. } => {
                *return_fields = Some(values);
            }
            Command::Replay { return_fields, .. } => {
                *return_fields = Some(values);
            }
            _ => {}
        }
        self
    }

    pub fn with_link_field(mut self, field: &str) -> Self {
        if let Command::Query { link_field, .. } = &mut self.inner {
            *link_field = Some(field.to_string());
        }
        self
    }

    pub fn with_aggs(mut self, aggs: Vec<AggSpec>) -> Self {
        if let Command::Query { aggs: a, .. } = &mut self.inner {
            *a = Some(aggs);
        }
        self
    }

    pub fn add_count(mut self) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Count { unique_field: None });
        }
        self
    }

    pub fn add_count_unique(mut self, field: &str) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Count {
                unique_field: Some(field.to_string()),
            });
        }
        self
    }

    pub fn add_total(mut self, field: &str) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Total {
                field: field.to_string(),
            });
        }
        self
    }

    pub fn add_avg(mut self, field: &str) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Avg {
                field: field.to_string(),
            });
        }
        self
    }

    pub fn add_min(mut self, field: &str) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Min {
                field: field.to_string(),
            });
        }
        self
    }

    pub fn add_max(mut self, field: &str) -> Self {
        if let Command::Query { aggs, .. } = &mut self.inner {
            let list = aggs.get_or_insert_with(Vec::new);
            list.push(AggSpec::Max {
                field: field.to_string(),
            });
        }
        self
    }

    pub fn with_time_bucket(mut self, gran: TimeGranularity) -> Self {
        if let Command::Query { time_bucket, .. } = &mut self.inner {
            *time_bucket = Some(gran);
        }
        self
    }

    pub fn with_group_by(mut self, fields: Vec<&str>) -> Self {
        let values: Vec<String> = fields.into_iter().map(|s| s.to_string()).collect();
        if let Command::Query { group_by, .. } = &mut self.inner {
            *group_by = Some(values);
        }
        self
    }

    pub fn create(self) -> Command {
        self.inner
    }
}
