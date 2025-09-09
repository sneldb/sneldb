use crate::engine::core::filter::condition::CompareOp;
use crate::engine::core::{
    Condition, LogicalCondition, LogicalOp, NumericCondition, StringCondition,
};
use std::collections::HashMap;

pub struct ConditionFactory {
    kind: String,
    params: HashMap<String, String>,
    subconditions: Vec<Box<dyn Condition>>,
}

impl ConditionFactory {
    pub fn new_numeric() -> Self {
        let mut params = HashMap::new();
        params.insert("field".into(), "score".into());
        params.insert("op".into(), "Gte".into());
        params.insert("value".into(), "10".into());
        Self {
            kind: "numeric".into(),
            params,
            subconditions: vec![],
        }
    }

    pub fn new_string() -> Self {
        let mut params = HashMap::new();
        params.insert("field".into(), "status".into());
        params.insert("op".into(), "Eq".into());
        params.insert("value".into(), "active".into());
        Self {
            kind: "string".into(),
            params,
            subconditions: vec![],
        }
    }

    pub fn new_logical(op: LogicalOp) -> Self {
        let mut params = HashMap::new();
        params.insert("logical_op".into(), format!("{:?}", op));
        Self {
            kind: "logical".into(),
            params,
            subconditions: vec![],
        }
    }

    pub fn with(mut self, key: &str, value: impl ToString) -> Self {
        self.params.insert(key.into(), value.to_string());
        self
    }

    pub fn with_condition(mut self, cond: Box<dyn Condition>) -> Self {
        self.subconditions.push(cond);
        self
    }

    pub fn with_conditions(mut self, conds: Vec<Box<dyn Condition>>) -> Self {
        self.subconditions.extend(conds);
        self
    }

    pub fn create(self) -> Box<dyn Condition> {
        match self.kind.as_str() {
            "numeric" => {
                let op = parse_op(&self.params["op"]);
                let field = self.params["field"].clone();
                let value = self.params["value"].parse::<i64>().unwrap();
                Box::new(NumericCondition::new(field, op, value))
            }
            "string" => {
                let op = parse_op(&self.params["op"]);
                let field = self.params["field"].clone();
                let value = self.params["value"].clone();
                Box::new(StringCondition::new(field, op, value))
            }
            "logical" => {
                let op = parse_logical_op(&self.params["logical_op"]);
                Box::new(LogicalCondition::new(self.subconditions, op))
            }
            _ => panic!("Unknown condition kind"),
        }
    }
}

fn parse_op(s: &str) -> CompareOp {
    match s {
        "Eq" => CompareOp::Eq,
        "Neq" => CompareOp::Neq,
        "Gt" => CompareOp::Gt,
        "Gte" => CompareOp::Gte,
        "Lt" => CompareOp::Lt,
        "Lte" => CompareOp::Lte,
        _ => panic!("Unknown CompareOp: {}", s),
    }
}

fn parse_logical_op(s: &str) -> LogicalOp {
    match s {
        "And" => LogicalOp::And,
        "Or" => LogicalOp::Or,
        "Not" => LogicalOp::Not,
        _ => panic!("Unknown LogicalOp: {}", s),
    }
}
