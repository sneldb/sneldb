use crate::command::types::{CompareOp, Expr};
use serde_json::{Value, json};

pub struct ExprFactory {
    field: String,
    op: CompareOp,
    value: Value,
}

impl ExprFactory {
    pub fn new() -> Self {
        Self {
            field: "foo".into(),
            op: CompareOp::Eq,
            value: json!("bar"),
        }
    }

    pub fn with_field(mut self, field: &str) -> Self {
        self.field = field.into();
        self
    }

    pub fn with_op(mut self, op: CompareOp) -> Self {
        self.op = op;
        self
    }

    pub fn with_value(mut self, value: Value) -> Self {
        self.value = value;
        self
    }

    pub fn create(self) -> Expr {
        Expr::Compare {
            field: self.field,
            op: self.op,
            value: self.value,
        }
    }

    pub fn and(lhs: Expr, rhs: Expr) -> Expr {
        Expr::And(Box::new(lhs), Box::new(rhs))
    }

    pub fn or(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Or(Box::new(lhs), Box::new(rhs))
    }

    pub fn not(inner: Expr) -> Expr {
        Expr::Not(Box::new(inner))
    }
}
