use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use std::collections::HashMap;
use std::fmt::Debug;

/// Represents a condition that can be evaluated against zone values
pub trait Condition: Send + Sync + Debug {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool;
}

/// Numeric comparison condition
#[derive(Debug)]
pub struct NumericCondition {
    field: String,
    operation: CompareOp,
    value: i64,
}

impl NumericCondition {
    pub fn new(field: String, operation: CompareOp, value: i64) -> Self {
        Self {
            field,
            operation,
            value,
        }
    }
}

impl Condition for NumericCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            // For now, we'll check if any value in the zone matches the condition
            field_values.iter().any(|v| {
                if let Ok(num) = v.parse::<i64>() {
                    match self.operation {
                        CompareOp::Gt => num > self.value,
                        CompareOp::Gte => num >= self.value,
                        CompareOp::Lt => num < self.value,
                        CompareOp::Lte => num <= self.value,
                        CompareOp::Eq => num == self.value,
                        CompareOp::Neq => num != self.value,
                    }
                } else {
                    false
                }
            })
        } else {
            false
        }
    }
}

/// String comparison condition
#[derive(Debug)]
pub struct StringCondition {
    field: String,
    operation: CompareOp,
    value: String,
}

impl StringCondition {
    pub fn new(field: String, operation: CompareOp, value: String) -> Self {
        Self {
            field,
            operation,
            value,
        }
    }
}

impl Condition for StringCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        if let Some(field_values) = values.get(&self.field) {
            field_values.iter().any(|v| match self.operation {
                CompareOp::Eq => v == &self.value,
                CompareOp::Neq => v != &self.value,
                _ => false, // Other operations don't make sense for strings
            })
        } else {
            false
        }
    }
}

/// Logical combination of conditions
#[derive(Debug)]
pub struct LogicalCondition {
    conditions: Vec<Box<dyn Condition>>,
    operation: LogicalOp,
}

impl LogicalCondition {
    pub fn new(conditions: Vec<Box<dyn Condition>>, operation: LogicalOp) -> Self {
        Self {
            conditions,
            operation,
        }
    }
}

impl Condition for LogicalCondition {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool {
        match self.operation {
            LogicalOp::And => self.conditions.iter().all(|c| c.evaluate(values)),
            LogicalOp::Or => self.conditions.iter().any(|c| c.evaluate(values)),
            LogicalOp::Not => !self.conditions[0].evaluate(values),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Neq,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
    Not,
}

impl LogicalOp {
    /// Determines the logical operation from a query expression
    pub fn from_expr(expr: Option<&Expr>) -> Self {
        match expr {
            Some(Expr::And(_, _)) => LogicalOp::And,
            Some(Expr::Or(_, _)) => LogicalOp::Or,
            Some(Expr::Not(_)) => LogicalOp::Not,
            Some(Expr::Compare { .. }) => LogicalOp::And, // Single comparison is treated as AND
            None => LogicalOp::And,                       // Default to AND if no where clause
        }
    }

    /// Determines if this operation requires special handling
    pub fn requires_special_handling(&self) -> bool {
        matches!(self, LogicalOp::Not)
    }
}

impl From<CommandCompareOp> for CompareOp {
    fn from(op: CommandCompareOp) -> Self {
        match op {
            CommandCompareOp::Gt => CompareOp::Gt,
            CommandCompareOp::Gte => CompareOp::Gte,
            CommandCompareOp::Lt => CompareOp::Lt,
            CommandCompareOp::Lte => CompareOp::Lte,
            CommandCompareOp::Eq => CompareOp::Eq,
            CommandCompareOp::Neq => CompareOp::Neq,
        }
    }
}
