use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use std::collections::HashMap;
use std::fmt::Debug;

/// Represents a condition that can be evaluated against zone values
pub trait Condition: Send + Sync + Debug {
    fn evaluate(&self, values: &HashMap<String, Vec<String>>) -> bool;

    /// Fast path evaluation against a zone by index without constructing
    /// per-event HashMaps. Implementors should override this for performance.
    fn evaluate_at(&self, _accessor: &dyn FieldAccessor, _index: usize) -> bool {
        // Default fallback (slow path): materialize a single-value map
        // for all available fields using the accessor. Since the accessor
        // does not expose iteration over fields, we return false by default.
        // All current implementors override this method.
        false
    }
}

/// Provides indexed access to field values for a candidate zone.
pub trait FieldAccessor {
    fn get_str_at(&self, field: &str, index: usize) -> Option<&str>;
    fn get_i64_at(&self, field: &str, index: usize) -> Option<i64>;
    fn event_count(&self) -> usize;
}

/// A concrete accessor over a zone's columnar values that lazily builds
/// per-column numeric caches to avoid repeated string parsing.
pub struct PreparedAccessor<'a> {
    columns: &'a HashMap<String, Vec<String>>,
    event_count: usize,
}

impl<'a> PreparedAccessor<'a> {
    pub fn new(columns: &'a HashMap<String, Vec<String>>) -> Self {
        let event_count = columns.values().next().map(|v| v.len()).unwrap_or(0);
        Self {
            columns,
            event_count,
        }
    }
}

impl<'a> FieldAccessor for PreparedAccessor<'a> {
    fn get_str_at(&self, field: &str, index: usize) -> Option<&str> {
        self.columns
            .get(field)
            .and_then(|col| col.get(index))
            .map(|s| s.as_str())
    }

    fn get_i64_at(&self, field: &str, index: usize) -> Option<i64> {
        self.columns
            .get(field)
            .and_then(|col| col.get(index))
            .and_then(|s| s.parse::<i64>().ok())
    }

    fn event_count(&self) -> usize {
        self.event_count
    }
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

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        if let Some(num) = accessor.get_i64_at(&self.field, index) {
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

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        if let Some(val) = accessor.get_str_at(&self.field, index) {
            match self.operation {
                CompareOp::Eq => val == self.value,
                CompareOp::Neq => val != self.value,
                _ => false,
            }
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

    fn evaluate_at(&self, accessor: &dyn FieldAccessor, index: usize) -> bool {
        match self.operation {
            LogicalOp::And => self
                .conditions
                .iter()
                .all(|c| c.evaluate_at(accessor, index)),
            LogicalOp::Or => self
                .conditions
                .iter()
                .any(|c| c.evaluate_at(accessor, index)),
            LogicalOp::Not => !self.conditions[0].evaluate_at(accessor, index),
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
