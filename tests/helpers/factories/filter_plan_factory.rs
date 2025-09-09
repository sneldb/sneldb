use crate::command::types::CompareOp;
use crate::engine::core::FilterPlan;
use serde_json::Value;

/// Factory for creating `FilterPlan` objects in tests
pub struct FilterPlanFactory {
    column: String,
    operation: Option<CompareOp>,
    value: Option<Value>,
    priority: u32,
    uid: Option<String>,
}

impl FilterPlanFactory {
    pub fn new() -> Self {
        Self {
            column: "default_field".to_string(),
            operation: None,
            value: None,
            priority: 3,
            uid: None,
        }
    }

    pub fn with_column(mut self, column: &str) -> Self {
        self.column = column.to_string();
        self
    }

    pub fn with_operation(mut self, op: CompareOp) -> Self {
        self.operation = Some(op);
        self
    }

    pub fn with_value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = Some(uid.to_string());
        self
    }

    pub fn create(self) -> FilterPlan {
        FilterPlan {
            column: self.column,
            operation: self.operation,
            value: self.value,
            priority: self.priority,
            uid: self.uid,
        }
    }
}
