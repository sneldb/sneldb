use crate::command::types::CompareOp;
use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::types::ScalarValue;

/// Factory for creating `FilterGroup` objects in tests
pub struct FilterGroupFactory {
    column: String,
    operation: Option<CompareOp>,
    value: Option<ScalarValue>,
    priority: u32,
    uid: Option<String>,
}

impl FilterGroupFactory {
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

    pub fn with_value(mut self, value: impl Into<ScalarValue>) -> Self {
        self.value = Some(value.into());
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

    pub fn create(self) -> FilterGroup {
        FilterGroup::Filter {
            column: self.column,
            operation: self.operation,
            value: self.value,
            priority: self.priority,
            uid: self.uid,
            index_strategy: None,
        }
    }
}
