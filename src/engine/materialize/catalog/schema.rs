use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSnapshot {
    pub name: String,
    pub logical_type: String,
}

impl SchemaSnapshot {
    pub fn new(name: impl Into<String>, logical_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            logical_type: logical_type.into(),
        }
    }
}
