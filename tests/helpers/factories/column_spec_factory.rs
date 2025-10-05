use crate::engine::core::read::result::ColumnSpec;

pub struct ColumnSpecFactory {
    name: String,
    logical_type: String,
}

impl ColumnSpecFactory {
    pub fn new() -> Self {
        Self {
            name: "col".to_string(),
            logical_type: "String".to_string(),
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn with_logical_type(mut self, logical_type: &str) -> Self {
        self.logical_type = logical_type.to_string();
        self
    }

    pub fn string(name: &str) -> ColumnSpec {
        Self::new()
            .with_name(name)
            .with_logical_type("String")
            .create()
    }

    pub fn integer(name: &str) -> ColumnSpec {
        Self::new()
            .with_name(name)
            .with_logical_type("Integer")
            .create()
    }

    pub fn float(name: &str) -> ColumnSpec {
        Self::new()
            .with_name(name)
            .with_logical_type("Float")
            .create()
    }

    pub fn timestamp(name: &str) -> ColumnSpec {
        Self::new()
            .with_name(name)
            .with_logical_type("Timestamp")
            .create()
    }

    pub fn create(self) -> ColumnSpec {
        ColumnSpec {
            name: self.name,
            logical_type: self.logical_type,
        }
    }
}
