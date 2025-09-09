use crate::engine::schema::registry::MiniSchema;
use crate::engine::schema::{EnumType, FieldType};
use std::collections::HashMap;

pub struct MiniSchemaFactory {
    fields: HashMap<String, FieldType>,
}

impl MiniSchemaFactory {
    pub fn new() -> Self {
        let mut fields = HashMap::new();
        fields.insert("username".to_string(), FieldType::String);
        // map legacy "datetime" to string for tests
        fields.insert("created_at".to_string(), FieldType::String);
        Self { fields }
    }

    pub fn with(mut self, key: &str, value: &str) -> Self {
        let ft = FieldType::from_primitive_str(value).unwrap_or(FieldType::String);
        self.fields.insert(key.to_string(), ft);
        self
    }

    pub fn with_enum(mut self, key: &str, variants: &[&str]) -> Self {
        let enum_type = EnumType {
            variants: variants.iter().map(|s| s.to_string()).collect(),
        };
        self.fields
            .insert(key.to_string(), FieldType::Enum(enum_type));
        self
    }

    pub fn with_optional(mut self, key: &str, base: &str) -> Self {
        let base_ft = FieldType::from_primitive_str(base).unwrap_or(FieldType::String);
        self.fields
            .insert(key.to_string(), FieldType::Optional(Box::new(base_ft)));
        self
    }

    pub fn without(mut self, key: &str) -> Self {
        self.fields.remove(key);
        self
    }

    pub fn empty() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn create(self) -> MiniSchema {
        MiniSchema {
            fields: self.fields,
        }
    }
}
