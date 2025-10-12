use crate::engine::schema::FieldType;
use crate::engine::schema::registry::{MiniSchema, SchemaRecord};
use std::collections::HashMap;

pub struct SchemaRecordFactory {
    uid: String,
    event_type: String,
    fields: HashMap<String, FieldType>,
}

impl SchemaRecordFactory {
    pub fn new(event_type: &str) -> Self {
        let mut fields = HashMap::new();
        fields.insert("field1".to_string(), FieldType::String);
        fields.insert("field2".to_string(), FieldType::U64);

        Self {
            uid: "uid123".to_string(),
            event_type: event_type.to_string(),
            fields,
        }
    }

    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = uid.to_string();
        self
    }

    pub fn with_field(mut self, key: &str, type_: &str) -> Self {
        let ft = FieldType::from_primitive_str(type_).unwrap_or(FieldType::String);
        self.fields.insert(key.to_string(), ft);
        self
    }

    pub fn without_field(mut self, key: &str) -> Self {
        self.fields.remove(key);
        self
    }

    pub fn create(self) -> SchemaRecord {
        SchemaRecord {
            uid: self.uid,
            event_type: self.event_type,
            schema: MiniSchema {
                fields: self.fields,
            },
        }
    }
}
