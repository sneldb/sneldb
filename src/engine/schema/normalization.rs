use crate::engine::schema::FieldType;
use crate::engine::schema::registry::MiniSchema;
use crate::shared::time::{TimeKind, TimeParser};

pub struct PayloadTimeNormalizer<'a> {
    schema: &'a MiniSchema,
}

impl<'a> PayloadTimeNormalizer<'a> {
    pub fn new(schema: &'a MiniSchema) -> Self {
        Self { schema }
    }

    /// Walk payload according to schema and normalize time-typed fields to epoch seconds (i64).
    pub fn normalize(&self, payload: &mut serde_json::Value) -> Result<(), String> {
        let obj = payload
            .as_object_mut()
            .ok_or_else(|| "Payload must be a JSON object".to_string())?;

        for (field, field_type) in &self.schema.fields {
            match field_type {
                FieldType::Timestamp => {
                    if let Some(v) = obj.get_mut(field) {
                        TimeParser::normalize_json_value(v, TimeKind::DateTime)?;
                    }
                }
                FieldType::Date => {
                    if let Some(v) = obj.get_mut(field) {
                        TimeParser::normalize_json_value(v, TimeKind::Date)?;
                    }
                }
                FieldType::Optional(inner) => {
                    if matches!(**inner, FieldType::Timestamp | FieldType::Date) {
                        if let Some(v) = obj.get_mut(field) {
                            let kind = if matches!(**inner, FieldType::Timestamp) {
                                TimeKind::DateTime
                            } else {
                                TimeKind::Date
                            };
                            if !v.is_null() {
                                TimeParser::normalize_json_value(v, kind)?;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}
