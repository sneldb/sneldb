use crate::engine::core::{ColumnKey, WriteJob};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Clone)]
pub struct WriteJobFactory {
    key: ColumnKey,
    zone_id: u32,
    path: PathBuf,
}

impl WriteJobFactory {
    pub fn new() -> Self {
        Self {
            key: ("event".to_string(), "field".to_string()),
            zone_id: 0,
            path: PathBuf::from("/dev/null"),
        }
    }

    pub fn with_key<S: Into<String>>(mut self, event_type: S, field: S) -> Self {
        self.key = (event_type.into(), field.into());
        self
    }

    pub fn with_zone_id(mut self, zone_id: u32) -> Self {
        self.zone_id = zone_id;
        self
    }

    pub fn with_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = path.into();
        self
    }

    pub fn create_with_value<S: Into<String>>(&self, value: S) -> WriteJob {
        WriteJob {
            key: self.key.clone(),
            zone_id: self.zone_id,
            path: self.path.clone(),
            value: Value::String(value.into()),
        }
    }

    pub fn create_many_with_values<S: AsRef<str>>(&self, values: &[S]) -> Vec<WriteJob> {
        values
            .iter()
            .map(|v| self.create_with_value(v.as_ref()))
            .collect()
    }
}
