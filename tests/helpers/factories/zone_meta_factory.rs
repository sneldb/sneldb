use crate::engine::core::ZoneMeta;
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct ZoneMetaFactory {
    params: HashMap<String, Value>,
}

impl ZoneMetaFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        params.insert("zone_id".into(), json!(0));
        params.insert("uid".into(), json!("uid_test"));
        params.insert("segment_id".into(), json!(42));
        params.insert("start_row".into(), json!(0));
        params.insert("end_row".into(), json!(99));
        params.insert("timestamp_min".into(), json!(1_000_000));
        params.insert("timestamp_max".into(), json!(1_000_999));
        Self { params }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn create(self) -> ZoneMeta {
        ZoneMeta {
            zone_id: self.params["zone_id"].as_u64().unwrap() as u32,
            uid: self.params["uid"].as_str().unwrap().to_string(),
            segment_id: self.params["segment_id"].as_u64().unwrap(),
            start_row: self.params["start_row"].as_u64().unwrap() as u32,
            end_row: self.params["end_row"].as_u64().unwrap() as u32,
            timestamp_min: self.params["timestamp_min"].as_u64().unwrap(),
            timestamp_max: self.params["timestamp_max"].as_u64().unwrap(),
        }
    }
}
