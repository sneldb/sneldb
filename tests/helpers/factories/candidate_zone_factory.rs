use crate::engine::core::CandidateZone;
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct CandidateZoneFactory {
    params: HashMap<String, Value>,
    values: Option<HashMap<String, Vec<String>>>,
}

impl CandidateZoneFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        params.insert("zone_id".into(), json!(0));
        params.insert("segment_id".into(), json!("segment-0001"));
        Self {
            params,
            values: None,
        }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn with_values(mut self, values: HashMap<String, Vec<String>>) -> Self {
        self.values = Some(values);
        self
    }

    pub fn create(self) -> CandidateZone {
        let zone_id = self
            .params
            .get("zone_id")
            .and_then(|v| v.as_u64())
            .expect("zone_id is missing or not a u64");

        let segment_id = self
            .params
            .get("segment_id")
            .and_then(|v| v.as_str())
            .expect("segment_id is missing or not a string");

        let mut zone = CandidateZone::new(zone_id as u32, segment_id.to_string());

        if let Some(values) = self.values {
            zone.set_values(values);
        }

        zone
    }
}
