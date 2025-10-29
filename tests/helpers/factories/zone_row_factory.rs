use crate::engine::core::ZoneRow;
use serde_json::Value;
use std::collections::HashMap;

pub struct ZoneRowFactory {
    context_id: String,
    timestamp: String,
    event_type: String,
    segment_id: u64,
    zone_id: u32,
    payload: HashMap<String, Value>,
}

impl ZoneRowFactory {
    pub fn new() -> Self {
        Self {
            context_id: "ctx1".into(),
            timestamp: "123456".into(),
            event_type: "signup".into(),
            segment_id: 42,
            zone_id: 0,
            payload: HashMap::from([("plan".into(), Value::String("free".into()))]),
        }
    }

    pub fn with_context_id(mut self, id: &str) -> Self {
        self.context_id = id.into();
        self
    }

    pub fn with_timestamp(mut self, ts: &str) -> Self {
        self.timestamp = ts.into();
        self
    }

    pub fn with_event_type(mut self, et: &str) -> Self {
        self.event_type = et.into();
        self
    }

    pub fn with_payload_field(mut self, key: &str, value: &str) -> Self {
        self.payload.insert(key.into(), Value::String(value.into()));
        self
    }

    pub fn with_payload_map(mut self, map: HashMap<String, Value>) -> Self {
        self.payload = map;
        self
    }

    pub fn with_segment_id(mut self, id: u64) -> Self {
        self.segment_id = id;
        self
    }

    pub fn with_zone_id(mut self, id: u32) -> Self {
        self.zone_id = id;
        self
    }

    pub fn create(self) -> ZoneRow {
        ZoneRow {
            segment_id: self.segment_id,
            zone_id: self.zone_id,
            context_id: self.context_id,
            timestamp: self.timestamp,
            event_type: self.event_type,
            payload: self.payload,
        }
    }
}
