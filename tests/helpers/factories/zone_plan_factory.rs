use crate::engine::core::{Event, ZonePlan};
use crate::test_helpers::factory::Factory;
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct ZonePlanFactory {
    params: HashMap<String, Value>,
}

impl ZonePlanFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        let events = serde_json::to_value(Factory::event().create_list(2)).unwrap();
        params.insert("id".into(), json!(0));
        params.insert("start_index".into(), json!(0));
        params.insert("end_index".into(), json!(0));
        params.insert("uid".into(), json!("uid_test"));
        params.insert("event_type".into(), json!("test_event"));
        params.insert("segment_id".into(), json!(1));
        params.insert("events".into(), events); // default to empty
        Self { params }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn create(self) -> ZonePlan {
        let events: Vec<Event> =
            serde_json::from_value(self.params["events"].clone()).unwrap_or_default();

        ZonePlan {
            id: self.params["id"].as_u64().unwrap() as u32,
            start_index: self.params["start_index"].as_u64().unwrap() as usize,
            end_index: self.params["end_index"].as_u64().unwrap() as usize,
            uid: self.params["uid"].as_str().unwrap().to_string(),
            event_type: self.params["event_type"].as_str().unwrap().to_string(),
            segment_id: self.params["segment_id"].as_u64().unwrap(),
            events,
        }
    }
}
