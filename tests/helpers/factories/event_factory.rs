use crate::engine::core::{Event, EventId};
use rand::Rng;
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct EventFactory {
    params: HashMap<String, Value>,
}

impl EventFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        params.insert("context_id".into(), json!("ctx1"));
        params.insert("timestamp".into(), json!(123456));
        params.insert("event_type".into(), json!("test_event"));
        params.insert("payload".into(), json!({ "key": "value" }));
        Self { params }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn create(self) -> Event {
        let event_id_raw = self
            .params
            .get("event_id")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| rand::random::<u64>());
        Event {
            context_id: self.params["context_id"].as_str().unwrap().to_string(),
            timestamp: self.params["timestamp"].as_u64().unwrap(),
            event_type: self.params["event_type"].as_str().unwrap().to_string(),
            id: EventId::from(event_id_raw),
            payload: self.params["payload"].clone(),
        }
    }

    pub fn create_list(self, count: usize) -> Vec<Event> {
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|i| {
                // Optionally modify context_id/timestamp per event
                let context_id = format!("ctx{}", i + 1);
                // Generate a random timestamp between 123456 and 123456 + 1000000
                let timestamp = rng.gen_range(123456..123456 + 1000000);

                let mut payload = self.params["payload"].clone();
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("index".into(), json!(i));
                }

                let event_id = self
                    .params
                    .get("event_id")
                    .and_then(|v| v.as_u64())
                    .unwrap_or_else(rand::random);

                Event {
                    context_id,
                    timestamp,
                    event_type: self.params["event_type"].as_str().unwrap().to_string(),
                    id: EventId::from(event_id),
                    payload,
                }
            })
            .collect()
    }
}
