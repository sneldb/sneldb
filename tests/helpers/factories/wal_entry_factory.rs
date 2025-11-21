use crate::engine::core::{EventId, WalEntry};
use rand::Rng;
use serde_json::{Value, json};
use std::collections::HashMap;

pub struct WalEntryFactory {
    params: HashMap<String, Value>,
}

impl WalEntryFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        params.insert("context_id".into(), json!("ctx1"));
        params.insert("timestamp".into(), json!(123456));
        params.insert("event_type".into(), json!("test_event"));
        params.insert("payload".into(), json!({ "key": "value" }));
        params.insert("event_id".into(), json!(1));
        Self { params }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn create(self) -> WalEntry {
        let mut entry = WalEntry {
            context_id: self.params["context_id"].as_str().unwrap().to_string(),
            timestamp: self.params["timestamp"].as_u64().unwrap(),
            event_type: self.params["event_type"].as_str().unwrap().to_string(),
            payload: HashMap::new(),
            event_id: self.params["event_id"]
                .as_u64()
                .map(EventId::from)
                .unwrap_or_default(),
        };
        entry.set_payload_json(self.params["payload"].clone());
        entry
    }

    pub fn create_list(self, count: usize) -> Vec<WalEntry> {
        let mut rng = rand::thread_rng();
        (0..count)
            .map(|i| {
                let context_id = format!("ctx{}", i + 1);
                let timestamp = rng.gen_range(123456..123456 + 1000000);

                let mut payload_json = self.params["payload"].clone();
                if let Some(obj) = payload_json.as_object_mut() {
                    obj.insert("index".into(), json!(i));
                }
                let mut entry = WalEntry {
                    context_id,
                    timestamp,
                    event_type: self.params["event_type"].as_str().unwrap().to_string(),
                    payload: HashMap::new(),
                    event_id: EventId::from(i as u64 + 1),
                };
                entry.set_payload_json(payload_json);
                entry
            })
            .collect()
    }
}
