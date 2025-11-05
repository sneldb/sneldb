use crate::engine::core::{Event, EventId};
use crate::engine::types::ScalarValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalEntry {
    pub timestamp: u64,
    pub context_id: String,
    pub event_type: String,
    pub payload: BTreeMap<String, ScalarValue>,
    #[serde(default)]
    pub event_id: EventId,
}

impl WalEntry {
    pub fn from_event(event: &Event) -> Self {
        Self {
            timestamp: event.timestamp,
            context_id: event.context_id.clone(),
            event_type: event.event_type.clone(),
            payload: event.payload.clone(),
            event_id: event.event_id(),
        }
    }

    pub fn payload_as_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for (k, v) in &self.payload {
            map.insert(k.clone(), v.to_json());
        }
        serde_json::Value::Object(map)
    }

    pub fn set_payload_json(&mut self, value: serde_json::Value) {
        self.payload = match value {
            serde_json::Value::Object(map) => {
                let mut out = BTreeMap::new();
                for (k, v) in map.into_iter() {
                    out.insert(k, ScalarValue::from(v));
                }
                out
            }
            _ => BTreeMap::new(),
        };
    }
}
