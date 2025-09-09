use crate::engine::core::Event;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalEntry {
    pub timestamp: u64,
    pub context_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

impl WalEntry {
    pub fn from_event(event: &Event) -> Self {
        Self {
            timestamp: event.timestamp,
            context_id: event.context_id.clone(),
            event_type: event.event_type.clone(),
            payload: event.payload.clone(),
        }
    }
}
