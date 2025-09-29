use crate::engine::core::Event;
use serde_json::{Number, Value};
use std::collections::HashMap;
use tracing::{debug, warn};

/// Builds Event objects from zone values
pub struct EventBuilder {
    pub event_type: String,
    pub context_id: String,
    pub timestamp: u64,
    pub payload: HashMap<String, Value>,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            event_type: String::new(),
            context_id: String::new(),
            timestamp: 0,
            payload: HashMap::new(),
        }
    }

    pub fn with_payload_capacity(capacity: usize) -> Self {
        Self {
            event_type: String::new(),
            context_id: String::new(),
            timestamp: 0,
            payload: HashMap::with_capacity(capacity),
        }
    }

    pub fn build(self) -> Event {
        debug!(target: "builder::event", "Building event with {} payload fields", self.payload.len());
        Event {
            event_type: self.event_type,
            context_id: self.context_id,
            timestamp: self.timestamp,
            payload: Value::Object(self.payload.into_iter().collect()),
        }
    }

    pub fn add_field(&mut self, field: &str, value: &str) {
        debug!(target: "builder::event", %field, %value, "Adding field");
        match field {
            "event_type" => {
                self.event_type = value.to_string();
                debug!(target: "builder::event", "Set event_type = {}", self.event_type);
            }
            "context_id" => {
                self.context_id = value.to_string();
                debug!(target: "builder::event", "Set context_id = {}", self.context_id);
            }
            "timestamp" => match value.parse::<u64>() {
                Ok(ts) => {
                    self.timestamp = ts;
                    debug!(target: "builder::event", "Parsed timestamp = {}", ts);
                }
                Err(_) => {
                    warn!(target: "builder::event", %value, "Failed to parse timestamp, using 0");
                    self.timestamp = 0;
                }
            },
            _ => self.add_payload_field(field, value),
        }
    }

    fn add_payload_field(&mut self, field: &str, value: &str) {
        // Try to parse as number first
        if let Ok(num) = value.parse::<i64>() {
            self.payload
                .insert(field.to_string(), Value::Number(num.into()));
            debug!(target: "builder::event", %field, "Parsed as i64");
        } else if let Ok(f) = value.parse::<f64>() {
            if let Some(n) = Number::from_f64(f) {
                self.payload.insert(field.to_string(), Value::Number(n));
                debug!(target: "builder::event", %field, "Parsed as f64");
            } else {
                warn!(target: "builder::event", %field, %value, "f64 parsed but could not convert to JSON Number");
                self.payload
                    .insert(field.to_string(), Value::String(value.to_string()));
            }
        } else {
            self.payload
                .insert(field.to_string(), Value::String(value.to_string()));
            debug!(target: "builder::event", %field, "Stored as String");
        }
    }
}
