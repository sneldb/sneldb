use crate::engine::core::Event;
use serde_json::{Map, Number, Value};
use tracing::{debug, warn};

/// Builds Event objects from zone values
pub struct EventBuilder {
    pub event_type: String,
    pub context_id: String,
    pub timestamp: u64,
    pub payload: Map<String, Value>,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            event_type: String::new(),
            context_id: String::new(),
            timestamp: 0,
            payload: Map::new(),
        }
    }

    pub fn build(self) -> Event {
        debug!(target: "builder::event", "Building event with {} payload fields", self.payload.len());
        Event {
            event_type: self.event_type,
            context_id: self.context_id,
            timestamp: self.timestamp,
            payload: Value::Object(self.payload),
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
        // Normalize whitespace
        let trimmed = value.trim();

        // Booleans and null (JSON-style keywords, lowercase)
        match trimmed {
            "true" => {
                self.payload.insert(field.to_string(), Value::Bool(true));
                debug!(target: "builder::event", %field, "Parsed as bool true");
                return;
            }
            "false" => {
                self.payload.insert(field.to_string(), Value::Bool(false));
                debug!(target: "builder::event", %field, "Parsed as bool false");
                return;
            }
            "null" => {
                self.payload.insert(field.to_string(), Value::Null);
                debug!(target: "builder::event", %field, "Parsed as null");
                return;
            }
            _ => {}
        }

        // Try integers: prefer signed if a leading '-'; else try u64 first to capture large positives
        if trimmed.starts_with('-') {
            if let Ok(i) = trimmed.parse::<i64>() {
                self.payload
                    .insert(field.to_string(), Value::Number(i.into()));
                debug!(target: "builder::event", %field, "Parsed as i64");
                return;
            }
        } else if let Ok(u) = trimmed.parse::<u64>() {
            self.payload
                .insert(field.to_string(), Value::Number(Number::from(u)));
            debug!(target: "builder::event", %field, "Parsed as u64");
            return;
        } else if let Ok(i) = trimmed.parse::<i64>() {
            self.payload
                .insert(field.to_string(), Value::Number(i.into()));
            debug!(target: "builder::event", %field, "Parsed as i64");
            return;
        }

        // Try float; reject NaN/Inf that JSON cannot represent
        if let Ok(f) = trimmed.parse::<f64>() {
            if let Some(n) = Number::from_f64(f) {
                self.payload.insert(field.to_string(), Value::Number(n));
                debug!(target: "builder::event", %field, "Parsed as f64");
                return;
            } else {
                warn!(target: "builder::event", %field, value=%trimmed, "Non-finite float not representable in JSON; storing as String");
            }
        }

        // Fallback: store as String (preserve original value, not trimmed)
        self.payload
            .insert(field.to_string(), Value::String(value.to_string()));
        debug!(target: "builder::event", %field, "Stored as String");
    }
}
