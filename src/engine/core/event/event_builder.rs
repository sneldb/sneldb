use crate::engine::core::Event;
use crate::engine::core::event::event_id::EventId;
use crate::engine::types::ScalarValue;
use std::collections::BTreeMap;

/// Builds Event objects from zone values
pub struct EventBuilder {
    pub event_type: String,
    pub context_id: String,
    pub timestamp: u64,
    pub event_id: EventId,
    pub payload: BTreeMap<String, ScalarValue>,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            event_type: String::new(),
            context_id: String::new(),
            timestamp: 0,
            event_id: EventId::default(),
            payload: BTreeMap::new(),
        }
    }

    /// Create a new EventBuilder (capacity hint not used for BTreeMap)
    #[inline]
    pub fn with_capacity(_capacity: usize) -> Self {
        // BTreeMap doesn't support with_capacity, just create new
        Self::new()
    }

    pub fn build(self) -> Event {
        Event {
            event_type: self.event_type,
            context_id: self.context_id,
            timestamp: self.timestamp,
            id: self.event_id,
            payload: self.payload,
        }
    }

    #[inline]
    pub fn add_field_i64(&mut self, field: &str, value: i64) {
        match field {
            "timestamp" => {
                self.timestamp = value as u64;
            }
            "event_id" => {
                self.event_id = EventId::from((value as i128).max(0) as u64);
            }
            "context_id" | "event_type" => {
                // fall back to string semantics for non-numeric fixed fields
                self.add_field(field, &value.to_string());
            }
            _ => {
                // Reuse the field string allocation from the match
                self.insert_value(field, ScalarValue::Int64(value));
            }
        }
    }

    /// Helper to insert a value with field name (avoids repeated to_string calls in hot paths)
    #[inline(always)]
    fn insert_value(&mut self, field: &str, value: ScalarValue) {
        self.payload.insert(field.to_string(), value);
    }

    #[inline]
    pub fn add_field_u64(&mut self, field: &str, value: u64) {
        match field {
            "timestamp" => {
                self.timestamp = value;
            }
            "event_id" => {
                self.event_id = EventId::from(value);
            }
            "context_id" | "event_type" => {
                self.add_field(field, &value.to_string());
            }
            _ => {
                self.insert_value(
                    field,
                    i64::try_from(value)
                        .map(ScalarValue::Int64)
                        .unwrap_or_else(|_| ScalarValue::Utf8(value.to_string())),
                );
            }
        }
    }

    #[inline]
    pub fn add_field_f64(&mut self, field: &str, value: f64) {
        if value.is_finite() {
            self.insert_value(field, ScalarValue::Float64(value));
        } else {
            self.insert_value(field, ScalarValue::Null);
        }
    }

    #[inline]
    pub fn add_field_bool(&mut self, field: &str, value: bool) {
        match field {
            "context_id" | "event_type" => {
                self.add_field(field, if value { "true" } else { "false" })
            }
            _ => {
                self.insert_value(field, ScalarValue::Boolean(value));
            }
        }
    }

    #[inline]
    pub fn add_field_null(&mut self, field: &str) {
        match field {
            "context_id" | "event_type" => self.add_field(field, ""),
            _ => {
                self.insert_value(field, ScalarValue::Null);
            }
        }
    }

    pub fn add_field(&mut self, field: &str, value: &str) {
        match field {
            "event_type" => {
                self.event_type = value.to_string();
            }
            "context_id" => {
                self.context_id = value.to_string();
            }
            "timestamp" => {
                self.timestamp = value.parse::<u64>().unwrap_or(0);
            }
            "event_id" => {
                self.event_id = value
                    .trim()
                    .parse::<u64>()
                    .map(EventId::from)
                    .unwrap_or_default();
            }
            _ => self.add_payload_field(field, value),
        }
    }

    #[inline]
    fn add_payload_field(&mut self, field: &str, value: &str) {
        // Normalize whitespace
        let trimmed = value.trim();

        // Booleans and null (JSON-style keywords, lowercase)
        match trimmed {
            "true" => {
                self.insert_value(field, ScalarValue::Boolean(true));
                return;
            }
            "false" => {
                self.insert_value(field, ScalarValue::Boolean(false));
                return;
            }
            "null" => {
                self.insert_value(field, ScalarValue::Null);
                return;
            }
            _ => {}
        }

        // Try integers: prefer signed if a leading '-'; else try u64 first to capture large positives
        if trimmed.starts_with('-') {
            if let Ok(i) = trimmed.parse::<i64>() {
                self.insert_value(field, ScalarValue::Int64(i));
                return;
            }
        } else if let Ok(u) = trimmed.parse::<u64>() {
            self.insert_value(
                field,
                i64::try_from(u)
                    .map(ScalarValue::Int64)
                    .unwrap_or_else(|_| ScalarValue::Utf8(u.to_string())),
            );
            return;
        } else if let Ok(i) = trimmed.parse::<i64>() {
            self.insert_value(field, ScalarValue::Int64(i));
            return;
        }

        // Try float; reject NaN/Inf that JSON cannot represent
        if let Ok(f) = trimmed.parse::<f64>() {
            if f.is_finite() {
                self.insert_value(field, ScalarValue::Float64(f));
                return;
            }
            // Non-finite float: fall through to store as string
        }

        // Fallback: store as String (preserve original value, not trimmed)
        self.insert_value(field, ScalarValue::Utf8(value.to_string()));
    }
}
