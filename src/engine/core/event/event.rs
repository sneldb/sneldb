use crate::engine::errors::StoreError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub event_type: String,
    pub context_id: String,
    pub timestamp: u64,
    pub payload: Value,
}

impl Event {
    pub fn validate(&self) -> Result<(), StoreError> {
        if self.context_id.trim().is_empty() {
            warn!(target: "event::validate", "Invalid context_id: empty");
            return Err(StoreError::InvalidContextId);
        }
        if self.event_type.trim().is_empty() {
            warn!(target: "event::validate", "Invalid event_type: empty");
            return Err(StoreError::InvalidEventType);
        }
        Ok(())
    }

    pub fn get_field(&self, name: &str) -> Option<Value> {
        match name {
            "context_id" => Some(Value::String(self.context_id.clone())),
            "event_type" => Some(Value::String(self.event_type.clone())),
            "timestamp" => Some(Value::Number(self.timestamp.into())),
            _ => self.payload.get(name).cloned(),
        }
    }

    /// Returns the value of the specified field as a String.
    pub fn get_field_value(&self, field: &str) -> String {
        debug!(target: "event::access", %field, "Getting field value");
        match field {
            "context_id" => self.context_id.clone(),
            "event_type" => self.event_type.clone(),
            "timestamp" => self.timestamp.to_string(),
            other => {
                if let Some(obj) = self.payload.as_object() {
                    if let Some(v) = obj.get(other) {
                        let result = if let Some(s) = v.as_str() {
                            s.to_string()
                        } else if let Some(n) = v.as_i64() {
                            n.to_string()
                        } else if let Some(n) = v.as_u64() {
                            n.to_string()
                        } else if let Some(f) = v.as_f64() {
                            f.to_string()
                        } else if let Some(b) = v.as_bool() {
                            b.to_string()
                        } else {
                            v.to_string()
                        };
                        debug!(target: "event::access", %field, %result, "Resolved dynamic field value");
                        return result;
                    }
                }
                debug!(target: "event::access", %field, "Field not found or not resolvable, returning empty string");
                "".to_string()
            }
        }
    }

    /// Returns the value of the specified field as a zero-padded sortable String.
    /// Numeric values are zero-padded so lexicographic comparison works correctly.
    /// This is used by RLTE index for proper ordering of numeric fields.
    pub fn get_field_value_sortable(&self, field: &str) -> String {
        match field {
            "context_id" => self.context_id.clone(),
            "event_type" => self.event_type.clone(),
            // Pad timestamp to 20 digits for u64 (max: 18446744073709551615 = 20 digits)
            "timestamp" => format!("{:020}", self.timestamp),
            other => {
                if let Some(obj) = self.payload.as_object() {
                    if let Some(v) = obj.get(other) {
                        if let Some(s) = v.as_str() {
                            // Keep strings as-is
                            s.to_string()
                        } else if let Some(n) = v.as_i64() {
                            // Pad signed integers with bias to make them sortable
                            // Use wrapping_sub to avoid i128 overhead
                            // Maps i64::MIN -> 0, 0 -> 9223372036854775808, i64::MAX -> u64::MAX
                            let biased = n.wrapping_sub(i64::MIN) as u64;
                            format!("{:020}", biased)
                        } else if let Some(n) = v.as_u64() {
                            // Pad unsigned integers to 20 digits
                            format!("{:020}", n)
                        } else if let Some(f) = v.as_f64() {
                            // For floats, convert to sortable format (limited precision)
                            // Use scientific notation with fixed width
                            format!("{:+025.10e}", f)
                        } else if let Some(b) = v.as_bool() {
                            // Booleans: "false" < "true" lexicographically (correct)
                            b.to_string()
                        } else {
                            v.to_string()
                        }
                    } else {
                        "".to_string()
                    }
                } else {
                    "".to_string()
                }
            }
        }
    }

    /// Collects all field names in this event: fixed + dynamic (from payload).
    pub fn collect_all_fields(&self) -> HashSet<String> {
        let mut fields: HashSet<String> = HashSet::new();
        fields.insert("context_id".to_string());
        fields.insert("event_type".to_string());
        fields.insert("timestamp".to_string());
        if let Some(obj) = self.payload.as_object() {
            for key in obj.keys() {
                fields.insert(key.clone());
            }
        }
        debug!(target: "event::meta", ?fields, "Collected all field names");
        fields
    }

    pub fn order_by(events: &[Event], field: &str) -> Vec<Event> {
        let mut sorted = events.to_vec();
        match field {
            "timestamp" => {
                debug!(target: "event::meta", "Ordering events by timestamp");
                sorted.sort_by_key(|e| e.timestamp)
            }
            "event_type" => {
                debug!(target: "event::meta", "Ordering events by event_type");
                sorted.sort_by(|a, b| a.event_type.cmp(&b.event_type))
            }
            _ => {
                debug!(target: "event::meta", %field, "Ordering not supported for this field");
            }
        }
        sorted
    }

    pub fn group_by<'a>(events: &'a [Event], field: &str) -> HashMap<String, Vec<&'a Event>> {
        let mut map: HashMap<String, Vec<&'a Event>> = HashMap::new();
        for event in events {
            let key = match field {
                "context_id" => event.context_id.clone(),
                "event_type" => event.event_type.clone(),
                _ => {
                    debug!(target: "event::meta", %field, "Grouping not supported for this field");
                    continue;
                }
            };
            map.entry(key).or_default().push(event);
        }
        debug!(target: "event::meta", "Grouped events by {}", field);
        map
    }
}
