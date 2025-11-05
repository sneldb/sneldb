use crate::engine::core::event::event_id::EventId;
use crate::engine::errors::StoreError;
use crate::engine::types::ScalarValue;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub event_type: String,
    pub context_id: String,
    pub timestamp: u64,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub(crate) id: EventId,
    #[serde(default)]
    pub payload: BTreeMap<String, ScalarValue>,
}

impl Event {
    #[inline]
    pub fn event_id(&self) -> EventId {
        self.id
    }

    #[inline]
    pub(crate) fn set_event_id(&mut self, id: EventId) {
        self.id = id;
    }

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

    pub fn get_field(&self, name: &str) -> Option<JsonValue> {
        self.get_field_scalar(name)
            .map(|v: ScalarValue| v.to_json())
    }

    pub fn get_field_scalar(&self, name: &str) -> Option<ScalarValue> {
        match name {
            "context_id" => Some(ScalarValue::Utf8(self.context_id.clone())),
            "event_type" => Some(ScalarValue::Utf8(self.event_type.clone())),
            "timestamp" => Some(ScalarValue::Timestamp(self.timestamp as i64)),
            "event_id" => Some(ScalarValue::Int64(self.id.raw() as i64)),
            _ => self.payload.get(name).cloned(),
        }
    }

    /// Returns the value of the specified field as a String.
    #[inline]
    pub fn get_field_value(&self, field: &str) -> String {
        match field {
            "context_id" => self.context_id.clone(),
            "event_type" => self.event_type.clone(),
            "timestamp" => self.timestamp.to_string(),
            "event_id" => self.id.raw().to_string(),
            other => self
                .payload
                .get(other)
                .map(Self::scalar_to_string)
                .unwrap_or_default(),
        }
    }

    /// Returns the value of the specified field as a zero-padded sortable String.
    /// Numeric values are zero-padded so lexicographic comparison works correctly.
    /// This is used by RLTE index for proper ordering of numeric fields.
    #[inline]
    pub fn get_field_value_sortable(&self, field: &str) -> String {
        match field {
            "context_id" => self.context_id.clone(),
            "event_type" => self.event_type.clone(),
            // Pad timestamp to 20 digits for u64 (max: 18446744073709551615 = 20 digits)
            "timestamp" => format!("{:020}", self.timestamp),
            "event_id" => format!("{:020}", self.id.raw()),
            other => self
                .payload
                .get(other)
                .map(Self::scalar_to_sortable)
                .unwrap_or_default(),
        }
    }

    /// Collects all field names in this event: fixed + dynamic (from payload).
    pub fn collect_all_fields(&self) -> HashSet<String> {
        let mut fields: HashSet<String> = HashSet::new();
        fields.insert("context_id".to_string());
        fields.insert("event_type".to_string());
        fields.insert("timestamp".to_string());
        fields.insert("event_id".to_string());
        for key in self.payload.keys() {
            fields.insert(key.clone());
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
            "event_id" => {
                debug!(target: "event::meta", "Ordering events by event_id");
                sorted.sort_by_key(|e| e.id.raw())
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

impl Event {
    fn scalar_to_string(value: &ScalarValue) -> String {
        match value {
            ScalarValue::Utf8(s) => s.clone(),
            ScalarValue::Boolean(b) => b.to_string(),
            ScalarValue::Int64(i) => i.to_string(),
            ScalarValue::Float64(f) => f.to_string(),
            ScalarValue::Timestamp(ts) => ts.to_string(),
            ScalarValue::Binary(bytes) => BASE64_STANDARD.encode(bytes),
            ScalarValue::Null => "null".to_string(),
        }
    }

    fn scalar_to_sortable(value: &ScalarValue) -> String {
        if let Some(i) = value.as_i64() {
            let biased = i.wrapping_sub(i64::MIN) as u64;
            return format!("{:020}", biased);
        }
        if let Some(u) = value.as_u64() {
            return format!("{:020}", u);
        }
        if let Some(f) = value.as_f64() {
            return format!("{:+025.10e}", f);
        }
        if let Some(s) = value.as_str() {
            return s.to_string();
        }
        if let Some(b) = value.as_bool() {
            return b.to_string();
        }
        value.to_json().to_string()
    }

    pub fn set_payload_json(&mut self, value: JsonValue) {
        self.payload = match value {
            JsonValue::Object(map) => {
                let mut out = BTreeMap::new();
                for (k, v) in map.into_iter() {
                    out.insert(k, ScalarValue::from(v));
                }
                out
            }
            _ => BTreeMap::new(),
        };
    }

    pub fn payload_as_json(&self) -> JsonValue {
        let mut map = serde_json::Map::new();
        for (k, v) in &self.payload {
            map.insert(k.clone(), v.to_json());
        }
        JsonValue::Object(map)
    }

    /// Optimized: Build JSON string directly without intermediate JsonValue.
    /// Since payloads are flat, we can serialize directly.
    pub fn payload_as_json_string(&self) -> String {
        if self.payload.is_empty() {
            return "{}".to_string();
        }

        // Pre-allocate capacity: estimate ~20 chars per field (key + value + overhead)
        let estimated_capacity = self.payload.len() * 30;
        let mut result = String::with_capacity(estimated_capacity);
        result.push('{');
        let mut first = true;

        for (k, v) in &self.payload {
            if !first {
                result.push(',');
            }
            first = false;

            // Escape and add key
            result.push('"');
            escape_json_string(k, &mut result);
            result.push('"');
            result.push(':');

            // Add value (no JSON parsing needed for flat payloads)
            match v {
                ScalarValue::Null => result.push_str("null"),
                ScalarValue::Boolean(b) => result.push_str(if *b { "true" } else { "false" }),
                ScalarValue::Int64(i) => {
                    // Use itoa for fast integer formatting
                    let mut buffer = itoa::Buffer::new();
                    result.push_str(buffer.format(*i));
                }
                ScalarValue::Float64(f) => {
                    // Use ryu for fast float formatting
                    let mut buffer = ryu::Buffer::new();
                    result.push_str(buffer.format(*f));
                }
                ScalarValue::Timestamp(ts) => {
                    // Use itoa for fast integer formatting
                    let mut buffer = itoa::Buffer::new();
                    result.push_str(buffer.format(*ts));
                }
                ScalarValue::Utf8(s) => {
                    result.push('"');
                    escape_json_string(s, &mut result);
                    result.push('"');
                }
                ScalarValue::Binary(bytes) => {
                    result.push('"');
                    let encoded = BASE64_STANDARD.encode(bytes);
                    escape_json_string(&encoded, &mut result);
                    result.push('"');
                }
            }
        }

        result.push('}');
        result
    }
}

/// Helper to escape JSON string values
/// Optimized to avoid format! allocations by writing directly to the buffer
#[inline(always)]
fn escape_json_string(s: &str, out: &mut String) {
    // Reserve capacity for worst case (every char needs escaping)
    out.reserve(s.len() * 2);

    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            c if c < '\u{0020}' => {
                // Write unicode escape directly without format! allocation
                out.push_str("\\u");
                // Write hex digits directly (4 hex digits)
                let n = c as u32;
                out.push(char::from_digit((n >> 12) & 0xF, 16).unwrap());
                out.push(char::from_digit((n >> 8) & 0xF, 16).unwrap());
                out.push(char::from_digit((n >> 4) & 0xF, 16).unwrap());
                out.push(char::from_digit(n & 0xF, 16).unwrap());
            }
            c => out.push(c),
        }
    }
}
