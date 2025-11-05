use std::fmt;
use std::str::FromStr;

use arrow_schema::DataType;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Number, Value as JsonValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicalType {
    Null,
    Boolean,
    Integer,
    Float,
    Timestamp,
    String,
    Json,
    Binary,
}

impl LogicalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogicalType::Null => "Null",
            LogicalType::Boolean => "Boolean",
            LogicalType::Integer => "Integer",
            LogicalType::Float => "Float",
            LogicalType::Timestamp => "Timestamp",
            LogicalType::String => "String",
            LogicalType::Json => "JSON",
            LogicalType::Binary => "Binary",
        }
    }

    pub fn to_arrow_data_type(&self) -> DataType {
        match self {
            LogicalType::Null => DataType::Null,
            LogicalType::Boolean => DataType::Boolean,
            LogicalType::Integer => DataType::Int64,
            LogicalType::Float => DataType::Float64,
            LogicalType::Timestamp => {
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
            }
            LogicalType::String | LogicalType::Json => DataType::LargeUtf8,
            LogicalType::Binary => DataType::LargeBinary,
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LogicalType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Boolean" => Ok(LogicalType::Boolean),
            "Integer" | "Number" => Ok(LogicalType::Integer),
            "Float" => Ok(LogicalType::Float),
            "Timestamp" => Ok(LogicalType::Timestamp),
            "String" => Ok(LogicalType::String),
            "JSON" | "Object" | "Array" => Ok(LogicalType::Json),
            "Binary" => Ok(LogicalType::Binary),
            "Null" => Ok(LogicalType::Null),
            _ => Err(()),
        }
    }
}

impl From<&str> for LogicalType {
    fn from(value: &str) -> Self {
        LogicalType::from_str(value).unwrap_or(LogicalType::String)
    }
}

impl From<String> for LogicalType {
    fn from(value: String) -> Self {
        LogicalType::from(value.as_str())
    }
}

impl From<LogicalType> for DataType {
    fn from(value: LogicalType) -> Self {
        value.to_arrow_data_type()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Timestamp(i64),
    Utf8(String),
    Binary(Vec<u8>),
}

impl ScalarValue {
    pub fn logical_type(&self) -> LogicalType {
        match self {
            ScalarValue::Null => LogicalType::Null,
            ScalarValue::Boolean(_) => LogicalType::Boolean,
            ScalarValue::Int64(_) => LogicalType::Integer,
            ScalarValue::Float64(_) => LogicalType::Float,
            ScalarValue::Timestamp(_) => LogicalType::Timestamp,
            ScalarValue::Utf8(_) => LogicalType::String,
            ScalarValue::Binary(_) => LogicalType::Binary,
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            ScalarValue::Null => JsonValue::Null,
            ScalarValue::Boolean(b) => JsonValue::Bool(*b),
            ScalarValue::Int64(i) => JsonValue::Number(Number::from(*i)),
            ScalarValue::Float64(f) => Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            ScalarValue::Timestamp(ts) => JsonValue::Number(Number::from(*ts)),
            ScalarValue::Utf8(s) => {
                // Try to parse as JSON first (handles objects, arrays, etc.)
                // This is important when Utf8 contains serialized JSON (e.g., from json! macro)
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    match &parsed {
                        // Always return parsed objects and arrays
                        JsonValue::Object(_) | JsonValue::Array(_) => return parsed,
                        // For numbers: only parse if it's a large u64 (> i64::MAX)
                        // This handles large u64 values stored as Utf8 strings from EventBuilder
                        // Small numbers stay as strings to preserve aggregate result semantics
                        JsonValue::Number(n) => {
                            if let Some(u) = n.as_u64() {
                                if u > i64::MAX as u64 {
                                    return parsed; // Large u64: return as Number
                                }
                            }
                            // Small number: keep as string
                        }
                        // For other simple values (strings, booleans, null), keep as string
                        _ => {}
                    }
                }
                // Payloads are flat - Utf8 values are always strings, never nested JSON
                JsonValue::String(s.clone())
            }
            ScalarValue::Binary(bytes) => JsonValue::String(BASE64_STANDARD.encode(bytes)),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarValue::Utf8(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ScalarValue::Int64(i) => Some(*i),
            ScalarValue::Timestamp(ts) => Some(*ts),
            ScalarValue::Utf8(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            ScalarValue::Int64(i) if *i >= 0 => Some(*i as u64),
            ScalarValue::Timestamp(ts) if *ts >= 0 => Some(*ts as u64),
            ScalarValue::Utf8(s) => s.parse::<u64>().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Float64(f) => Some(*f),
            ScalarValue::Int64(i) => Some(*i as f64),
            ScalarValue::Timestamp(ts) => Some(*ts as f64),
            ScalarValue::Utf8(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Boolean(b) => Some(*b),
            ScalarValue::Utf8(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            },
            ScalarValue::Int64(i) => Some(*i != 0),
            _ => None,
        }
    }

    /// Convert to string representation directly (no JsonValue intermediate)
    /// This avoids the overhead of converting through JsonValue when we just need a string.
    pub fn to_string_repr(&self) -> String {
        match self {
            ScalarValue::Null => String::new(),
            ScalarValue::Boolean(b) => b.to_string(),
            ScalarValue::Int64(i) => i.to_string(),
            ScalarValue::Float64(f) => f.to_string(),
            ScalarValue::Timestamp(ts) => ts.to_string(),
            ScalarValue::Utf8(s) => s.clone(),
            ScalarValue::Binary(bytes) => BASE64_STANDARD.encode(bytes),
        }
    }

    /// Compare two ScalarValues directly without JSON conversion
    /// This is more efficient than converting to JSON strings for comparison.
    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // Try u64 first (for consistency with existing comparison functions)
        if let (Some(va), Some(vb)) = (self.as_u64(), other.as_u64()) {
            return va.cmp(&vb);
        }
        // Try i64 next (fast path for signed integers)
        if let (Some(va), Some(vb)) = (self.as_i64(), other.as_i64()) {
            return va.cmp(&vb);
        }
        // Try f64 (floating point comparison)
        if let (Some(va), Some(vb)) = (self.as_f64(), other.as_f64()) {
            return va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
        }
        // Try bool
        if let (Some(va), Some(vb)) = (self.as_bool(), other.as_bool()) {
            return va.cmp(&vb);
        }
        // Try string
        if let (Some(va), Some(vb)) = (self.as_str(), other.as_str()) {
            return va.cmp(vb);
        }
        // Fallback: convert to string representation (but more efficiently than via JSON)
        self.to_string_repr().cmp(&other.to_string_repr())
    }
}

impl From<JsonValue> for ScalarValue {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => ScalarValue::Null,
            JsonValue::Bool(b) => ScalarValue::Boolean(b),
            JsonValue::Number(num) => {
                if let Some(i) = num.as_i64() {
                    ScalarValue::Int64(i)
                } else if let Some(u) = num.as_u64() {
                    // Convert u64 to i64, but if it's too large, use Utf8 string representation
                    if u <= i64::MAX as u64 {
                        ScalarValue::Int64(u as i64)
                    } else {
                        ScalarValue::Utf8(u.to_string())
                    }
                } else if let Some(f) = num.as_f64() {
                    ScalarValue::Float64(f)
                } else {
                    // Fallback: convert number to string
                    ScalarValue::Utf8(num.to_string())
                }
            }
            JsonValue::String(s) => ScalarValue::Utf8(s),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                // Serialize complex JSON to string
                ScalarValue::Utf8(
                    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string()),
                )
            }
        }
    }
}

impl From<&ScalarValue> for JsonValue {
    fn from(value: &ScalarValue) -> Self {
        value.to_json()
    }
}

impl Serialize for ScalarValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ScalarValue::Null => serializer.serialize_unit(),
            ScalarValue::Boolean(b) => serializer.serialize_bool(*b),
            ScalarValue::Int64(i) => serializer.serialize_i64(*i),
            ScalarValue::Float64(f) => serializer.serialize_f64(*f),
            ScalarValue::Timestamp(ts) => serializer.serialize_i64(*ts),
            ScalarValue::Utf8(s) => {
                // Payloads are flat - Utf8 values are always strings
                serializer.serialize_str(s)
            }
            ScalarValue::Binary(bytes) => serializer.serialize_str(&BASE64_STANDARD.encode(bytes)),
        }
    }
}

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json = JsonValue::deserialize(deserializer)?;
        Ok(ScalarValue::from(json))
    }
}
