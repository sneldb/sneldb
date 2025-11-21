use crate::engine::core::{Event, EventId};
use crate::engine::types::ScalarValue;
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub timestamp: u64,
    pub context_id: String,
    pub event_type: String,
    #[serde(serialize_with = "serialize_payload", deserialize_with = "deserialize_payload")]
    pub payload: HashMap<Arc<str>, ScalarValue>,
    #[serde(default)]
    pub event_id: EventId,
}

fn serialize_payload<S>(
    payload: &HashMap<Arc<str>, ScalarValue>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(payload.len()))?;
    for (k, v) in payload {
        map.serialize_entry(k.as_ref(), v)?;
    }
    map.end()
}

fn deserialize_payload<'de, D>(
    deserializer: D,
) -> Result<HashMap<Arc<str>, ScalarValue>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PayloadVisitor;

    impl<'de> Visitor<'de> for PayloadVisitor {
        type Value = HashMap<Arc<str>, ScalarValue>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((k, v)) = access.next_entry::<String, ScalarValue>()? {
                map.insert(Arc::from(k), v);
            }
            Ok(map)
        }
    }

    deserializer.deserialize_map(PayloadVisitor)
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
            map.insert(k.as_ref().to_string(), v.to_json());
        }
        serde_json::Value::Object(map)
    }

    pub fn set_payload_json(&mut self, value: serde_json::Value) {
        self.payload = match value {
            serde_json::Value::Object(map) => {
                let mut out = HashMap::with_capacity(map.len());
                for (k, v) in map.into_iter() {
                    out.insert(Arc::from(k), ScalarValue::from(v));
                }
                out
            }
            _ => HashMap::new(),
        };
    }
}
