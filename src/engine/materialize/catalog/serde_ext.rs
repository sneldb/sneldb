use crate::command::types::MaterializedQuerySpec;
use serde::{Deserialize, Deserializer, Serializer};

pub mod materialized_query_spec {
    use super::*;

    pub fn serialize<S>(spec: &MaterializedQuerySpec, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let json = serde_json::to_string(spec).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&json)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<MaterializedQuerySpec, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        serde_json::from_str(&s).map_err(serde::de::Error::custom)
    }
}
