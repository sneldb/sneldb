use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::{CandidateZone, ColumnValues};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

pub struct CandidateZoneFactory {
    params: HashMap<String, Value>,
    values: Option<HashMap<String, Vec<String>>>,
}

impl CandidateZoneFactory {
    pub fn new() -> Self {
        let mut params = HashMap::new();
        params.insert("zone_id".into(), json!(0));
        params.insert("segment_id".into(), json!("segment-0001"));
        Self {
            params,
            values: None,
        }
    }

    pub fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.params.insert(key.to_string(), value.into());
        self
    }

    pub fn with_values(mut self, values: HashMap<String, Vec<String>>) -> Self {
        self.values = Some(values);
        self
    }

    pub fn create(self) -> CandidateZone {
        let zone_id = self
            .params
            .get("zone_id")
            .and_then(|v| v.as_u64())
            .expect("zone_id is missing or not a u64");

        let segment_id = self
            .params
            .get("segment_id")
            .and_then(|v| v.as_str())
            .expect("segment_id is missing or not a string");

        let mut zone = CandidateZone::new(zone_id as u32, segment_id.to_string());

        if let Some(values) = self.values {
            // Convert Vec<String> values to zero-copy ColumnValues for tests
            let mut conv: HashMap<String, ColumnValues> = HashMap::new();
            for (k, vec_str) in values.into_iter() {
                let mut bytes: Vec<u8> = Vec::new();
                let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(vec_str.len());
                for s in vec_str {
                    let start = bytes.len();
                    bytes.extend_from_slice(s.as_bytes());
                    let len = s.as_bytes().len();
                    ranges.push((start, len));
                }
                let block = Arc::new(DecompressedBlock::from_bytes(bytes));
                conv.insert(k, ColumnValues::new(block, ranges));
            }
            zone.set_values(conv);
        }

        zone
    }
}
