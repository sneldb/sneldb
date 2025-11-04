use crate::engine::core::{EventId, ZoneCursor, ZoneRow};
use serde_json::Value;
use std::collections::HashMap;

pub struct ZoneCursorFactory {
    segment_id: u64,
    zone_id: u32,
    rows: Vec<ZoneRow>,
}

impl ZoneCursorFactory {
    pub fn new() -> Self {
        Self {
            segment_id: 42,
            zone_id: 0,
            rows: Vec::new(),
        }
    }

    pub fn with_segment_id(mut self, segment_id: u64) -> Self {
        self.segment_id = segment_id;
        self
    }

    pub fn with_zone_id(mut self, zone_id: u32) -> Self {
        self.zone_id = zone_id;
        self
    }

    pub fn with_rows(mut self, rows: Vec<ZoneRow>) -> Self {
        self.rows = rows;
        self
    }

    pub fn create(self) -> ZoneCursor {
        let mut context_ids = Vec::new();
        let mut timestamps = Vec::new();
        let mut event_types = Vec::new();
        let mut event_ids = Vec::new();
        let mut payload_fields: HashMap<String, Vec<Value>> = HashMap::new();

        for row in &self.rows {
            context_ids.push(row.context_id.clone());
            timestamps.push(row.timestamp.clone());
            event_types.push(row.event_type.clone());
            event_ids.push(row.event_id);

            for (key, value) in &row.payload {
                payload_fields
                    .entry(key.clone())
                    .or_default()
                    .push(value.clone());
            }
        }

        ZoneCursor {
            segment_id: self.segment_id,
            zone_id: self.zone_id,
            context_ids,
            timestamps,
            event_types,
            event_ids,
            payload_fields,
            pos: 0,
            created_at: 0,
        }
    }
}
