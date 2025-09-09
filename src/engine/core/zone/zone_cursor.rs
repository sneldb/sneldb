use crate::engine::core::ZoneRow;
use std::collections::HashMap;
use tracing::trace;

/// Represents one zone of one segment for a given event_type UID
#[derive(Debug, Clone)]
pub struct ZoneCursor {
    pub segment_id: u64,
    pub zone_id: u32,

    /// Data loaded from context_id.col, must be sorted
    pub context_ids: Vec<String>,

    /// Fixed columns
    pub timestamps: Vec<String>,
    pub event_types: Vec<String>, // redundant but aligns structure

    /// All payload fields (e.g., {"plan": ["free", "pro", ...]})
    pub payload_fields: HashMap<String, Vec<String>>,

    /// Current position inside this zone
    pub pos: usize,
}

impl ZoneCursor {
    pub fn len(&self) -> usize {
        self.context_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.context_ids.is_empty()
    }

    /// Peek the current context_id without advancing
    pub fn peek_context_id(&self) -> Option<&str> {
        let id = self.context_ids.get(self.pos).map(String::as_str);
        trace!(
            target: "sneldb::cursor",
            segment_id = self.segment_id,
            zone_id = self.zone_id,
            pos = self.pos,
            peek = ?id,
            "Peeking context_id"
        );
        id
    }

    /// Advance and return full row
    pub fn next_row(&mut self) -> Option<ZoneRow> {
        if self.pos >= self.context_ids.len() {
            trace!(
                target: "sneldb::cursor",
                segment_id = self.segment_id,
                zone_id = self.zone_id,
                pos = self.pos,
                len = self.context_ids.len(),
                "End of cursor reached"
            );
            return None;
        }

        let idx = self.pos;
        self.pos += 1;

        let payload: HashMap<String, String> = self
            .payload_fields
            .iter()
            .map(|(k, v)| (k.clone(), v[idx].clone()))
            .collect();

        trace!(
            target: "sneldb::cursor",
            segment_id = self.segment_id,
            zone_id = self.zone_id,
            row_idx = idx,
            "Returning next row"
        );

        Some(ZoneRow {
            segment_id: self.segment_id,
            zone_id: self.zone_id,
            context_id: self.context_ids[idx].clone(),
            timestamp: self.timestamps[idx].clone(),
            event_type: self.event_types[idx].clone(),
            payload,
        })
    }
}
