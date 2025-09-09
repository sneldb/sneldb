use std::collections::HashMap;

/// Represents one reconstructed row from a zone
#[derive(Debug, Clone)]
pub struct ZoneRow {
    pub segment_id: u64,
    pub zone_id: u32,
    pub context_id: String,
    pub timestamp: String,
    pub event_type: String,
    pub payload: HashMap<String, String>,
}
