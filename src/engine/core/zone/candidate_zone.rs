use std::collections::{HashMap, HashSet};
use tracing::info;

use crate::shared::config::CONFIG;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateZone {
    pub zone_id: u32,
    pub segment_id: String,
    pub values: HashMap<String, Vec<String>>,
}

impl CandidateZone {
    pub fn new(zone_id: u32, segment_id: String) -> Self {
        Self {
            zone_id,
            segment_id,
            values: HashMap::new(),
        }
    }

    pub fn set_values(&mut self, values: HashMap<String, Vec<String>>) {
        self.values = values;
    }

    pub fn create_all_zones_for_segment(segment_id: &str) -> Vec<Self> {
        let count = CONFIG.engine.fill_factor();
        info!(
            target: "sneldb::candidate_zone",
            %segment_id,
            zone_count = count,
            "Creating all zones for segment"
        );

        (0..count)
            .map(|zone_id| Self::new(zone_id as u32, segment_id.to_string()))
            .collect()
    }

    pub fn uniq(zones: Vec<Self>) -> Vec<Self> {
        let original_len = zones.len();
        let mut seen = HashSet::with_capacity(original_len);
        let mut unique = Vec::with_capacity(original_len);

        for zone in zones {
            let key = (zone.zone_id, zone.segment_id.clone());
            if seen.insert(key) {
                unique.push(zone);
            }
        }

        info!(
            target: "sneldb::candidate_zone",
            original = original_len,
            deduplicated = unique.len(),
            "Deduplicated zones"
        );

        unique
    }
}
