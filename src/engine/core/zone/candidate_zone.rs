use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::info;

use crate::engine::core::ZoneMeta;
use crate::engine::core::column::column_values::ColumnValues;
use crate::shared::config::CONFIG;

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateZone {
    pub zone_id: u32,
    pub segment_id: String,
    pub values: HashMap<String, ColumnValues>,
}

impl CandidateZone {
    pub fn new(zone_id: u32, segment_id: String) -> Self {
        Self {
            zone_id,
            segment_id,
            values: HashMap::new(),
        }
    }

    pub fn set_values(&mut self, values: HashMap<String, ColumnValues>) {
        self.values = values;
    }

    pub fn create_all_zones_for_segment(segment_id: &str) -> Vec<Self> {
        let count = CONFIG.engine.fill_factor;
        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::candidate_zone",
                %segment_id,
                zone_count = count,
                "Creating all zones for segment"
            );
        }

        (0..count)
            .map(|zone_id| Self::new(zone_id as u32, segment_id.to_string()))
            .collect()
    }

    /// Prefer reading actual zone count from on-disk metadata; fallback to fill_factor.
    pub fn create_all_zones_for_segment_from_meta(
        segment_base_dir: &Path,
        segment_id: &str,
        uid: &str,
    ) -> Vec<Self> {
        let segment_path = segment_base_dir.join(segment_id);
        let zones_file = segment_path.join(format!("{}.zones", uid));
        match ZoneMeta::load(&zones_file) {
            Ok(metas) => (0..metas.len())
                .map(|z| Self::new(z as u32, segment_id.to_string()))
                .collect(),
            Err(_) => Self::create_all_zones_for_segment(segment_id),
        }
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

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::candidate_zone",
                original = original_len,
                deduplicated = unique.len(),
                "Deduplicated zones"
            );
        }

        unique
    }
}
