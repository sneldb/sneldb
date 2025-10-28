use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::info;

use crate::engine::core::ZoneMeta;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::read::cache::QueryCaches;
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

    /// Cached variant: consult per-query caches to avoid repeated .zones disk loads.
    pub fn create_all_zones_for_segment_from_meta_cached(
        segment_base_dir: &Path,
        segment_id: &str,
        uid: &str,
        caches: Option<&QueryCaches>,
    ) -> Vec<Self> {
        if let Some(c) = caches {
            if let Ok(metas_arc) = c.get_or_load_zone_meta(segment_id, uid) {
                let count = metas_arc.len();
                return (0..count)
                    .map(|z| Self::new(z as u32, segment_id.to_string()))
                    .collect();
            }
        }
        // Fallback to direct load or fill_factor path
        Self::create_all_zones_for_segment_from_meta(segment_base_dir, segment_id, uid)
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
