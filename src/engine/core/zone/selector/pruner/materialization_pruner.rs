use crate::engine::core::CandidateZone;
use crate::engine::core::read::cache::QueryCaches;
use crate::engine::core::zone::zone_meta::ZoneMeta;
use std::path::PathBuf;
use tracing::debug;

/// Pruner that filters zones based on materialization creation time.
/// Skips zones that were created before or at the materialization creation time,
/// as those zones have already been processed by the materialization.
pub struct MaterializationPruner<'a> {
    base_dir: &'a PathBuf,
    caches: Option<&'a QueryCaches>,
    materialization_created_at: u64,
}

impl<'a> MaterializationPruner<'a> {
    pub fn new(
        base_dir: &'a PathBuf,
        caches: Option<&'a QueryCaches>,
        materialization_created_at: u64,
    ) -> Self {
        Self {
            base_dir,
            caches,
            materialization_created_at,
        }
    }

    /// Filters candidate zones, removing those created at or before materialization creation time.
    /// Returns the filtered list of zones that were created after the materialization.
    pub fn apply(&self, zones: &[CandidateZone], uid: &str) -> Vec<CandidateZone> {
        if zones.is_empty() {
            return Vec::new();
        }

        // Only log start if there are zones to process (reduce noise)
        if !zones.is_empty() {
            debug!(
                target: "sneldb::materialization_pruner",
                input_zones = zones.len(),
                materialization_created_at = self.materialization_created_at,
                uid = %uid,
                "Starting materialization pruning"
            );
        }

        // Load zone metadata and filter in a single pass for efficiency
        let mut zone_meta_by_segment: std::collections::HashMap<String, Vec<ZoneMeta>> =
            std::collections::HashMap::new();
        let mut filtered = Vec::new();

        for zone in zones {
            // Load metadata for this segment if not already loaded
            if !zone_meta_by_segment.contains_key(&zone.segment_id) {
                let segment_dir = self.base_dir.join(&zone.segment_id);
                let zones_path = segment_dir.join(format!("{}.zones", uid));

                // Try cache first
                let meta_result = if let Some(caches) = self.caches {
                    caches
                        .get_or_load_zone_meta(&zone.segment_id, uid)
                        .map(|arc| (*arc).clone())
                        .map_err(|e| format!("{:?}", e))
                } else {
                    ZoneMeta::load(&zones_path).map_err(|e| format!("{:?}", e))
                };

                match meta_result {
                    Ok(metas) => {
                        zone_meta_by_segment.insert(zone.segment_id.clone(), metas);
                    }
                    Err(e) => {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(
                                target: "sneldb::materialization_pruner",
                                segment_id = %zone.segment_id,
                                uid = %uid,
                                error = %e,
                                "Failed to load zone metadata, skipping filter for this segment (fail open)"
                            );
                        }
                        // If we can't load metadata, include all zones from this segment
                        // (fail open to avoid false negatives)
                        filtered.push(zone.clone());
                        continue;
                    }
                }
            }

            // Filter zones: only include those with created_at > materialization_created_at
            if let Some(metas) = zone_meta_by_segment.get(&zone.segment_id) {
                if let Some(meta) = metas.get(zone.zone_id as usize) {
                    if meta.created_at > self.materialization_created_at {
                        filtered.push(zone.clone());
                    } else {
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            debug!(
                                target: "sneldb::materialization_pruner",
                                segment_id = %zone.segment_id,
                                zone_id = zone.zone_id,
                                zone_created_at = meta.created_at,
                                materialization_created_at = self.materialization_created_at,
                                "Filtered out zone (already materialized)"
                            );
                        }
                    }
                } else {
                    // Zone ID out of bounds - include it (fail open)
                    filtered.push(zone.clone());
                }
            } else {
                // Segment metadata not loaded - include it (fail open)
                filtered.push(zone.clone());
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                target: "sneldb::materialization_pruner",
                input_zones = zones.len(),
                filtered_zones = filtered.len(),
                materialization_created_at = self.materialization_created_at,
                "Materialization pruning applied"
            );
        }

        filtered
    }
}
