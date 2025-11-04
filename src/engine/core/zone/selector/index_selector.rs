use crate::engine::core::zone::selector::pruner::materialization_pruner::MaterializationPruner;
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};

#[derive(Debug)]
pub enum MissingIndexPolicy {
    AllZonesIfNoContext,
    AllZones,
    Empty,
}

pub struct IndexZoneSelector<'a> {
    pub plan: &'a QueryPlan,
    pub caches: Option<&'a QueryCaches>,
    pub artifacts: ZoneArtifacts<'a>,
    pub policy: MissingIndexPolicy,
    pub uid: &'a str,
    pub event_type: &'a str,
    pub context_id: Option<&'a str>,
}

impl<'a> ZoneSelector for IndexZoneSelector<'a> {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        // Early materialization check: if all zones would be filtered, skip expensive index loading
        if let Some(created_at_str) = self.plan.metadata.get("materialization_created_at") {
            if let Ok(materialization_created_at) = created_at_str.parse::<u64>() {
                let zones_path = self
                    .plan
                    .segment_base_dir
                    .join(segment_id)
                    .join(format!("{}.zones", self.uid));

                // Fast path: Check file modification time first (much faster than loading metadata)
                // If the file was last modified before materialization creation, all zones are old
                // Note: materialization_created_at is in seconds (from now())
                if let Ok(metadata) = std::fs::metadata(&zones_path) {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                            let file_mtime_sec = duration.as_secs();

                            // If file was modified before materialization was created, all zones are old
                            // Add 1 second tolerance to account for potential file system timestamp precision
                            if file_mtime_sec < materialization_created_at.saturating_sub(1) {
                                // File is older than materialization - skip loading metadata entirely
                                return Vec::new();
                            }
                        }
                    }
                }

                // File is newer or we can't check mtime - load metadata to verify
                let zone_metas_result = if let Some(caches) = self.caches {
                    caches
                        .get_or_load_zone_meta(segment_id, self.uid)
                        .map(|arc| (*arc).clone())
                        .map_err(|e| format!("{:?}", e))
                } else {
                    crate::engine::core::zone::zone_meta::ZoneMeta::load(&zones_path)
                        .map_err(|e| format!("{:?}", e))
                };

                if let Ok(zone_metas) = zone_metas_result {
                    // Check if ALL zones would be filtered out
                    let all_filtered = zone_metas
                        .iter()
                        .all(|meta| meta.created_at <= materialization_created_at);
                    if all_filtered {
                        // All zones would be filtered - skip expensive index loading
                        return Vec::new();
                    }
                }
            }
        }

        let mut candidate_zones = match self.artifacts.load_zone_index(segment_id, self.uid) {
            Ok(index) => index.find_candidate_zones(self.event_type, self.context_id, segment_id),
            Err(err) => {
                tracing::error!(target: "sneldb::query", %segment_id, uid = %self.uid, error = %err, policy = ?self.policy, context_id = ?self.context_id, "Failed to load ZoneIndex; applying missing-index policy");
                match self.policy {
                    MissingIndexPolicy::AllZonesIfNoContext => match self.context_id {
                        Some(_) => vec![],
                        None => CandidateZone::create_all_zones_for_segment_from_meta_cached(
                            &self.plan.segment_base_dir,
                            segment_id,
                            self.uid,
                            self.caches,
                        ),
                    },
                    MissingIndexPolicy::AllZones => {
                        CandidateZone::create_all_zones_for_segment_from_meta_cached(
                            &self.plan.segment_base_dir,
                            segment_id,
                            self.uid,
                            self.caches,
                        )
                    }
                    MissingIndexPolicy::Empty => Vec::new(),
                }
            }
        };

        // Apply materialization pruning if materialization_created_at is set in query metadata
        if let Some(created_at_str) = self.plan.metadata.get("materialization_created_at") {
            if let Ok(materialization_created_at) = created_at_str.parse::<u64>() {
                let pruner = MaterializationPruner::new(
                    &self.plan.segment_base_dir,
                    self.caches,
                    materialization_created_at,
                );
                candidate_zones = pruner.apply(&candidate_zones, self.uid);
            }
        }

        candidate_zones
    }
}
