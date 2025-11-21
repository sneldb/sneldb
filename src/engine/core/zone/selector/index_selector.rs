use crate::engine::core::zone::selector::pruner::materialization_pruner::MaterializationPruner;
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::{zone_artifacts::ZoneArtifacts, zone_meta::ZoneMeta};
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};
use std::path::Path;
use std::time::UNIX_EPOCH;
use tracing::{debug, error};

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
        let materialization_guard = MaterializationGuard::from_plan(self.plan);
        if let Some(guard) = &materialization_guard {
            let zones_path = self
                .plan
                .segment_base_dir
                .join(segment_id)
                .join(format!("{}.zones", self.uid));

            if guard.file_definitely_stale(&zones_path) {
                return Vec::new();
            }

            let zone_metas_result = if let Some(caches) = self.caches {
                caches
                    .get_or_load_zone_meta(segment_id, self.uid)
                    .map(|arc| (*arc).clone())
                    .map_err(|e| format!("{:?}", e))
            } else {
                ZoneMeta::load(&zones_path).map_err(|e| format!("{:?}", e))
            };

            if let Ok(zone_metas) = zone_metas_result {
                if guard.segment_fully_materialized(&zone_metas) {
                    return Vec::new();
                }
            }
        }

        let mut candidate_zones = match self.artifacts.load_zone_index(segment_id, self.uid) {
            Ok(index) => index.find_candidate_zones(self.event_type, self.context_id, segment_id),
            Err(err) => {
                if !self.plan.segment_maybe_contains_uid(segment_id, self.uid) {
                    debug!(
                        target: "sneldb::query",
                        %segment_id,
                        uid = %self.uid,
                        error = %err,
                        "Zone index missing for unrelated segment; skipping"
                    );
                    return Vec::new();
                }

                error!(
                    target: "sneldb::query",
                    %segment_id,
                    uid = %self.uid,
                    error = %err,
                    policy = ?self.policy,
                    context_id = ?self.context_id,
                    "Failed to load ZoneIndex; applying missing-index policy"
                );
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

        // Apply materialization pruning once zones are materialized
        if let Some(guard) = &materialization_guard {
            let pruner = MaterializationPruner::new(
                &self.plan.segment_base_dir,
                self.caches,
                guard.created_at(),
                guard.high_water_ts(),
            );
            candidate_zones = pruner.apply(&candidate_zones, self.uid);
        }

        candidate_zones
    }
}

#[derive(Clone, Copy, Debug)]
struct MaterializationGuard {
    created_at: u64,
    high_water_ts: Option<u64>,
}

impl MaterializationGuard {
    fn from_plan(plan: &QueryPlan) -> Option<Self> {
        let created_at_str = plan.metadata.get("materialization_created_at")?;
        let created_at = match created_at_str.parse::<u64>() {
            Ok(value) => value,
            Err(err) => {
                tracing::debug!(
                    target: "sneldb::materialization_pruner",
                    error = %err,
                    "Unable to parse materialization_created_at metadata"
                );
                return None;
            }
        };

        let high_water_ts = match plan.metadata.get("materialization_high_water_ts") {
            Some(value) => match value.parse::<u64>() {
                Ok(ts) => Some(ts),
                Err(err) => {
                    tracing::debug!(
                        target: "sneldb::materialization_pruner",
                        error = %err,
                        "Unable to parse materialization_high_water_ts metadata; falling back to created_at comparison"
                    );
                    None
                }
            },
            None => None,
        };

        Some(Self {
            created_at,
            high_water_ts,
        })
    }

    fn created_at(&self) -> u64 {
        self.created_at
    }

    fn high_water_ts(&self) -> Option<u64> {
        self.high_water_ts
    }

    fn file_definitely_stale(&self, zones_path: &Path) -> bool {
        if let Ok(metadata) = std::fs::metadata(zones_path) {
            if let Ok(modified) = metadata.modified() {
                if let Ok(duration) = modified.duration_since(UNIX_EPOCH) {
                    let cutoff = self.high_water_ts.unwrap_or(self.created_at);
                    return duration.as_secs() < cutoff.saturating_sub(1);
                }
            }
        }
        false
    }

    fn segment_fully_materialized(&self, metas: &[ZoneMeta]) -> bool {
        if metas.is_empty() {
            return false;
        }

        if let Some(high_water) = self.high_water_ts {
            metas.iter().all(|meta| meta.timestamp_max < high_water)
        } else {
            metas.iter().all(|meta| meta.created_at <= self.created_at)
        }
    }
}
