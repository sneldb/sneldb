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
        match self.artifacts.load_zone_index(segment_id, self.uid) {
            Ok(index) => index.find_candidate_zones(self.event_type, self.context_id, segment_id),
            Err(err) => {
                tracing::error!(target: "sneldb::query", %segment_id, uid = %self.uid, error = %err, policy = ?self.policy, context_id = ?self.context_id, "Failed to load ZoneIndex; applying missing-index policy");
                match self.policy {
                    MissingIndexPolicy::AllZonesIfNoContext => match self.context_id {
                        Some(_) => vec![],
                        None => CandidateZone::create_all_zones_for_segment_from_meta(
                            &self.plan.segment_base_dir,
                            segment_id,
                            self.uid,
                        ),
                    },
                    MissingIndexPolicy::AllZones => {
                        CandidateZone::create_all_zones_for_segment_from_meta(
                            &self.plan.segment_base_dir,
                            segment_id,
                            self.uid,
                        )
                    }
                    MissingIndexPolicy::Empty => Vec::new(),
                }
            }
        }
    }
}
