use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::materialization_pruner::MaterializationPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::temporal_pruner::TemporalPruner;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::{CandidateZone, FilterPlan, QueryCaches, QueryPlan};

pub struct FieldSelector<'a> {
    pub plan: &'a FilterPlan,
    pub qplan: &'a QueryPlan,
    pub caches: Option<&'a QueryCaches>,
    pub range_pruner: RangePruner<'a>,
    pub temporal_pruner: TemporalPruner<'a>,
    pub enum_pruner: EnumPruner<'a>,
    pub xor_pruner: XorPruner<'a>,
}

impl<'a> FieldSelector<'a> {}

impl<'a> ZoneSelector for FieldSelector<'a> {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        let Some(uid) = &self.plan.uid else {
            return Vec::new();
        };
        if self.plan.value.is_none() {
            if let Some(uid) = &self.plan.uid {
                return CandidateZone::create_all_zones_for_segment_from_meta_cached(
                    &self.qplan.segment_base_dir,
                    segment_id,
                    uid,
                    self.caches,
                );
            }
            return CandidateZone::create_all_zones_for_segment(segment_id);
        }

        let args = PruneArgs {
            segment_id,
            uid,
            column: &self.plan.column,
            value: self.plan.value.as_ref(),
            op: self.plan.operation.as_ref(),
        };

        let mut candidate_zones = Vec::new();

        // If a strategy is assigned, dispatch directly to the corresponding executor
        if let Some(strategy) = &self.plan.index_strategy {
            match strategy {
                IndexStrategy::TemporalEq { .. } | IndexStrategy::TemporalRange { .. } => {
                    if let Some(z) = self.temporal_pruner.apply_temporal_only(&args) {
                        candidate_zones = z;
                    } else {
                        return Vec::new();
                    }
                }
                IndexStrategy::EnumBitmap { .. } => {
                    if let Some(z) = self.enum_pruner.apply(&args) {
                        candidate_zones = z;
                    } else {
                        return Vec::new();
                    }
                }
                IndexStrategy::ZoneSuRF { .. } => {
                    if let Some(z) = self.range_pruner.apply_surf_only(&args) {
                        candidate_zones = z;
                    } else {
                        return Vec::new();
                    }
                }
                IndexStrategy::ZoneXorIndex { .. } => {
                    if let Some(z) = self.xor_pruner.apply_zone_index_only(&args) {
                        candidate_zones = z;
                    } else {
                        return Vec::new();
                    }
                }
                IndexStrategy::XorPresence { .. } => {
                    if let Some(z) = self.xor_pruner.apply_presence_only(&args) {
                        candidate_zones = z;
                    } else {
                        return Vec::new();
                    }
                }
                IndexStrategy::FullScan => {
                    // For FullScan, return all zones (will be filtered by materialization pruner if needed)
                    if let Some(uid) = &self.plan.uid {
                        candidate_zones =
                            CandidateZone::create_all_zones_for_segment_from_meta_cached(
                                &self.qplan.segment_base_dir,
                                segment_id,
                                uid,
                                self.caches,
                            );
                    } else {
                        candidate_zones = CandidateZone::create_all_zones_for_segment(segment_id);
                    }
                }
            }
        } else {
            // No explicit strategy, return empty
            return Vec::new();
        }

        // Apply materialization pruning if materialization_created_at is set in query metadata
        if let Some(uid) = &self.plan.uid {
            if let Some(created_at_str) = self.qplan.metadata.get("materialization_created_at") {
                if let Ok(materialization_created_at) = created_at_str.parse::<u64>() {
                    let pruner = MaterializationPruner::new(
                        &self.qplan.segment_base_dir,
                        self.caches,
                        materialization_created_at,
                    );
                    candidate_zones = pruner.apply(&candidate_zones, uid);
                }
            }
        }

        candidate_zones
    }
}
