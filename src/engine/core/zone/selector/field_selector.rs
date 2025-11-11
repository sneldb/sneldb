use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::materialization_pruner::MaterializationPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::temporal_pruner::TemporalPruner;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};
use tracing::warn;

pub struct FieldSelector<'a> {
    pub plan: &'a FilterGroup,
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
        let (uid, column, value, operation, index_strategy) = match self.plan {
            FilterGroup::Filter {
                uid,
                column,
                value,
                operation,
                index_strategy,
                ..
            } => (
                uid.as_ref(),
                column.as_str(),
                value.as_ref(),
                operation.as_ref(),
                index_strategy.as_ref(),
            ),
            _ => return Vec::new(), // Only single filters supported
        };

        let Some(uid) = uid else {
            return Vec::new();
        };
        if value.is_none() {
            return CandidateZone::create_all_zones_for_segment_from_meta_cached(
                &self.qplan.segment_base_dir,
                segment_id,
                uid,
                self.caches,
            );
        }

        let args = PruneArgs {
            segment_id,
            uid,
            column,
            value: value.map(|v| v),
            op: operation,
        };

        let mut candidate_zones = Vec::new();

        // If a strategy is assigned, dispatch directly to the corresponding executor
        if let Some(strategy) = index_strategy {
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
                        // Fallback to FullScan if ZoneSuRF fails to load or matched >90% of zones
                        // The specific reason is already logged in range_pruner.rs
                        candidate_zones =
                            CandidateZone::create_all_zones_for_segment_from_meta_cached(
                                &self.qplan.segment_base_dir,
                                args.segment_id,
                                uid,
                                self.caches,
                            );
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
                    candidate_zones = CandidateZone::create_all_zones_for_segment_from_meta_cached(
                        &self.qplan.segment_base_dir,
                        segment_id,
                        uid,
                        self.caches,
                    );
                }
            }
        } else {
            // No explicit strategy, return empty
            return Vec::new();
        }

        // Apply materialization pruning if materialization_created_at is set in query metadata
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

        candidate_zones
    }
}
