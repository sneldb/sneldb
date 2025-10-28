use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
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

        // If a strategy is assigned, dispatch directly to the corresponding executor
        if let Some(strategy) = &self.plan.index_strategy {
            match strategy {
                IndexStrategy::TemporalEq { .. } | IndexStrategy::TemporalRange { .. } => {
                    if let Some(z) = self.temporal_pruner.apply_temporal_only(&args) {
                        return z;
                    }
                }
                IndexStrategy::EnumBitmap { .. } => {
                    if let Some(z) = self.enum_pruner.apply(&args) {
                        return z;
                    }
                }
                IndexStrategy::ZoneSuRF { .. } => {
                    if let Some(z) = self.range_pruner.apply_surf_only(&args) {
                        return z;
                    }
                }
                IndexStrategy::ZoneXorIndex { .. } => {
                    if let Some(z) = self.xor_pruner.apply_zone_index_only(&args) {
                        return z;
                    }
                }
                IndexStrategy::XorPresence { .. } => {
                    if let Some(z) = self.xor_pruner.apply_presence_only(&args) {
                        return z;
                    }
                }
                IndexStrategy::FullScan => {}
            }
            // If the chosen index failed to produce candidates, fall through to empty (or optional fallback path below)
            return Vec::new();
        }

        // No legacy path: if no explicit strategy, return empty
        Vec::new()
    }
}
