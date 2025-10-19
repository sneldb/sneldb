use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::temporal_pruner::TemporalPruner;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::selector::pruner::{PruneArgs, ZonePruner};
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::{CandidateZone, FilterPlan, QueryPlan};

pub struct FieldSelector<'a> {
    pub plan: &'a FilterPlan,
    pub qplan: &'a QueryPlan,
    pub range_pruner: RangePruner<'a>,
    pub temporal_pruner: TemporalPruner<'a>,
    pub enum_pruner: EnumPruner<'a>,
    pub xor_pruner: XorPruner<'a>,
}

impl<'a> FieldSelector<'a> {
    #[inline]
    fn is_enum_field(&self, uid: &str, column: &str) -> bool {
        self.qplan
            .registry
            .try_read()
            .map(|reg| reg.is_enum_field_by_uid(uid, column))
            .unwrap_or(false)
    }
}

impl<'a> ZoneSelector for FieldSelector<'a> {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        let Some(uid) = &self.plan.uid else {
            return Vec::new();
        };
        if self.plan.value.is_none() {
            if let Some(uid) = &self.plan.uid {
                return CandidateZone::create_all_zones_for_segment_from_meta(
                    &self.qplan.segment_base_dir,
                    segment_id,
                    uid,
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

        // Time-first pruning
        if let Some(z) = self.temporal_pruner.apply(&args) {
            return z;
        }
        if let Some(z) = self.range_pruner.apply(&args) {
            return z;
        }
        if self.is_enum_field(uid, &self.plan.column) {
            if let Some(z) = self.enum_pruner.apply(&args) {
                return z;
            }
        }
        if let Some(z) = self.xor_pruner.apply(&args) {
            return z;
        }

        Vec::new()
    }
}
