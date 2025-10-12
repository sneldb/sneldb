use crate::engine::core::zone::selector::field_selector::FieldSelector;
use crate::engine::core::zone::selector::index_selector::{IndexZoneSelector, MissingIndexPolicy};
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;

pub struct ZoneSelectorBuilder<'a> {
    inputs: SelectionContext<'a>,
}

impl<'a> ZoneSelectorBuilder<'a> {
    pub fn new(inputs: SelectionContext<'a>) -> Self {
        Self { inputs }
    }

    #[inline]
    fn make_artifacts(&self) -> ZoneArtifacts<'a> {
        ZoneArtifacts::new(self.inputs.base_dir, self.inputs.caches)
    }

    #[inline]
    fn make_field_selector(&self, artifacts: ZoneArtifacts<'a>) -> FieldSelector<'a> {
        let range_pruner = RangePruner {
            artifacts: self.make_artifacts(),
        };
        let enum_pruner = EnumPruner {
            artifacts: self.make_artifacts(),
        };
        let xor_pruner = XorPruner { artifacts };
        FieldSelector {
            plan: self.inputs.plan,
            qplan: self.inputs.query_plan,
            range_pruner,
            enum_pruner,
            xor_pruner,
        }
    }

    pub fn build(&self) -> Box<dyn ZoneSelector + 'a> {
        let artifacts = self.make_artifacts();
        match self.inputs.plan.column.as_str() {
            "event_type" => {
                let Some(uid) = self.inputs.plan.uid.as_deref() else {
                    return Box::new(EmptySelector {});
                };
                let Some(event_type) = self.inputs.plan.value.as_ref().and_then(|v| v.as_str())
                else {
                    return Box::new(EmptySelector {});
                };
                let context_id = self
                    .inputs
                    .query_plan
                    .context_id_plan()
                    .and_then(|p| p.value.as_ref().and_then(|v| v.as_str()));
                Box::new(IndexZoneSelector {
                    plan: self.inputs.query_plan,
                    caches: self.inputs.caches,
                    artifacts,
                    policy: MissingIndexPolicy::AllZonesIfNoContext,
                    uid,
                    event_type,
                    context_id,
                })
            }
            "context_id" => {
                let Some(uid) = self.inputs.plan.uid.as_deref() else {
                    return Box::new(AllZonesSelector {});
                };
                let event_type = self
                    .inputs
                    .query_plan
                    .event_type_plan()
                    .and_then(|p| p.value.as_ref().and_then(|v| v.as_str()));
                let context_id = self.inputs.plan.value.as_ref().and_then(|v| v.as_str());
                match event_type {
                    Some(et) => Box::new(IndexZoneSelector {
                        plan: self.inputs.query_plan,
                        caches: self.inputs.caches,
                        artifacts,
                        policy: MissingIndexPolicy::AllZones,
                        uid,
                        event_type: et,
                        context_id,
                    }),
                    None => Box::new(AllZonesSelector {}),
                }
            }
            _ => Box::new(self.make_field_selector(artifacts)),
        }
    }
}

use crate::engine::core::CandidateZone;

struct AllZonesSelector {}
impl ZoneSelector for AllZonesSelector {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        CandidateZone::create_all_zones_for_segment(segment_id)
    }
}

struct EmptySelector {}
impl ZoneSelector for EmptySelector {
    fn select_for_segment(&self, _segment_id: &str) -> Vec<CandidateZone> {
        Vec::new()
    }
}
