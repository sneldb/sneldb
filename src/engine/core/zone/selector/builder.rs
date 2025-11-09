use crate::engine::core::zone::selector::field_selector::FieldSelector;
use crate::engine::core::zone::selector::index_selector::{IndexZoneSelector, MissingIndexPolicy};
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::temporal_pruner::TemporalPruner;
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
        let temporal_pruner = TemporalPruner {
            artifacts: self.make_artifacts(),
        };
        let enum_pruner = EnumPruner {
            artifacts: self.make_artifacts(),
        };
        let xor_pruner = XorPruner { artifacts };
        FieldSelector {
            plan: self.inputs.plan,
            qplan: self.inputs.query_plan,
            caches: self.inputs.caches,
            range_pruner,
            temporal_pruner,
            enum_pruner,
            xor_pruner,
        }
    }

    pub fn build(&self) -> Box<dyn ZoneSelector + 'a> {
        let artifacts = self.make_artifacts();
        let column = self.inputs.plan.column().unwrap_or("");
        match column {
            "event_type" => {
                let uid = self.inputs.plan.value().and_then(|v| v.as_str()).and_then(|_| {
                    // Get uid from FilterGroup
                    match self.inputs.plan {
                        crate::engine::core::filter::filter_group::FilterGroup::Filter { uid, .. } => uid.as_deref(),
                        _ => None,
                    }
                });
                let Some(uid) = uid else {
                    return Box::new(EmptySelector {});
                };
                let Some(event_type) = self.inputs.plan.value().and_then(|v| v.as_str()) else {
                    return Box::new(EmptySelector {});
                };
                let context_id = self
                    .inputs
                    .query_plan
                    .context_id_plan()
                    .and_then(|p| p.value().and_then(|v| v.as_str()));
                // If no catalog for this segment, disable index usage to avoid fs probing; use AllZones policy
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
                let event_type = self
                    .inputs
                    .query_plan
                    .event_type_plan()
                    .and_then(|p| p.value().and_then(|v| v.as_str()));
                let context_id = self.inputs.plan.value().and_then(|v| v.as_str());
                let uid = match self.inputs.plan {
                    crate::engine::core::filter::filter_group::FilterGroup::Filter { uid, .. } => uid.as_deref(),
                    _ => None,
                };

                match (uid, event_type) {
                    (Some(uid), Some(et)) => Box::new(IndexZoneSelector {
                        plan: self.inputs.query_plan,
                        caches: self.inputs.caches,
                        artifacts,
                        policy: MissingIndexPolicy::AllZones,
                        uid,
                        event_type: et,
                        context_id,
                    }),
                    (None, Some(et)) => {
                        // Resolve uid from registry for event_type and return all zones via metadata
                        if let Ok(reg) = self.inputs.query_plan.registry.try_read() {
                            if let Some(uid) = reg.get_uid(et) {
                                return Box::new(AllZonesSelectorMeta {
                                    base_dir: self.inputs.base_dir,
                                    uid: uid.to_string(),
                                });
                            }
                        }
                        Box::new(AllZonesSelector {})
                    }
                    _ => Box::new(AllZonesSelector {}),
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
        // Fallback: no uid/event_type to resolve metadata; return configured fill_factor zones
        CandidateZone::create_all_zones_for_segment(segment_id)
    }
}

struct AllZonesSelectorMeta<'a> {
    base_dir: &'a std::path::PathBuf,
    uid: String,
}
impl<'a> ZoneSelector for AllZonesSelectorMeta<'a> {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        CandidateZone::create_all_zones_for_segment_from_meta(self.base_dir, segment_id, &self.uid)
    }
}

struct EmptySelector {}
impl ZoneSelector for EmptySelector {
    fn select_for_segment(&self, _segment_id: &str) -> Vec<CandidateZone> {
        Vec::new()
    }
}
