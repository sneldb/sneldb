use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::core::zone::selector::field_selector::FieldSelector;
use crate::engine::core::zone::selector::index_selector::{IndexZoneSelector, MissingIndexPolicy};
use crate::engine::core::zone::selector::pruner::enum_pruner::EnumPruner;
use crate::engine::core::zone::selector::pruner::range_pruner::RangePruner;
use crate::engine::core::zone::selector::pruner::temporal_pruner::TemporalPruner;
use crate::engine::core::zone::selector::pruner::xor_pruner::XorPruner;
use crate::engine::core::zone::selector::scope::collect_zones_for_scope;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::zone::selector::selector_kind::ZoneSelector;
use crate::engine::core::zone::zone_artifacts::ZoneArtifacts;
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};

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
                let raw_value = self.inputs.plan.value().and_then(|v| v.as_str());

                // Wildcard or missing event_type -> scan everything.
                if matches!(raw_value, Some("*") | None) {
                    return self.fallback_all_zones_selector();
                }

                let uid = match self.inputs.plan {
                    FilterGroup::Filter { uid, .. } => uid.as_deref(),
                    _ => None,
                };
                let Some(uid) = uid else {
                    return self.fallback_all_zones_selector();
                };
                let event_type = raw_value.unwrap();
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
                    FilterGroup::Filter { uid, .. } => uid.as_deref(),
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
                        if let Some(scope_uid) = self
                            .inputs
                            .query_plan
                            .event_scope()
                            .uid_for(et)
                            .map(|u| u.to_string())
                        {
                            return self.scoped_selector(Some(scope_uid));
                        }
                        self.fallback_all_zones_selector()
                    }
                    _ => self.fallback_all_zones_selector(),
                }
            }
            _ => Box::new(self.make_field_selector(artifacts)),
        }
    }

    fn fallback_all_zones_selector(&self) -> Box<dyn ZoneSelector + 'a> {
        self.scoped_selector(None)
    }

    fn scoped_selector(&self, uid_hint: Option<String>) -> Box<dyn ZoneSelector + 'a> {
        Box::new(ScopedAllZonesSelector {
            plan: self.inputs.query_plan,
            caches: self.inputs.caches,
            uid_hint,
        })
    }
}

struct ScopedAllZonesSelector<'a> {
    plan: &'a QueryPlan,
    caches: Option<&'a QueryCaches>,
    uid_hint: Option<String>,
}

impl<'a> ZoneSelector for ScopedAllZonesSelector<'a> {
    fn select_for_segment(&self, segment_id: &str) -> Vec<CandidateZone> {
        collect_zones_for_scope(self.plan, self.caches, segment_id, self.uid_hint.as_deref())
    }
}
