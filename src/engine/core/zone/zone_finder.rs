use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::{CandidateZone, FilterPlan, QueryCaches, QueryPlan};
use std::path::PathBuf;
use tracing::debug;

pub struct ZoneFinder<'a> {
    plan: &'a FilterPlan,
    query_plan: &'a QueryPlan,
    segment_ids: &'a [String],
    base_dir: &'a PathBuf,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneFinder<'a> {
    pub fn new(
        plan: &'a FilterPlan,
        query_plan: &'a QueryPlan,
        segment_ids: &'a [String],
        base_dir: &'a PathBuf,
    ) -> Self {
        Self {
            plan,
            query_plan,
            segment_ids,
            base_dir,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    pub fn find(&self) -> Vec<CandidateZone> {
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(target: "sneldb::query", "Finding candidate zones for filter: {:?}", self.plan);
        }
        let mut out: Vec<CandidateZone> = Vec::new();
        out.reserve(self.segment_ids.len());
        let ctx = SelectionContext {
            plan: self.plan,
            query_plan: self.query_plan,
            base_dir: self.base_dir,
            caches: self.caches,
        };
        let selector = ZoneSelectorBuilder::new(ctx).build();
        for segment_id in self.segment_ids.iter() {
            let zones = selector.select_for_segment(segment_id);
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::query", segment = %segment_id, column = %self.plan.column, zones = zones.len(), "Segment zones computed");
            }
            out.extend(zones);
        }
        out
    }

    // finders and pruners are implemented in dedicated modules
}
