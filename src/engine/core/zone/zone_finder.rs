use crate::engine::core::zone::selector::builder::ZoneSelectorBuilder;
use crate::engine::core::zone::selector::selection_context::SelectionContext;
use crate::engine::core::{CandidateZone, QueryCaches, QueryPlan};
use crate::engine::core::filter::filter_group::FilterGroup;
use std::path::PathBuf;
use tracing::debug;

pub struct ZoneFinder<'a> {
    plan: &'a FilterGroup,
    query_plan: &'a QueryPlan,
    segment_ids: &'a [String],
    base_dir: &'a PathBuf,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneFinder<'a> {
    pub fn new(
        plan: &'a FilterGroup,
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
        let find_start = std::time::Instant::now();
        let has_materialization_metadata = self
            .query_plan
            .metadata
            .get("materialization_created_at")
            .is_some();

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
        let mut segments_checked = 0usize;
        let mut total_zones_found = 0usize;
        let column = self.plan.column().unwrap_or("unknown");

        for segment_id in self.segment_ids.iter() {
            segments_checked += 1;
            let zones = selector.select_for_segment(segment_id);
            total_zones_found += zones.len();
            if tracing::enabled!(tracing::Level::DEBUG) {
                debug!(target: "sneldb::query", segment = %segment_id, column = %column, zones = zones.len(), "Segment zones computed");
            }
            out.extend(zones);
        }

        let find_time = find_start.elapsed();
        if has_materialization_metadata && out.is_empty() && find_time.as_millis() > 10 {
            if tracing::enabled!(tracing::Level::INFO) {
                tracing::info!(
                    target: "sneldb::zone_finder",
                    column = %column,
                    segments_checked = segments_checked,
                    total_zones_found = total_zones_found,
                    find_time_ms = find_time.as_millis(),
                    "ZoneFinder completed with 0 zones after checking all segments"
                );
            }
        }

        out
    }

    // finders and pruners are implemented in dedicated modules
}
