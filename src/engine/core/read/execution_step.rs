use crate::engine::core::{CandidateZone, FilterPlan, QueryCaches, QueryPlan, ZoneFinder};
use tracing::debug;

/// Represents a single step in the query execution
#[derive(Debug, Clone)]
pub struct ExecutionStep<'a> {
    pub filter: FilterPlan,
    pub plan: &'a QueryPlan,
    pub candidate_zones: Vec<CandidateZone>,
}

impl<'a> ExecutionStep<'a> {
    /// Create a new execution step with the provided filter plan and query plan
    pub fn new(filter: FilterPlan, plan: &'a QueryPlan) -> Self {
        Self {
            filter,
            plan,
            candidate_zones: Vec::new(),
        }
    }

    /// Runs zone finding logic for this step restricted to the provided segment list
    pub fn get_candidate_zones_with_segments(
        &mut self,
        _caches: Option<&QueryCaches>,
        segments: &[String],
    ) {
        debug!(
            target: "sneldb::query::step",
            column = %self.filter.column,
            segments = segments.len(),
            "Finding candidate zones (pruned segments)"
        );

        let finder = ZoneFinder::new(
            &self.filter,
            self.plan,
            segments,
            &self.plan.segment_base_dir,
        )
        .with_caches(_caches);

        self.candidate_zones = finder.find();

        debug!(
            target: "sneldb::query::step",
            column = %self.filter.column,
            zones = self.candidate_zones.len(),
            "Found candidate zones (pruned segments)"
        );
    }
}
