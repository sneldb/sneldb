use crate::engine::core::{CandidateZone, FilterPlan, QueryPlan, ZoneFinder};
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

    /// Runs zone finding logic for this step and stores the results
    pub fn get_candidate_zones(&mut self) {
        let segment_ids = self.plan.segment_ids.read().unwrap();
        debug!(
            target: "sneldb::query::step",
            column = %self.filter.column,
            segments = segment_ids.len(),
            "Finding candidate zones"
        );

        let finder = ZoneFinder::new(
            &self.filter,
            self.plan,
            &segment_ids,
            &self.plan.segment_base_dir,
        );

        self.candidate_zones = finder.find();

        debug!(
            target: "sneldb::query::step",
            column = %self.filter.column,
            zones = self.candidate_zones.len(),
            "Found candidate zones"
        );
    }
}
