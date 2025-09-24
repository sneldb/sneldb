use crate::engine::core::{
    CandidateZone, ExecutionStep, LogicalOp, QueryCaches, QueryPlan, ZoneCombiner,
};
use std::thread;
use tracing::info;

/// Handles collection and combination of zones from execution steps
pub struct ZoneCollector<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneCollector<'a> {
    /// Creates a new ZoneCollector for the given query plan and steps
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
        }
    }

    /// Collects and combines zones from all execution steps
    pub fn collect_zones(&mut self) -> Vec<CandidateZone> {
        info!(target: "sneldb::zone_collector", step_count = self.steps.len(), "Starting zone collection");

        let columns: Vec<&str> = self
            .steps
            .iter()
            .map(|s| s.filter.column.as_str())
            .collect();
        info!(target: "sneldb::zone_collector", columns = ?columns, "Columns to process");

        let mut all_zones: Vec<Vec<CandidateZone>> = Vec::with_capacity(self.steps.len());
        for step in self.steps.iter_mut() {
            if tracing::enabled!(tracing::Level::INFO) {
                info!(
                    target: "sneldb::zone_collector",
                    thread_id = ?thread::current().id(),
                    column = %step.filter.column,
                    "Step started"
                );
            }
            step.get_candidate_zones(self.caches);
            if tracing::enabled!(tracing::Level::INFO) {
                info!(
                    target: "sneldb::zone_collector",
                    thread_id = ?thread::current().id(),
                    column = %step.filter.column,
                    zone_count = step.candidate_zones.len(),
                    "Step finished"
                );
            }

            all_zones.push(std::mem::take(&mut step.candidate_zones));
        }

        info!(target: "sneldb::zone_collector", "All steps completed");

        let op = LogicalOp::from_expr(self.plan.where_clause());
        info!(target: "sneldb::zone_collector", ?op, "Combining zones with logical operation");

        let result = ZoneCombiner::new(all_zones, op).combine();

        info!(
            target: "sneldb::zone_collector",
            zone_count = result.len(),
            "Zone collection complete"
        );

        result
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }
}
