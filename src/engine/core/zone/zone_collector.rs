use crate::engine::core::{CandidateZone, ExecutionStep, LogicalOp, QueryPlan, ZoneCombiner};
use rayon::prelude::*;
use std::thread;
use tracing::info;

/// Handles collection and combination of zones from execution steps
pub struct ZoneCollector<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
}

impl<'a> ZoneCollector<'a> {
    /// Creates a new ZoneCollector for the given query plan and steps
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self { plan, steps }
    }

    /// Collects and combines zones from all execution steps in parallel
    pub fn collect_zones(&mut self) -> Vec<CandidateZone> {
        info!(target: "sneldb::zone_collector", step_count = self.steps.len(), "Starting parallel zone collection");

        let columns: Vec<String> = self.steps.iter().map(|s| s.filter.column.clone()).collect();
        info!(target: "sneldb::zone_collector", ?columns, "Columns to process");

        let all_zones: Vec<Vec<CandidateZone>> = self
            .steps
            .par_iter_mut()
            .map(|step| {
                let thread_id = format!("{:?}", thread::current().id());
                let col = step.filter.column.clone();

                info!(target: "sneldb::zone_collector", %thread_id, %col, "Step started");
                step.get_candidate_zones();
                info!(
                    target: "sneldb::zone_collector",
                    %thread_id,
                    %col,
                    zone_count = step.candidate_zones.len(),
                    "Step finished"
                );

                step.candidate_zones.clone()
            })
            .collect();

        info!(target: "sneldb::zone_collector", "All parallel steps completed");

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
}
