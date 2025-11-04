use crate::engine::core::{
    CandidateZone, ExecutionStep, LogicalOp, QueryCaches, QueryPlan, ZoneCombiner,
};

use tracing::info;

use super::zone_step_planner::ZoneStepPlanner;
use super::zone_step_runner::ZoneStepRunner;

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
        let collect_start = std::time::Instant::now();
        let has_materialization_metadata = self
            .plan
            .metadata
            .get("materialization_created_at")
            .is_some();

        if tracing::enabled!(tracing::Level::INFO) {
            info!(target: "sneldb::zone_collector", step_count = self.steps.len(), "Starting zone collection");
        }

        if tracing::enabled!(tracing::Level::INFO) {
            let columns: Vec<&str> = self
                .steps
                .iter()
                .map(|s| s.filter.column.as_str())
                .collect();
            info!(target: "sneldb::zone_collector", columns = ?columns, "Columns to process");
        }

        // Plan order/pruning separately
        let planner = ZoneStepPlanner::new(self.plan);
        let order = planner.plan(&self.steps);

        // Execute steps in planned order using the runner
        let runner = ZoneStepRunner::new(self.plan).with_caches(self.caches);
        let (all_zones, _pruned) = runner.run(&mut self.steps, &order);

        if tracing::enabled!(tracing::Level::INFO) {
            info!(target: "sneldb::zone_collector", "All steps completed");
        }

        let op = LogicalOp::from_expr(self.plan.where_clause());
        if tracing::enabled!(tracing::Level::INFO) {
            info!(target: "sneldb::zone_collector", ?op, "Combining zones with logical operation");
        }

        let total_zones_before_combine: usize = all_zones.iter().map(|zones| zones.len()).sum();
        let result = ZoneCombiner::new(all_zones, op).combine();

        let collect_time = collect_start.elapsed();

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                target: "sneldb::zone_collector",
                step_count = self.steps.len(),
                total_zones_before_combine = total_zones_before_combine,
                zones_after_combine = result.len(),
                has_materialization_metadata = has_materialization_metadata,
                collect_time_ms = collect_time.as_millis(),
                "Zone collection completed"
            );
        }

        if tracing::enabled!(tracing::Level::INFO) {
            info!(
                target: "sneldb::zone_collector",
                zone_count = result.len(),
                "Zone collection complete"
            );
        }

        result
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }
}
