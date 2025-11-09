use crate::engine::core::{
    CandidateZone, ExecutionStep, LogicalOp, QueryCaches, QueryPlan, ZoneCombiner,
};
use crate::engine::core::zone::zone_group_collector::ZoneGroupCollector;
use crate::engine::core::filter::filter_group::{filter_key, FilterGroup};
use std::collections::HashMap;

use tracing::info;

use super::zone_step_planner::ZoneStepPlanner;
use super::zone_step_runner::ZoneStepRunner;

/// Builds a zone cache from unique filters
/// **Single Responsibility**: Build zone cache efficiently without duplication
struct ZoneCacheBuilder<'a> {
    plan: &'a QueryPlan,
    caches: Option<&'a QueryCaches>,
}

impl<'a> ZoneCacheBuilder<'a> {
    fn new(plan: &'a QueryPlan, caches: Option<&'a QueryCaches>) -> Self {
        Self { plan, caches }
    }

    /// Builds zone cache for unique filters
    /// Returns HashMap mapping filter_key -> zones
    fn build_cache(
        &self,
        unique_filters: &[FilterGroup],
    ) -> HashMap<String, Vec<CandidateZone>> {
        // Create execution steps
        let mut steps: Vec<ExecutionStep> = unique_filters
            .iter()
            .map(|filter| ExecutionStep::new(filter.clone(), self.plan))
            .collect();

        // Plan and execute once
        let planner = ZoneStepPlanner::new(self.plan);
        let order = planner.plan(&steps);
        let runner = ZoneStepRunner::new(self.plan).with_caches(self.caches);
        let (zones, _) = runner.run(&mut steps, &order);

        // Build cache efficiently
        order
            .iter()
            .enumerate()
            .filter_map(|(pos, (step_idx, _))| {
                let step = steps.get(*step_idx)?;
                let zones_vec = zones.get(pos)?;
                match &step.filter {
                    FilterGroup::Filter { column, operation, value, .. } => {
                        Some((filter_key(column, operation, value), zones_vec.clone()))
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

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
                .filter_map(|s| s.filter.column())
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

        // Use FilterGroup if available for correct logical combination
        let result = if let Some(ref filter_group) = self.plan.filter_group {
            if tracing::enabled!(tracing::Level::INFO) {
                info!(target: "sneldb::zone_collector", "Using FilterGroup for zone combination");
            }

            // OPTIMIZATION: Extract unique filters to avoid collecting zones multiple times
            let unique_filters = filter_group.extract_unique_filters();
            if tracing::enabled!(tracing::Level::INFO) {
                info!(
                    target: "sneldb::zone_collector",
                    unique_filter_count = unique_filters.len(),
                    total_filter_count = self.steps.len(),
                    "Extracted unique filters for optimization"
                );
            }

            // Build cache using dedicated builder
            let cache_builder = ZoneCacheBuilder::new(self.plan, self.caches);
            let filter_to_zones = cache_builder.build_cache(&unique_filters);

            if tracing::enabled!(tracing::Level::INFO) {
                info!(
                    target: "sneldb::zone_collector",
                    cache_size = filter_to_zones.len(),
                    "Built zone cache for FilterGroup tree"
                );
            }

            // Use ZoneGroupCollector with cache to process FilterGroup tree
            // Pass plan and caches for smart NOT handling
            let group_collector = ZoneGroupCollector::new(
                filter_to_zones,
                self.plan,
                self.caches,
            );
            group_collector.collect_zones_from_group(filter_group)
        } else {
            // Fallback to flat combination using top-level operator
            let op = LogicalOp::from_expr(self.plan.where_clause());
            if tracing::enabled!(tracing::Level::INFO) {
                info!(target: "sneldb::zone_collector", ?op, "Combining zones with logical operation");
            }
            ZoneCombiner::new(all_zones, op).combine()
        };

        let collect_time = collect_start.elapsed();

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                target: "sneldb::zone_collector",
                step_count = self.steps.len(),
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
