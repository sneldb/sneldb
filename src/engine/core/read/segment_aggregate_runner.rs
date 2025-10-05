use crate::engine::core::ConditionEvaluatorBuilder;
use crate::engine::core::filter::condition::{FieldAccessor, PreparedAccessor};
use crate::engine::core::read::sink::ResultSink;
use crate::engine::core::zone::zone_hydrator::ZoneHydrator;
use crate::engine::core::{ExecutionStep, QueryCaches, QueryPlan};
use std::collections::HashMap;

/// Runs segment scans in a columnar fashion and emits matching rows to a ResultSink
pub struct SegmentAggregateRunner<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
}

impl<'a> SegmentAggregateRunner<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
        }
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    /// Hydrates zones, evaluates predicates, and for each matching row calls sink.on_row
    pub async fn run<S: ResultSink>(&self, sink: &mut S) {
        // Hydrate zones with only necessary columns (ProjectionPlanner inside)
        let zones = ZoneHydrator::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .hydrate()
            .await;

        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);

        let mut total_matched: usize = 0;
        for zone in &zones {
            // Build a column reference map owned by this function
            let mut columns: HashMap<
                String,
                crate::engine::core::column::column_values::ColumnValues,
            > = HashMap::new();
            for (k, v) in &zone.values {
                columns.insert(k.clone(), v.clone());
            }
            if columns.is_empty() {
                continue;
            }

            let accessor = PreparedAccessor::new(&columns);
            let n = accessor.event_count();
            let mut matched_in_zone = 0usize;
            for row_idx in 0..n {
                if evaluator.evaluate_row_at(&accessor, row_idx) {
                    sink.on_row(row_idx, &columns);
                    matched_in_zone += 1;
                }
            }
            total_matched += matched_in_zone;
        }
    }
}
