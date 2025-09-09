use crate::engine::core::{
    ConditionEvaluatorBuilder, Event, ExecutionStep, QueryPlan, ZoneHydrator,
};
use tracing::{debug, info};

pub struct SegmentQueryRunner<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
}

impl<'a> SegmentQueryRunner<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self { plan, steps }
    }

    pub async fn run(&self) -> Vec<Event> {
        info!(target: "sneldb::query::segment", "Running segment query runner");

        let candidate_zones = ZoneHydrator::new(self.plan, self.steps.clone())
            .hydrate()
            .await;

        debug!(target: "sneldb::query::segment", "Candidate zones: {:?}", candidate_zones);

        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        let events = evaluator.evaluate_zones(candidate_zones);

        info!(
            target: "sneldb::query::segment",
            "Found {} matching events in disk segments",
            events.len()
        );
        events
    }
}
