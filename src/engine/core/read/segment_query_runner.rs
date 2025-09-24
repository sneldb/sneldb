use crate::engine::core::{
    ConditionEvaluatorBuilder, Event, ExecutionStep, QueryCaches, QueryPlan, ZoneHydrator,
};
use tracing::{debug, info};

pub struct SegmentQueryRunner<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
}

impl<'a> SegmentQueryRunner<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
        }
    }

    pub async fn run(&self) -> Vec<Event> {
        info!(target: "sneldb::query::segment", "Running segment query runner");

        let candidate_zones = ZoneHydrator::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .hydrate()
            .await;

        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        let events = evaluator.evaluate_zones(candidate_zones);

        info!(
            target: "sneldb::query::segment",
            "Found {} matching events in disk segments",
            events.len()
        );
        events
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }
}
