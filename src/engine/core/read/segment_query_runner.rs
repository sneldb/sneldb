use crate::engine::core::{
    ConditionEvaluatorBuilder, Event, EventSorter, ExecutionStep, QueryCaches, QueryContext,
    QueryPlan, ZoneHydrator,
};
use tracing::info;

pub struct SegmentQueryRunner<'a> {
    plan: &'a QueryPlan,
    steps: Vec<ExecutionStep<'a>>,
    caches: Option<&'a QueryCaches>,
    limit: Option<usize>,
}

impl<'a> SegmentQueryRunner<'a> {
    pub fn new(plan: &'a QueryPlan, steps: Vec<ExecutionStep<'a>>) -> Self {
        Self {
            plan,
            steps,
            caches: None,
            limit: None,
        }
    }

    pub async fn run(&self) -> Vec<Event> {
        info!(target: "sneldb::query::segment", "Running segment query runner");

        // Extract query context from command
        let ctx = QueryContext::from_command(&self.plan.command);

        // Hydrate zones with optional zone filtering
        let candidate_zones = self.hydrate_zones(&ctx).await;

        // Determine evaluation limit (defer if ordering required)
        let eval_limit = self.determine_eval_limit(&ctx);

        // Evaluate zones to get matching events
        let mut events = self.evaluate_zones(candidate_zones, eval_limit);

        // Sort events if ORDER BY is present
        if let Some(sorter) = self.create_sorter(&ctx) {
            sorter.sort(&mut events);
        }

        info!(
            target: "sneldb::query::segment",
            "Found {} matching events in disk segments",
            events.len()
        );
        events
    }

    /// Hydrates candidate zones, applying zone filtering if present in context.
    async fn hydrate_zones(&self, ctx: &QueryContext) -> Vec<crate::engine::core::CandidateZone> {
        ZoneHydrator::new(self.plan, self.steps.clone())
            .with_caches(self.caches)
            .with_allowed_zones(ctx.picked_zones.clone())
            .hydrate()
            .await
    }

    /// Determines the evaluation limit based on context.
    ///
    /// If ORDER BY is present, returns None to allow all events to be collected
    /// for proper sorting. Otherwise, returns the configured limit.
    fn determine_eval_limit(&self, ctx: &QueryContext) -> Option<usize> {
        if ctx.should_defer_limit() {
            None
        } else {
            self.limit
        }
    }

    /// Evaluates zones to produce matching events.
    fn evaluate_zones(
        &self,
        zones: Vec<crate::engine::core::CandidateZone>,
        limit: Option<usize>,
    ) -> Vec<Event> {
        let evaluator = ConditionEvaluatorBuilder::build_from_plan(self.plan);
        evaluator.evaluate_zones_with_limit(zones, limit)
    }

    /// Creates an EventSorter if ORDER BY is present in context.
    fn create_sorter(&self, ctx: &QueryContext) -> Option<EventSorter> {
        ctx.order_by.as_ref().map(EventSorter::from_order_spec)
    }

    pub fn with_caches(mut self, caches: Option<&'a QueryCaches>) -> Self {
        self.caches = caches;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
}
