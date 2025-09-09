use crate::engine::core::{ExecutionStep, FilterPlan, QueryPlan};

#[derive(Default)]
pub struct ExecutionStepFactory<'a> {
    filter: Option<FilterPlan>,
    plan: Option<&'a QueryPlan>,
}

impl<'a> ExecutionStepFactory<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_filter(mut self, filter: FilterPlan) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_plan(mut self, plan: &'a QueryPlan) -> Self {
        self.plan = Some(plan);
        self
    }

    pub fn create(self) -> ExecutionStep<'a> {
        let filter = self
            .filter
            .expect("ExecutionStepFactory: `filter` is required");
        let plan = self.plan.expect("ExecutionStepFactory: `plan` is required");

        ExecutionStep::new(filter, plan)
    }
}
