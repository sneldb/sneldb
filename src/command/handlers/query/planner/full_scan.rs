use async_trait::async_trait;

use super::plan_outcome::PlanOutcome;
use super::traits::QueryPlanner;
use crate::command::handlers::query::context::QueryContext;

pub struct FullScanPlanner;

impl FullScanPlanner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl QueryPlanner for FullScanPlanner {
    async fn build_plan(&self, _ctx: &QueryContext<'_>) -> Result<PlanOutcome, String> {
        Ok(PlanOutcome::without_zones())
    }
}
