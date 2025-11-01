use async_trait::async_trait;

use super::plan_outcome::PlanOutcome;
use crate::command::handlers::query::context::QueryContext;

/// Trait for query planning strategies.
#[async_trait]
pub trait QueryPlanner: Send + Sync {
    async fn build_plan(&self, ctx: &QueryContext<'_>) -> Result<PlanOutcome, String>;
}
