use async_trait::async_trait;

use crate::engine::core::read::flow::shard_pipeline::ShardFlowHandle;

use super::super::context::QueryContext;
use super::super::planner::PlanOutcome;

/// Trait for streaming query dispatch strategies.
#[async_trait]
pub trait StreamingDispatch: Send + Sync {
    async fn dispatch(
        &self,
        ctx: &QueryContext<'_>,
        plan: &PlanOutcome,
    ) -> Result<Vec<ShardFlowHandle>, String>;
}
