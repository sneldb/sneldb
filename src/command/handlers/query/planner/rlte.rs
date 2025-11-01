use std::collections::HashMap;

use async_trait::async_trait;

use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::planner::{PlanOutcome, QueryPlanner};
use crate::command::handlers::rlte_coordinator::RlteCoordinator;
use crate::command::handlers::segment_discovery::SegmentDiscovery;

pub struct RltePlanner;

impl RltePlanner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl QueryPlanner for RltePlanner {
    async fn build_plan(&self, ctx: &QueryContext<'_>) -> Result<PlanOutcome, String> {
        let shard_info: Vec<(usize, std::path::PathBuf)> = ctx
            .shard_manager
            .all_shards()
            .iter()
            .map(|shard| (shard.id, shard.base_dir.clone()))
            .collect();

        let shard_data = SegmentDiscovery::discover_all(shard_info).await;

        let base_dirs: HashMap<usize, std::path::PathBuf> =
            SegmentDiscovery::extract_base_dirs(&shard_data);
        let segments: HashMap<usize, Vec<String>> =
            SegmentDiscovery::extract_segment_map(&shard_data);

        let planning_output =
            RlteCoordinator::plan(ctx.command, ctx.registry.clone(), &base_dirs, &segments).await;

        match planning_output {
            Some(result) => Ok(PlanOutcome::with_zones(result.per_shard)),
            None => Ok(PlanOutcome::without_zones()),
        }
    }
}
