use super::traits::QueryPlanner;
use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::planner::full_scan::FullScanPlanner;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};

#[tokio::test]
async fn query_planner_trait_is_implemented() {
    let planner = FullScanPlanner::new();
    let command = Box::leak(Box::new(CommandFactory::query().create()));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);

    let result: Result<_, String> = planner.build_plan(&ctx).await;
    assert!(result.is_ok());
}
