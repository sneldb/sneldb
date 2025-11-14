use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::planner::full_scan::FullScanPlanner;
use crate::command::handlers::query::planner::QueryPlanner;
use crate::command::types::Command;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::SchemaRegistryFactory;

fn create_context() -> QueryContext<'static> {
    let command = Box::leak(Box::new(Command::Query {
        event_type: "test_event".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    }));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();

    QueryContext::new(command, manager, registry)
}

#[tokio::test]
async fn build_plan_returns_outcome_without_zones() {
    let planner = FullScanPlanner::new();
    let ctx = create_context();

    let outcome = planner.build_plan(&ctx).await.expect("plan should succeed");

    assert!(outcome.picked_zones.is_none());
}

#[tokio::test]
async fn build_plan_is_idempotent() {
    let planner = FullScanPlanner::new();
    let ctx = create_context();

    let outcome1 = planner.build_plan(&ctx).await.expect("plan should succeed");
    let outcome2 = planner.build_plan(&ctx).await.expect("plan should succeed");

    assert_eq!(
        outcome1.picked_zones.is_none(),
        outcome2.picked_zones.is_none()
    );
}

#[test]
fn new_creates_planner() {
    let planner = FullScanPlanner::new();
    // Just ensure planner is created
    let _ = planner;
}
