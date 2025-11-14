use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::planner::QueryPlanner;
use crate::command::handlers::query::planner::rlte::RltePlanner;
use crate::command::types::{Command, OrderSpec};
use crate::engine::shard::{Shard, manager::ShardManager};
use crate::test_helpers::factories::SchemaRegistryFactory;
use tempfile::TempDir;

fn create_context_with_shard_dirs() -> (QueryContext<'static>, TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let shard_dir = tempdir.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).expect("create shard dir");

    let command = Box::leak(Box::new(Command::Query {
        event_type: "test_event".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: Some(OrderSpec {
            field: "timestamp".to_string(),
            desc: false,
        }),
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    }));

    let (tx, _rx) = tokio::sync::mpsc::channel(10);
    let manager = Box::leak(Box::new(ShardManager {
        shards: vec![Shard {
            id: 0,
            base_dir: shard_dir.clone(),
            tx,
        }],
    }));

    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);

    (ctx, tempdir)
}

#[tokio::test]
async fn build_plan_returns_outcome() {
    let planner = RltePlanner::new();
    let (ctx, _tempdir) = create_context_with_shard_dirs();

    let outcome = planner.build_plan(&ctx).await;

    assert!(outcome.is_ok());
}

#[tokio::test]
async fn build_plan_handles_empty_shards() {
    let planner = RltePlanner::new();

    let command = Box::leak(Box::new(Command::Query {
        event_type: "test_event".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: Some(10),
        offset: None,
        order_by: Some(OrderSpec {
            field: "timestamp".to_string(),
            desc: false,
        }),
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
    let ctx = QueryContext::new(command, manager, registry);

    let outcome = planner.build_plan(&ctx).await.expect("should not error");

    assert!(outcome.picked_zones.is_none() || outcome.picked_zones.unwrap().is_empty());
}

#[test]
fn new_creates_planner() {
    let planner = RltePlanner::new();
    // Just ensure planner is created
    let _ = planner;
}
