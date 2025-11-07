use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::dispatch::BatchDispatch;
use crate::command::handlers::query::dispatch::batch::BatchShardDispatcher;
use crate::command::handlers::query::planner::PlanOutcome;
use crate::command::types::Command;
use crate::engine::shard::Shard;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::SchemaRegistryFactory;
use tempfile::TempDir;
use tokio::sync::mpsc;

fn create_test_context() -> (QueryContext<'static>, TempDir) {
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

    let (tx, _rx) = mpsc::channel(10);
    let manager = Box::leak(Box::new(ShardManager {
        shards: vec![Shard {
            id: 0,
            base_dir: shard_dir,
            tx,
        }],
    }));

    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);
    (ctx, tempdir)
}

#[tokio::test]
async fn dispatch_handles_empty_shards() {
    let dispatcher = BatchShardDispatcher::new();
    let command = Box::leak(Box::new(Command::Query {
        event_type: "test".to_string(),
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
    let ctx = QueryContext::new(command, manager, registry);
    let plan = PlanOutcome::without_zones();

    let result = dispatcher
        .dispatch(&ctx, &plan)
        .await
        .expect("dispatch should succeed");
    assert_eq!(result.len(), 0);
}

#[test]
fn new_creates_dispatcher() {
    let dispatcher = BatchShardDispatcher::new();
    assert!(std::mem::size_of_val(&dispatcher) >= 0);
}
