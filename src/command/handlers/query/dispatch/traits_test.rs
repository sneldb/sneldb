use super::traits::StreamingDispatch;
use super::StreamingShardDispatcher;
use crate::command::handlers::query::context::QueryContext;
use crate::command::handlers::query::planner::PlanOutcome;
use crate::command::types::Command;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::SchemaRegistryFactory;

#[tokio::test]
async fn streaming_dispatch_trait_is_implemented() {
    let dispatcher = StreamingShardDispatcher::new();
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

    let result = dispatcher.dispatch(&ctx, &plan).await;
    assert!(result.is_ok());
}
