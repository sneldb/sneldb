use super::stream_merger::StreamMergerKind;
use crate::command::handlers::query::context::QueryContext;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};

#[test]
fn for_context_returns_ordered_merger_with_order_by() {
    let command = Box::leak(Box::new(
        CommandFactory::query()
            .with_limit(10)
            .with_offset(5)
            .with_order_by("timestamp", false)
            .create(),
    ));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);

    let merger = StreamMergerKind::for_context(&ctx);
    match merger {
        StreamMergerKind::Ordered(_) => {}
        StreamMergerKind::Unordered(_) => panic!("Expected Ordered merger"),
    }
}

#[test]
fn for_context_returns_unordered_merger_without_order_by() {
    let command = Box::leak(Box::new(
        CommandFactory::query()
            .with_limit(10)
            .with_offset(5)
            .create(),
    ));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);

    let merger = StreamMergerKind::for_context(&ctx);
    match merger {
        StreamMergerKind::Ordered(_) => panic!("Expected Unordered merger"),
        StreamMergerKind::Unordered(_) => {}
    }
}

#[test]
fn for_context_handles_empty_command() {
    let command = Box::leak(Box::new(CommandFactory::query().create()));

    let manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    let registry = SchemaRegistryFactory::new().registry();
    let ctx = QueryContext::new(command, manager, registry);

    let merger = StreamMergerKind::for_context(&ctx);
    match merger {
        StreamMergerKind::Ordered(_) => panic!("Expected Unordered merger"),
        StreamMergerKind::Unordered(_) => {}
    }
}
