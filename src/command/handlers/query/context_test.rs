use crate::command::handlers::query::context::QueryContext;
use crate::command::types::Command;
use crate::engine::shard::manager::ShardManager;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use std::sync::Arc;

#[test]
fn new_creates_context_with_given_parameters() {
    let command = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let ctx = QueryContext::new(&command, &manager, Arc::clone(&registry));

    assert_eq!(ctx.command as *const Command, &command as *const Command);
    assert_eq!(
        ctx.shard_manager as *const ShardManager,
        &manager as *const ShardManager
    );
    assert_eq!(
        Arc::as_ptr(&ctx.registry) as *const _,
        Arc::as_ptr(&registry) as *const _
    );
}

#[test]
fn new_stores_references_correctly() {
    let command = CommandFactory::query()
        .with_event_type("different_event")
        .with_context_id("ctx123")
        .with_limit(100)
        .with_offset(10)
        .create();

    let manager = ShardManager { shards: Vec::new() };
    let registry = SchemaRegistryFactory::new().registry();

    let ctx = QueryContext::new(&command, &manager, Arc::clone(&registry));

    match ctx.command {
        Command::Query {
            event_type,
            context_id,
            limit,
            offset,
            ..
        } => {
            assert_eq!(event_type, "different_event");
            assert_eq!(context_id, &Some("ctx123".to_string()));
            assert_eq!(limit, &Some(100));
            assert_eq!(offset, &Some(10));
        }
        _ => panic!("Expected Query command"),
    }
}
