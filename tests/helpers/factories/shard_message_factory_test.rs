use crate::command::types::Command;
use crate::engine::core::read::result::{QueryResult, SelectionResult};
use crate::engine::shard::message::ShardMessage;
use crate::test_helpers::factories::{EventFactory, SchemaRegistryFactory, ShardMessageFactory};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_shard_message_factory_variants() {
    // Setup registry factory and schema
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Register a dummy schema for the test event_type
    registry_factory
        .define_with_fields("test_event", &[("key", "string")])
        .await
        .expect("Failed to define schema");

    let factory = ShardMessageFactory::new(registry.clone());

    // Prepare a test Event
    let event = EventFactory::new()
        .with("context_id", "ctx-test")
        .with("event_type", "test_event")
        .create();

    // Prepare a test Command
    let cmd = Command::Ping;

    // Store
    let msg = factory.store(event.clone());
    match msg {
        ShardMessage::Store(e, reg) => {
            assert_eq!(e.context_id, event.context_id);
            assert!(Arc::ptr_eq(&reg, &registry));
        }
        _ => panic!("Expected Store variant"),
    }

    // Flush
    let (tx_flush, _) = mpsc::channel(1);
    let msg = factory.flush(tx_flush.clone());
    match msg {
        ShardMessage::Flush(sender, reg, _) => {
            sender.clone().send(vec![]).await.ok(); // ensure it's usable
            assert!(Arc::ptr_eq(&reg, &registry));
        }
        _ => panic!("Expected Flush variant"),
    }

    // Query
    let (tx_query, _) = mpsc::channel(1);
    let msg = factory.query(cmd.clone(), tx_query.clone());
    match msg {
        ShardMessage::Query(c, sender, reg) => {
            assert_eq!(format!("{:?}", c), format!("{:?}", cmd));
            sender
                .clone()
                .send(QueryResult::Selection(SelectionResult {
                    columns: vec![],
                    rows: vec![],
                }))
                .await
                .ok();
            assert!(Arc::ptr_eq(&reg, &registry));
        }
        _ => panic!("Expected Query variant"),
    }

    // Replay
    let (tx_replay, _) = mpsc::channel(1);
    let msg = factory.replay(cmd.clone(), tx_replay.clone());
    match msg {
        ShardMessage::Replay(c, sender, reg) => {
            assert_eq!(format!("{:?}", c), format!("{:?}", cmd));
            sender.clone().send(vec![]).await.ok();
            assert!(Arc::ptr_eq(&reg, &registry));
        }
        _ => panic!("Expected Replay variant"),
    }
}
