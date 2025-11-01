use std::sync::Arc;

use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::query::streaming::context::StreamingContext;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};

#[tokio::test]
async fn new_populates_passive_snapshot_and_limits() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[
                ("context_id", "string"),
                ("timestamp", "u64"),
                ("payload", "object"),
                ("value", "int"),
            ],
        )
        .await
        .expect("schema defined");

    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .with_limit(5)
        .with_offset(2)
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry_factory.registry())
        .create()
        .await;

    let event = EventFactory::new()
        .with("event_type", "stream_event")
        .with("payload", serde_json::json!({"value": 10}))
        .create();
    let memtable = MemTableFactory::new()
        .with_events(vec![event])
        .create()
        .expect("memtable");

    let passive_buffers = Arc::new(PassiveBufferSet::new(4));
    passive_buffers.add_from(&memtable).await;

    let ctx = StreamingContext::new(Arc::new(plan.clone()), &passive_buffers, 32)
        .await
        .expect("context initializes");

    assert_eq!(ctx.batch_size(), 32);
    assert_eq!(ctx.effective_limit(), Some(7));
    assert_eq!(ctx.passive_refs().len(), 1);
    assert_eq!(ctx.plan().event_type(), "stream_event");
    assert_eq!(ctx.metrics().total_sent_batches(), 0);
}

#[tokio::test]
async fn new_handles_missing_passives_and_limit() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[("context_id", "string"), ("timestamp", "u64")],
        )
        .await
        .expect("schema defined");

    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry_factory.registry())
        .create()
        .await;

    let passive_buffers = Arc::new(PassiveBufferSet::new(2));
    let ctx = StreamingContext::new(Arc::new(plan.clone()), &passive_buffers, 16)
        .await
        .expect("context initializes");

    assert_eq!(ctx.batch_size(), 16);
    assert!(ctx.effective_limit().is_none());
    assert!(ctx.passive_refs().is_empty());
    assert_eq!(ctx.plan().event_type(), "stream_event");
}
