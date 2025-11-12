use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::command::types::Command;
use crate::engine::core::memory::passive_buffer_set::PassiveBufferSet;
use crate::engine::query::streaming::builders::FlowBuilders;
use crate::engine::query::streaming::context::StreamingContext;
use crate::test_helpers::factories::{
    CommandFactory, EventFactory, MemTableFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use tokio::time::timeout;

async fn build_context(
    registry_factory: &SchemaRegistryFactory,
    command: Command,
    batch_size: usize,
) -> StreamingContext {
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(registry_factory.registry())
        .create()
        .await;

    let passive_buffers = Arc::new(PassiveBufferSet::new(4));
    StreamingContext::new(Arc::new(plan), &passive_buffers, batch_size)
        .await
        .expect("context")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memtable_flow_streams_rows() {
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
        .expect("schema");

    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();

    let context = build_context(&registry_factory, command, 8).await;

    let events = vec![
        EventFactory::new()
            .with("event_type", "stream_event")
            .with("context_id", "ctx-a")
            .with("payload", json!({"value": 11}))
            .create(),
        EventFactory::new()
            .with("event_type", "stream_event")
            .with("context_id", "ctx-b")
            .with("payload", json!({"value": 13}))
            .create(),
    ];
    let memtable = MemTableFactory::new()
        .with_events(events)
        .create()
        .expect("memtable");

    let builders = FlowBuilders::new(&memtable);
    let handle = builders
        .memtable_flow(&context)
        .await
        .expect("flow")
        .expect("handle produced");

    let context_idx = handle
        .schema
        .columns()
        .iter()
        .position(|col| col.name == "context_id")
        .expect("context column");

    let mut receiver = handle.receiver;
    let mut contexts = HashSet::new();
    while let Some(batch) = receiver.recv().await {
        let column = batch.column(context_idx).expect("context column data");
        for value in column {
            if let Some(context) = value.as_str() {
                contexts.insert(context.to_string());
            }
        }
    }

    assert!(contexts.contains("ctx-a"));
    assert!(contexts.contains("ctx-b"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_respects_zero_limit() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[("context_id", "string"), ("timestamp", "u64")],
        )
        .await
        .expect("schema");

    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .with_limit(0)
        .create();

    let context = build_context(&registry_factory, command, 8).await;
    let memtable = MemTableFactory::new().create().expect("memtable");
    let builders = FlowBuilders::new(&memtable);

    let result = builders.segment_flow(&context).await.expect("result");
    assert!(result.is_none(), "segment flow should honour zero limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_flow_returns_handle_without_events() {
    let registry_factory = SchemaRegistryFactory::new();
    registry_factory
        .define_with_fields(
            "stream_event",
            &[("context_id", "string"), ("timestamp", "u64")],
        )
        .await
        .expect("schema");

    let command = CommandFactory::query()
        .with_event_type("stream_event")
        .create();

    let context = build_context(&registry_factory, command, 4).await;
    let memtable = MemTableFactory::new().create().expect("memtable");
    let builders = FlowBuilders::new(&memtable);

    let handle = builders
        .segment_flow(&context)
        .await
        .expect("result")
        .expect("handle");

    let mut receiver = handle.receiver;
    let recv_result = timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("timeout");
    assert!(
        recv_result.is_none(),
        "segment flow should eventually close channel"
    );
}
