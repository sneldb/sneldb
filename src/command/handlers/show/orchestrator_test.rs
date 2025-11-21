use std::sync::Arc;

use super::orchestrator::ShowExecutionPipeline;
use crate::command::handlers::show::catalog::{CatalogGateway, CatalogHandle};
use crate::command::handlers::show::context::ShowContext;
use crate::command::handlers::show::errors::{ShowError, ShowResult};
use crate::command::handlers::show::result::ShowRefreshOutcome;
use crate::command::types::{Command, MaterializedQuerySpec};
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::{
    HighWaterMark, MaterializationEntry, MaterializedSink, MaterializedStore,
};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::engine::shard::message::ShardMessage;
use crate::engine::shard::types::Shard;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::command_factory::CommandFactory;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Notify, mpsc};
use tokio::time::Duration;

struct StubGateway;

impl CatalogGateway for StubGateway {
    fn load(&self) -> ShowResult<CatalogHandle> {
        Err(ShowError::new("not used"))
    }
}

fn make_context() -> (ShowContext<'static>, tempfile::TempDir) {
    let shard_manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
    make_context_with_manager(shard_manager)
}

fn make_context_with_manager(
    shard_manager: &'static ShardManager,
) -> (ShowContext<'static>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let registry =
        SchemaRegistry::new_with_path(temp_dir.path().join("schemas.bin")).expect("registry");
    let registry = Arc::new(tokio::sync::RwLock::new(registry));

    (
        ShowContext::new("orders_view", shard_manager, Arc::clone(&registry)),
        temp_dir,
    )
}

fn make_entry(root: &std::path::Path) -> MaterializationEntry {
    let command = CommandFactory::query().with_event_type("orders").create();

    let spec = MaterializedQuerySpec {
        name: "orders_view".to_string(),
        query: Box::new(command),
    };

    let mut entry = MaterializationEntry::new(spec, root).expect("entry");
    entry.schema = vec![
        SchemaSnapshot::new("timestamp", "Number"),
        SchemaSnapshot::new("event_id", "Number"),
    ];
    entry
}

fn make_batch_schema() -> Arc<BatchSchema> {
    Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
        ])
        .expect("schema"),
    )
}

#[test]
fn ensure_schema_present_errors_when_empty() {
    let (context, temp_dir) = make_context();
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let mut entry = make_entry(temp_dir.path());
    entry.schema.clear();

    let err = pipeline
        .test_ensure_schema_present(&entry)
        .expect_err("should fail");
    assert!(err.message().contains("does not have a stored schema"));
}

#[test]
fn timestamp_column_uses_custom_field_when_present() {
    let (context, temp_dir) = make_context();
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let mut entry = make_entry(temp_dir.path());
    if let Command::Query { time_field, .. } = entry.spec.query.as_mut() {
        *time_field = Some("created_at".to_string());
    }

    let column = pipeline.test_timestamp_column(&entry);
    assert_eq!(column, "created_at");
}

#[test]
fn build_delta_command_updates_since_from_watermark() {
    let (context, temp_dir) = make_context();
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let mut entry = make_entry(temp_dir.path());
    entry.high_water_mark = Some(HighWaterMark::new(123, 1));

    let command = pipeline
        .test_build_delta_command(&entry)
        .expect("delta command");

    if let Command::Query { since, .. } = command {
        assert_eq!(since, Some("123".to_string()));
    } else {
        panic!("expected query command");
    }
}

#[test]
fn build_outcome_updates_entry_metrics() {
    let (context, temp_dir) = make_context();
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let entry = make_entry(temp_dir.path());
    let schema = make_batch_schema();

    let store = MaterializedStore::open(&entry.storage_path).expect("store");
    let mut sink = MaterializedSink::new(store, entry.schema.clone()).expect("materialized sink");

    let batch = ColumnBatch::new(
        Arc::clone(&schema),
        vec![
            vec![ScalarValue::from(json!(1u64))],
            vec![ScalarValue::from(json!(10u64))],
        ],
        1,
        None,
    )
    .expect("batch");
    sink.append(&batch).expect("append");

    let outcome: ShowRefreshOutcome =
        pipeline.test_build_outcome(entry, sink, HighWaterMark::default());
    let updated = outcome.into_entry();

    assert_eq!(updated.row_count, 1);
    assert_eq!(updated.delta_rows_appended, 1);
    assert!(updated.high_water_mark.is_some());
}

#[tokio::test]
async fn wait_for_inflight_flushes_propagates_errors() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);

    let shard = Shard {
        id: 0,
        tx,
        base_dir: PathBuf::new(),
    };
    let shard_manager = Box::leak(Box::new(ShardManager {
        shards: vec![shard],
    }));
    let (context, _temp_dir) = make_context_with_manager(shard_manager);
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let err = pipeline
        .test_wait_for_inflight_flushes()
        .await
        .expect_err("flush should fail");
    assert!(
        err.message()
            .contains("Failed to wait for shard flushes before SHOW"),
        "unexpected error message: {}",
        err.message()
    );
}

#[tokio::test]
async fn wait_for_inflight_flushes_waits_for_completion() {
    let (tx, mut rx) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let flush_count = Arc::new(AtomicUsize::new(0));

    let notify_listener = Arc::clone(&notify);
    let counter = Arc::clone(&flush_count);
    tokio::spawn(async move {
        if let Some(message) = rx.recv().await {
            if let ShardMessage::AwaitFlush { completion } = message {
                counter.fetch_add(1, Ordering::SeqCst);
                notify_listener.notified().await;
                let _ = completion.send(Ok(()));
            }
        }
    });

    let shard = Shard {
        id: 0,
        tx,
        base_dir: PathBuf::new(),
    };
    let shard_manager = Box::leak(Box::new(ShardManager {
        shards: vec![shard],
    }));

    let (context, _temp_dir) = make_context_with_manager(shard_manager);
    let pipeline = ShowExecutionPipeline::new_with_gateway(context, StubGateway);

    let flush_future = pipeline.test_wait_for_inflight_flushes();
    tokio::pin!(flush_future);

    let pending = tokio::time::timeout(Duration::from_millis(25), flush_future.as_mut()).await;
    assert!(pending.is_err(), "flush completed before notify");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(flush_count.load(Ordering::SeqCst), 1, "flush not invoked");

    notify.notify_one();

    flush_future.await.expect("flush should succeed");
}
