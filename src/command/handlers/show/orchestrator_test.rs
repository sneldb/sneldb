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
use crate::test_helpers::factories::command_factory::CommandFactory;
use serde_json::json;

struct StubGateway;

impl CatalogGateway for StubGateway {
    fn load(&self) -> ShowResult<CatalogHandle> {
        Err(ShowError::new("not used"))
    }
}

fn make_context() -> (ShowContext<'static>, tempfile::TempDir) {
    let shard_manager = Box::leak(Box::new(ShardManager { shards: Vec::new() }));
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

    let mut entry = make_entry(temp_dir.path());
    let schema = make_batch_schema();

    let store = MaterializedStore::open(&entry.storage_path).expect("store");
    let mut sink = MaterializedSink::new(store, entry.schema.clone()).expect("materialized sink");

    use crate::engine::types::ScalarValue;
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
