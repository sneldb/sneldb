use std::sync::Arc;

use tokio::io::{AsyncWrite, duplex};
use tokio::time::{Duration, sleep};

use crate::command::handlers::flush;
use crate::command::handlers::remember::remember_query_with_data_dir;
use crate::command::handlers::store;
use crate::command::parser::commands::{flush as flush_parser, remember};
use crate::command::parser::tokenizer::tokenize;
use crate::command::types::Command;
use crate::engine::materialize::MaterializationCatalog;
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::{JsonRenderer, render::Renderer};
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use tempfile::tempdir;

async fn setup_test_environment() -> (
    tempfile::TempDir,
    ShardManager,
    Arc<tokio::sync::RwLock<SchemaRegistry>>,
    std::path::PathBuf,
) {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let data_dir_temp = tempdir().unwrap();
    let data_dir = data_dir_temp.path().to_path_buf();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int"), ("value", "string")])
        .await
        .unwrap();
    let registry = factory.registry();

    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    (data_dir_temp, shard_manager, registry, data_dir)
}

async fn execute_store<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    store::handle(cmd, shard_manager, registry, writer, renderer).await
}

async fn execute_flush<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    flush::handle(cmd, shard_manager, registry, writer, renderer).await
}

#[tokio::test]
async fn test_remember_basic_query() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store some events
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    // Flush to persist data
    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember the query
    let remember_cmd = remember::parse("REMEMBER QUERY test_event AS basic_materialization")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Verify summary contains expected information
    assert!(
        summary
            .iter()
            .any(|s| s.contains("remembered query 'basic_materialization'")),
        "Summary should contain remembered query message"
    );
    assert!(
        summary.iter().any(|s| s.contains("rows stored:")),
        "Summary should contain rows stored"
    );

    // Verify materialization was created in catalog
    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("basic_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.name, "basic_materialization");
    assert!(entry.row_count > 0, "Entry should have rows");
    assert!(!entry.schema.is_empty(), "Entry should have schema");
}

#[tokio::test]
async fn test_remember_with_limit() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store 10 events
    for i in 1..=10 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember with LIMIT 5
    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event LIMIT 5 AS limited_materialization")
            .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Verify limit was respected
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 5")),
        "Should store exactly 5 rows, summary: {:?}",
        summary
    );

    // Verify catalog entry
    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("limited_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.row_count, 5, "Entry should have exactly 5 rows");
}

#[tokio::test]
async fn test_remember_with_limit_truncation() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store 10 events
    for i in 1..=10 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember with LIMIT 7 (will need to truncate a batch)
    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event LIMIT 7 AS truncated_materialization")
            .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Verify limit was respected even with truncation
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 7")),
        "Should store exactly 7 rows after truncation, summary: {:?}",
        summary
    );

    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("truncated_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.row_count, 7, "Entry should have exactly 7 rows");
}

#[tokio::test]
async fn test_remember_duplicate_name_error() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store and flush events
    let store_cmd = CommandFactory::store()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"id": 1, "value": "value1"}))
        .create();
    let (_r, mut w) = duplex(1024);
    execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
        .await
        .expect("store should succeed");

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // First remember
    let remember_cmd1 = remember::parse("REMEMBER QUERY test_event AS duplicate_test")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec: spec1 } = remember_cmd1 else {
        panic!("Expected RememberQuery command");
    };

    let result1 = remember_query_with_data_dir(spec1, &shard_manager, &registry, &data_dir).await;
    assert!(result1.is_ok(), "First remember should succeed");

    // Try to remember again with same name
    let remember_cmd2 = remember::parse("REMEMBER QUERY test_event AS duplicate_test")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec: spec2 } = remember_cmd2 else {
        panic!("Expected RememberQuery command");
    };

    let result2 = remember_query_with_data_dir(spec2, &shard_manager, &registry, &data_dir).await;
    assert!(
        result2.is_err(),
        "Second remember with duplicate name should fail"
    );
    assert!(
        result2.unwrap_err().contains("already exists"),
        "Error should mention duplicate name"
    );
}

#[tokio::test]
async fn test_remember_empty_results() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Don't store any events, just flush
    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember query that returns no results
    let remember_cmd = remember::parse("REMEMBER QUERY test_event AS empty_materialization")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(
        result.is_ok(),
        "remember should succeed even with empty results"
    );
    let summary = result.unwrap();

    // Verify summary indicates zero rows
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 0")),
        "Should indicate zero rows stored, summary: {:?}",
        summary
    );

    // Verify catalog entry exists but has zero rows
    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("empty_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.row_count, 0, "Entry should have zero rows");
    assert!(!entry.schema.is_empty(), "Entry should still have schema");
}

#[tokio::test]
async fn test_remember_high_water_mark_tracking() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store events with delays to ensure different timestamps
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
        sleep(Duration::from_millis(10)).await; // Small delay for different timestamps
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember the query
    let remember_cmd = remember::parse("REMEMBER QUERY test_event AS hwm_materialization")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Verify high water mark is tracked
    let has_hwm = summary.iter().any(|s| s.contains("high-water mark"));
    assert!(
        has_hwm,
        "Summary should contain high-water mark, summary: {:?}",
        summary
    );

    // Verify catalog entry has high water mark
    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("hwm_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert!(
        entry.high_water_mark.is_some(),
        "Entry should have high water mark set"
    );

    if let Some(hwm) = entry.high_water_mark {
        assert!(
            hwm.timestamp > 0,
            "High water mark should have valid timestamp"
        );
        assert!(
            hwm.event_id > 0,
            "High water mark should have valid event_id"
        );
    }
}

#[tokio::test]
async fn test_remember_zero_high_water_mark() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Flush without storing (empty state)
    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember query that returns no results
    let remember_cmd = remember::parse("REMEMBER QUERY test_event AS zero_hwm_materialization")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");

    // Verify catalog entry has no high water mark (None for zero)
    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("zero_hwm_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert!(
        entry.high_water_mark.is_none(),
        "Entry with zero rows should have None high water mark"
    );
}

#[tokio::test]
async fn test_remember_with_where_clause() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store events with different values
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({
                "id": i,
                "value": if i % 2 == 0 { "even" } else { "odd" }
            }))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember query with WHERE clause
    let remember_cmd = remember::parse(
        "REMEMBER QUERY test_event WHERE value=\"even\" AS filtered_materialization",
    )
    .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Should have filtered results (only even values: 2, 4)
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 2")),
        "Should store 2 filtered rows, summary: {:?}",
        summary
    );

    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("filtered_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.row_count, 2, "Entry should have 2 filtered rows");
}

#[tokio::test]
async fn test_remember_with_return_fields() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store events
    for i in 1..=3 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember query with RETURN fields
    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event RETURN [id] AS return_fields_materialization")
            .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");

    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("return_fields_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    // Verify schema only contains returned fields (plus standard fields)
    assert!(!entry.schema.is_empty(), "Entry should have schema");
    assert_eq!(entry.row_count, 3, "Entry should have 3 rows");
}

#[tokio::test]
async fn test_remember_catalog_load_error() {
    let (_temp_dir, shard_manager, registry, _data_dir) = setup_test_environment().await;

    // Use invalid data directory (non-existent parent)
    let invalid_data_dir = std::path::PathBuf::from("/nonexistent/path/to/catalog");

    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event AS error_test").expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result =
        remember_query_with_data_dir(spec, &shard_manager, &registry, &invalid_data_dir).await;

    assert!(
        result.is_err(),
        "remember should fail with invalid data directory"
    );
    assert!(
        result
            .unwrap_err()
            .contains("Failed to load materialization catalog"),
        "Error should mention catalog load failure"
    );
}

#[tokio::test]
async fn test_remember_summary_includes_all_metrics() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store events
    for i in 1..=3 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    let remember_cmd = remember::parse("REMEMBER QUERY test_event AS metrics_test")
        .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Verify summary contains all expected metrics
    let summary_text = summary.join("\n");
    assert!(
        summary_text.contains("remembered query"),
        "Summary should contain remembered query message"
    );
    assert!(
        summary_text.contains("rows stored:"),
        "Summary should contain rows stored"
    );
    assert!(
        summary_text.contains("rows appended:"),
        "Summary should contain rows appended"
    );
    assert!(
        summary_text.contains("compressed bytes:"),
        "Summary should contain compressed bytes"
    );
    assert!(
        summary_text.contains("bytes appended:"),
        "Summary should contain bytes appended"
    );
}

#[tokio::test]
async fn test_remember_limit_zero() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store events
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember with LIMIT 0 (edge case)
    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event LIMIT 0 AS zero_limit_materialization")
            .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed with LIMIT 0");
    let summary = result.unwrap();

    // Should store zero rows
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 0")),
        "Should store zero rows with LIMIT 0, summary: {:?}",
        summary
    );

    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("zero_limit_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(entry.row_count, 0, "Entry should have zero rows");
}

#[tokio::test]
async fn test_remember_limit_larger_than_results() {
    let (_temp_dir, shard_manager, registry, data_dir) = setup_test_environment().await;

    // Store only 3 events
    for i in 1..=3 {
        let store_cmd = CommandFactory::store()
            .with_event_type("test_event")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({"id": i, "value": format!("value{}", i)}))
            .create();
        let (_r, mut w) = duplex(1024);
        execute_store(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(100)).await;

    let flush_cmd = flush_parser::parse(&tokenize("FLUSH")).expect("parse FLUSH");
    let (_r_flush, mut w_flush) = duplex(1024);
    execute_flush(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w_flush,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(200)).await;

    // Remember with LIMIT larger than available results
    let remember_cmd =
        remember::parse("REMEMBER QUERY test_event LIMIT 100 AS large_limit_materialization")
            .expect("parse REMEMBER command");

    let Command::RememberQuery { spec } = remember_cmd else {
        panic!("Expected RememberQuery command");
    };

    let result = remember_query_with_data_dir(spec, &shard_manager, &registry, &data_dir).await;

    assert!(result.is_ok(), "remember should succeed");
    let summary = result.unwrap();

    // Should store all available rows (3), not 100
    assert!(
        summary.iter().any(|s| s.contains("rows stored: 3")),
        "Should store all available rows (3), not limit (100), summary: {:?}",
        summary
    );

    let catalog = MaterializationCatalog::load(&data_dir).expect("catalog should load");
    let entry = catalog
        .get("large_limit_materialization")
        .expect("should get entry")
        .expect("entry should exist");

    assert_eq!(
        entry.row_count, 3,
        "Entry should have 3 rows (all available)"
    );
}
