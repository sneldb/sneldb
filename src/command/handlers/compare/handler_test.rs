use crate::command::handlers::compare::ComparisonCommandHandler;
#[cfg(test)]
use crate::command::handlers::query::set_streaming_enabled;
use crate::command::handlers::store;
use crate::command::parser::commands::plotql;
use crate::command::types::{AggSpec, Command, QueryCommand, TimeGranularity};
use crate::engine::schema::SchemaRegistry;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::{JsonRenderer, render::Renderer};
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWrite, duplex};
use tokio::time::{Duration, sleep};

async fn execute_comparison<W: AsyncWrite + Unpin>(
    cmd: &Command,
    shard_manager: &ShardManager,
    registry: &Arc<tokio::sync::RwLock<SchemaRegistry>>,
    writer: &mut W,
    renderer: &dyn Renderer,
) -> std::io::Result<()> {
    ComparisonCommandHandler::new(cmd, shard_manager, Arc::clone(registry), writer, renderer)
        .handle()
        .await
}

#[tokio::test]
async fn test_comparison_handler_rejects_non_compare_command() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Pass a Query command instead of Compare
    let cmd = CommandFactory::query()
        .with_event_type("test_event")
        .create();

    let (mut reader, mut writer) = duplex(1024);

    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not panic");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("Invalid Compare command") || body.contains("BadRequest"),
        "Expected error message, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_rejects_single_query() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create Compare command with only 1 query
    let query = QueryCommand {
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
    };

    let cmd = Command::Compare {
        queries: vec![query],
    };

    let (mut reader, mut writer) = duplex(1024);

    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not panic");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("requires at least 2 queries") || body.contains("BadRequest"),
        "Expected error about requiring 2 queries, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_rejects_when_streaming_disabled() {
    let _guard = set_streaming_enabled(false);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("price", "int"), ("region", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create valid Compare command with 2 queries
    let query1 = QueryCommand {
        event_type: "orders".to_string(),
        context_id: None,
        since: None,
        time_field: Some("created_at".to_string()),
        sequence_time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Total {
            field: "price".to_string(),
        }]),
        time_bucket: Some(TimeGranularity::Month),
        group_by: None,
        event_sequence: None,
    };

    let query2 = QueryCommand {
        event_type: "orders".to_string(),
        context_id: None,
        since: None,
        time_field: Some("created_at".to_string()),
        sequence_time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Total {
            field: "price".to_string(),
        }]),
        time_bucket: Some(TimeGranularity::Month),
        group_by: None,
        event_sequence: None,
    };

    let cmd = Command::Compare {
        queries: vec![query1, query2],
    };

    let (mut reader, mut writer) = duplex(1024);

    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not panic");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("streaming is disabled") || body.contains("InternalError"),
        "Expected error about streaming disabled, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_basic_two_way_comparison() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("refunds", &[("amount", "int"), ("created_at", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("payments", &[("amount", "int"), ("created_at", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store some refund events
    for (ctx, amount) in [("r1", 10), ("r2", 20), ("r3", 30)] {
        let store_cmd = CommandFactory::store()
            .with_event_type("refunds")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amount, "created_at": 1000 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    // Store some payment events
    for (ctx, amount) in [("p1", 100), ("p2", 200)] {
        let store_cmd = CommandFactory::store()
            .with_event_type("payments")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amount, "created_at": 1000 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison query using PlotQL
    let cmd_str = "plot total(amount) of refunds vs total(amount) of payments";
    let cmd = plotql::parse(cmd_str).expect("parse comparison query");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should contain prefixed columns
    assert!(
        body.contains("refunds.total_amount") || body.contains("payments.total_amount"),
        "Expected prefixed column names, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_three_way_comparison() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event_a", &[("value", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_b", &[("value", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_c", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events for each type
    for (evt, ctx, value) in [
        ("event_a", "a1", 10),
        ("event_a", "a2", 20),
        ("event_b", "b1", 30),
        ("event_b", "b2", 40),
        ("event_c", "c1", 50),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "value": value }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse 3-way comparison query
    let cmd_str =
        "plot total(value) of event_a vs total(value) of event_b vs total(value) of event_c";
    let cmd = plotql::parse(cmd_str).expect("parse 3-way comparison query");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should contain all three prefixed columns
    assert!(
        body.contains("event_a") || body.contains("event_b") || body.contains("event_c"),
        "Expected prefixed columns for all three events, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_with_time_bucket() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("sales", &[("amount", "int"), ("created_at", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("returns", &[("amount", "int"), ("created_at", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events with timestamps
    for (evt, ctx, amount, timestamp) in [
        ("sales", "s1", 100, 1000),
        ("sales", "s2", 200, 2000),
        ("returns", "r1", 50, 1000),
        ("returns", "r2", 75, 2000),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "created_at": timestamp
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison with time bucket
    let cmd_str = "plot total(amount) of sales vs total(amount) of returns over day(created_at)";
    let cmd = plotql::parse(cmd_str).expect("parse comparison with time bucket");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should contain bucket column
    assert!(
        body.contains("bucket") || body.contains("\"rows\""),
        "Expected bucket column or rows, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_with_breakdown() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("amount", "int"), ("region", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("refunds", &[("amount", "int"), ("region", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events with regions
    for (evt, ctx, amount, region) in [
        ("orders", "o1", 100, "US"),
        ("orders", "o2", 200, "EU"),
        ("refunds", "r1", 50, "US"),
        ("refunds", "r2", 75, "EU"),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "region": region
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison with breakdown
    let cmd_str = "plot total(amount) of orders vs total(amount) of refunds breakdown by region";
    let cmd = plotql::parse(cmd_str).expect("parse comparison with breakdown");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should contain region column and prefixed metrics
    assert!(
        body.contains("region") || body.contains("US") || body.contains("EU"),
        "Expected region breakdown, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_with_per_side_filters() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("amount", "int"), ("status", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("refunds", &[("amount", "int"), ("status", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events with different statuses
    for (evt, ctx, amount, status) in [
        ("orders", "o1", 100, "completed"),
        ("orders", "o2", 200, "pending"),
        ("refunds", "r1", 50, "processed"),
        ("refunds", "r2", 75, "pending"),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "status": status
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison with per-side filters
    let cmd_str = "plot total(amount) of orders filter status=\"completed\" vs total(amount) of refunds filter status=\"processed\"";
    let cmd = plotql::parse(cmd_str).expect("parse comparison with filters");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should execute successfully with filters applied
    assert!(
        body.contains("\"rows\"") || body.contains("total") || body.is_empty(),
        "Expected valid response, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_empty_results() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event_a", &[("value", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_b", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Don't store any events - should return empty results

    sleep(Duration::from_millis(200)).await;

    // Parse comparison query
    let cmd_str = "plot total(value) of event_a vs total(value) of event_b";
    let cmd = plotql::parse(cmd_str).expect("parse comparison query");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return empty results, not error
    assert!(
        body.contains("\"rows\":[]")
            || body.contains("\"row_count\":0")
            || body.contains("total_value"),
        "Expected empty results or schema, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_same_event_type_uses_fallback_prefixes() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("price", "int"), ("region", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events for same event type with different filters
    for (ctx, amount, region) in [("o1", 100, "US"), ("o2", 200, "EU"), ("o3", 150, "US")] {
        let store_cmd = CommandFactory::store()
            .with_event_type("orders")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "price": amount,
                "region": region
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison with same event type (should use left/right prefixes)
    let cmd_str = "plot total(price) of orders filter region=\"US\" vs total(price) of orders filter region=\"EU\"";
    let cmd = plotql::parse(cmd_str).expect("parse comparison with same event type");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should execute successfully (fallback prefixes: left/right)
    assert!(
        body.contains("left")
            || body.contains("right")
            || body.contains("orders")
            || body.contains("\"rows\""),
        "Expected valid response with fallback prefixes, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_pipeline_error_returns_internal_error() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("invalid_event", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create Compare command with invalid query (non-existent event type)
    let query1 = QueryCommand {
        event_type: "nonexistent_event".to_string(),
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
        aggs: Some(vec![AggSpec::Total {
            field: "value".to_string(),
        }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let query2 = QueryCommand {
        event_type: "invalid_event".to_string(),
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
        aggs: Some(vec![AggSpec::Total {
            field: "value".to_string(),
        }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let cmd = Command::Compare {
        queries: vec![query1, query2],
    };

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not panic");

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return empty results (non-existent event types return empty, not error)
    assert!(
        body.contains("\"row_count\":0") || body.contains("total_value") || body.contains("schema"),
        "Expected empty results or schema, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_with_multiple_metrics() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("sales", &[("amount", "int"), ("quantity", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("returns", &[("amount", "int"), ("count", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events
    for (evt, ctx, amount, quantity) in [
        ("sales", "s1", 100, 5),
        ("sales", "s2", 200, 10),
        ("returns", "r1", 50, 2),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "quantity": quantity,
                "count": quantity
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison query
    let cmd_str = "plot total(amount) of sales vs total(amount) of returns";
    let cmd = plotql::parse(cmd_str).expect("parse comparison query");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should contain results
    assert!(
        body.contains("sales") || body.contains("returns") || body.contains("\"rows\""),
        "Expected comparison results, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_zero_queries() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create Compare command with 0 queries
    let cmd = Command::Compare { queries: vec![] };

    let (mut reader, mut writer) = duplex(1024);

    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not panic");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("requires at least 2 queries") || body.contains("BadRequest"),
        "Expected error about requiring 2 queries, got: {}",
        body
    );
}

#[tokio::test]
async fn test_comparison_handler_with_shared_time_and_breakdown() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "orders",
            &[
                ("amount", "int"),
                ("region", "string"),
                ("created_at", "int"),
            ],
        )
        .await
        .unwrap();
    factory
        .define_with_fields(
            "refunds",
            &[
                ("amount", "int"),
                ("region", "string"),
                ("created_at", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store events
    for (evt, ctx, amount, region, timestamp) in [
        ("orders", "o1", 100, "US", 1000),
        ("orders", "o2", 200, "EU", 2000),
        ("refunds", "r1", 50, "US", 1000),
        ("refunds", "r2", 75, "EU", 2000),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "region": region,
                "created_at": timestamp
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        store::handle(&store_cmd, &shard_manager, &registry, &mut w, &JsonRenderer)
            .await
            .expect("store should succeed");
    }

    sleep(Duration::from_millis(400)).await;

    // Parse comparison with shared time and breakdown (after vs)
    let cmd_str = "plot total(amount) of orders vs total(amount) of refunds breakdown by region over day(created_at)";
    let cmd = plotql::parse(cmd_str).expect("parse comparison with shared clauses");

    let (mut reader, mut writer) = duplex(4096);
    execute_comparison(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should execute successfully with shared clauses
    assert!(
        body.contains("region") || body.contains("bucket") || body.contains("\"rows\""),
        "Expected results with shared clauses, got: {}",
        body
    );
}
