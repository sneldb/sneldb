#[cfg(test)]
use crate::command::handlers::compare::orchestrator::ComparisonExecutionPipeline;
use crate::command::handlers::store;
use crate::command::parser::commands::plotql;
use crate::command::types::{AggSpec, Command, QueryCommand, TimeGranularity};
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};

fn create_test_query_command(event_type: &str) -> QueryCommand {
    QueryCommand {
        event_type: event_type.to_string(),
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
            field: "amount".to_string(),
        }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    }
}

#[tokio::test]
async fn test_new_creates_pipeline() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let queries = vec![create_test_query_command("test_event")];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    // Pipeline should be created successfully (no panic)
    // Note: queries field is private, so we can't assert directly
    let _ = pipeline;
}

#[tokio::test]
async fn test_execute_streaming_rejects_empty_queries() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let queries: Vec<QueryCommand> = vec![];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_err());
    let error_msg = match result {
        Err(e) => e,
        Ok(_) => panic!("Expected error"),
    };
    assert!(error_msg.contains("Comparison query requires at least one query"));
}

#[tokio::test]
async fn test_execute_streaming_single_query() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store some data
    let store_cmd = CommandFactory::store()
        .with_event_type("orders")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({ "amount": 100 }))
        .create();
    let (mut _r, mut w) = tokio::io::duplex(1024);
    store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        None,
        None,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(200)).await;

    let queries = vec![create_test_query_command("orders")];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Verify we can read from the stream
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    assert!(schema.columns().len() > 0);
}

#[tokio::test]
async fn test_execute_streaming_two_queries() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("amount", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("refunds", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store data for both event types
    for (evt, ctx, amount) in [("orders", "o1", 100), ("refunds", "r1", 50)] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amount }))
            .create();
        let (mut _r, mut w) = tokio::io::duplex(1024);
        store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            None,
            None,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    let queries = vec![
        create_test_query_command("orders"),
        create_test_query_command("refunds"),
    ];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Verify merged stream has correct schema (should have prefixed columns)
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();

    // Should have prefixed columns for both queries
    assert!(
        column_names.iter().any(|n| n.contains("orders")),
        "Expected orders column, got: {:?}",
        column_names
    );
    assert!(
        column_names.iter().any(|n| n.contains("refunds")),
        "Expected refunds column, got: {:?}",
        column_names
    );
}

#[tokio::test]
async fn test_execute_streaming_three_queries() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event_a", &[("amount", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_b", &[("amount", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_c", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store data for all event types
    for (evt, ctx, amount) in [
        ("event_a", "a1", 10),
        ("event_b", "b1", 20),
        ("event_c", "c1", 30),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amount }))
            .create();
        let (mut _r, mut w) = tokio::io::duplex(1024);
        store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            None,
            None,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    let queries = vec![
        create_test_query_command("event_a"),
        create_test_query_command("event_b"),
        create_test_query_command("event_c"),
    ];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Verify merged stream has schema for all three queries
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    assert!(schema.columns().len() >= 3); // At least 3 metric columns
}

#[tokio::test]
async fn test_execute_streaming_with_breakdown() {
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

    // Store data with regions
    for (evt, ctx, amount, region) in [
        ("orders", "o1", 100, "US"),
        ("orders", "o2", 200, "EU"),
        ("refunds", "r1", 50, "US"),
    ] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "region": region
            }))
            .create();
        let (mut _r, mut w) = tokio::io::duplex(1024);
        store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            None,
            None,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    // Create queries with breakdown
    let mut query1 = create_test_query_command("orders");
    query1.group_by = Some(vec!["region".to_string()]);
    let mut query2 = create_test_query_command("refunds");
    query2.group_by = Some(vec!["region".to_string()]);

    let queries = vec![query1, query2];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Verify schema includes breakdown column
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();

    assert!(
        column_names.iter().any(|n| n == "region"),
        "Expected region column, got: {:?}",
        column_names
    );
}

#[tokio::test]
async fn test_execute_streaming_with_time_bucket() {
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

    // Store data with timestamps
    for (evt, ctx, amount, timestamp) in [("sales", "s1", 100, 1000), ("returns", "r1", 50, 1000)] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({
                "amount": amount,
                "created_at": timestamp
            }))
            .create();
        let (mut _r, mut w) = tokio::io::duplex(1024);
        store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            None,
            None,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    // Create queries with time bucket
    let mut query1 = create_test_query_command("sales");
    query1.time_bucket = Some(TimeGranularity::Day);
    query1.time_field = Some("created_at".to_string());
    let mut query2 = create_test_query_command("returns");
    query2.time_bucket = Some(TimeGranularity::Day);
    query2.time_field = Some("created_at".to_string());

    let queries = vec![query1, query2];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Verify schema includes bucket column
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();

    assert!(
        column_names.iter().any(|n| n == "bucket"),
        "Expected bucket column, got: {:?}",
        column_names
    );
}

#[tokio::test]
async fn test_execute_streaming_handles_nonexistent_event_type() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Query for non-existent event type
    let queries = vec![create_test_query_command("nonexistent_event")];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    // Should succeed but return empty results (not error)
    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());
}

#[tokio::test]
async fn test_execute_streaming_parses_plotql_query() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("orders", &[("amount", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("refunds", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store data
    for (evt, ctx, amount) in [("orders", "o1", 100), ("refunds", "r1", 50)] {
        let store_cmd = CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amount }))
            .create();
        let (mut _r, mut w) = tokio::io::duplex(1024);
        store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            None,
            None,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    // Parse PlotQL query
    let cmd_str = "plot total(amount) of orders vs total(amount) of refunds";
    let cmd = plotql::parse(cmd_str).expect("parse comparison query");

    // Extract queries from Command::Compare
    if let Command::Compare { queries } = cmd {
        let pipeline =
            ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

        let result = pipeline.execute_streaming().await;

        assert!(result.is_ok());
        let stream_opt = result.unwrap();
        assert!(stream_opt.is_some());
    } else {
        panic!("Expected Command::Compare");
    }
}

#[tokio::test]
async fn test_execute_streaming_empty_results() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event_a", &[("amount", "int")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_b", &[("amount", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Don't store any data - should return empty results

    sleep(Duration::from_millis(100)).await;

    let queries = vec![
        create_test_query_command("event_a"),
        create_test_query_command("event_b"),
    ];

    let pipeline =
        ComparisonExecutionPipeline::new(&queries, &shard_manager, Arc::clone(&registry));

    let result = pipeline.execute_streaming().await;

    // Should succeed with empty results
    assert!(result.is_ok());
    let stream_opt = result.unwrap();
    assert!(stream_opt.is_some());

    // Stream should have valid schema even with no data
    let stream = stream_opt.unwrap();
    let schema = stream.schema();
    assert!(schema.columns().len() > 0);
}
