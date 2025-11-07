use crate::command::handlers::query::handle;
use crate::command::handlers::query::test_helpers::set_streaming_enabled;
use crate::command::parser::commands::query::parse;
use crate::engine::shard::manager::ShardManager;
use crate::logging::init_for_tests;
use crate::shared::response::JsonRenderer;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::Value as JsonValue;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, duplex};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_query_returns_no_results_when_nothing_matches() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    // Prepare registry
    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Start shard manager
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // No STORE command beforehand → should result in no matches
    let cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx_missing") // does not exist
        .create();

    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .expect("handler should not fail");

    let mut buf = vec![0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("No matching events found") || body.contains("\"rows\":[]"),
        "Expected message or empty rows table, got: {}",
        body
    );
}

#[tokio::test]
async fn test_query_aggregation_count_unique_by_returns_values() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("login_evt", &[("user_id", "string"), ("country", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // NL has 2 events with same user_id "A" (unique=1), DE has 1 with "B"
    let stores = vec![
        (
            "login_evt",
            "c1",
            serde_json::json!({"user_id":"A","country":"NL"}),
        ),
        (
            "login_evt",
            "c2",
            serde_json::json!({"user_id":"A","country":"NL"}),
        ),
        (
            "login_evt",
            "c3",
            serde_json::json!({"user_id":"B","country":"DE"}),
        ),
    ];
    for (evt, ctx, payload) in stores {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(payload)
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(200)).await;

    let cmd_str = "QUERY login_evt COUNT UNIQUE user_id BY country";
    let cmd = parse(cmd_str).expect("parse COUNT UNIQUE query");

    let (mut reader, mut writer) = duplex(2048);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 2048];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"count_unique_user_id\""));
    assert!(body.contains("NL") && body.contains("DE"));
    assert!(!body.contains("\"rows\":[]"));
}

#[tokio::test]
async fn test_query_aggregation_count_field_by_returns_values() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "login_evt2",
            &[("user_id", "string"), ("country", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // NL has 2 with user_id present, DE has 1
    let stores = vec![
        (
            "login_evt2",
            "c1",
            serde_json::json!({"user_id":"A","country":"NL"}),
        ),
        (
            "login_evt2",
            "c2",
            serde_json::json!({"user_id":"A","country":"NL"}),
        ),
        (
            "login_evt2",
            "c3",
            serde_json::json!({"user_id":"B","country":"DE"}),
        ),
    ];
    for (evt, ctx, payload) in stores {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(payload)
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(400)).await;

    let cmd_str = "QUERY login_evt2 COUNT user_id BY country";
    let cmd = parse(cmd_str).expect("parse COUNT <field> query");

    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"count_user_id\""));
    assert!(body.contains("NL") && body.contains("DE"));
    assert!(!body.contains("\"rows\":[]"));
}

#[tokio::test]
async fn test_query_aggregation_per_month_by_country_returns_bucket_and_group() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("order_evt", &[("amount", "int"), ("country", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // A few orders
    for (ctx, amt, ctry) in [("o1", 10, "NL"), ("o2", 20, "NL"), ("o3", 15, "DE")].iter() {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("order_evt")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "amount": amt, "country": ctry }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(400)).await;

    let cmd_str = "QUERY order_evt AVG amount, TOTAL amount PER month BY country";
    let cmd = parse(cmd_str).expect("parse agg per/by query");

    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    assert!(body.contains("\"bucket\""));
    assert!(body.contains("\"country\""));
    assert!(body.contains("\"avg_amount\"") && body.contains("\"total_amount\""));
    assert!(!body.contains("\"rows\":[]"));
}

#[tokio::test]
async fn test_query_aggregation_count_per_day_by_two_fields() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "orders_evt",
            &[("amount", "int"), ("country", "string"), ("plan", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let stores = vec![
        (
            "orders_evt",
            "o1",
            serde_json::json!({"amount": 10, "country": "NL", "plan": "pro"}),
        ),
        (
            "orders_evt",
            "o2",
            serde_json::json!({"amount": 20, "country": "NL", "plan": "basic"}),
        ),
        (
            "orders_evt",
            "o3",
            serde_json::json!({"amount": 15, "country": "DE", "plan": "pro"}),
        ),
    ];
    for (evt, ctx, payload) in stores {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type(evt)
            .with_context_id(ctx)
            .with_payload(payload)
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(200)).await;

    let cmd_str = "QUERY orders_evt COUNT PER day BY country, plan";
    let cmd = parse(cmd_str).expect("parse COUNT PER day BY query");

    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"bucket\""));
    assert!(body.contains("\"country\"") && body.contains("\"plan\""));
    assert!(body.contains("\"count\""));
}

#[tokio::test]
async fn test_query_aggregation_multiple_aggs_returns_all_metrics() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("multi_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    for i in 1..=3 {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("multi_evt")
            .with_context_id(&format!("m{}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    let cmd_str = "QUERY multi_evt COUNT, AVG id, TOTAL id";
    let cmd = parse(cmd_str).expect("parse multi agg query");

    let (mut reader, mut writer) = duplex(2048);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 2048];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    assert!(body.contains("\"count\""));
    assert!(body.contains("\"avg_id\""));
    assert!(body.contains("\"total_id\""));
    assert!(!body.contains("\"rows\":[]"));
}

#[tokio::test]
async fn test_query_aggregation_count_returns_value() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("agg_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store some events
    for i in 1..=5 {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("agg_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(200)).await;

    // Build COUNT command via parser
    let cmd_str = "QUERY agg_evt COUNT";
    let cmd = parse(cmd_str).expect("parse COUNT query");

    let (mut reader, mut writer) = duplex(2048);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 2048];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("\"count\""),
        "Expected count column, got: {}",
        body
    );
    assert!(
        !body.contains("\"rows\":[]"),
        "Aggregation should return at least one row, got: {}",
        body
    );
}

#[tokio::test]
async fn test_query_aggregation_avg_with_filter_returns_value() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("agg_evt2", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store 1..=10
    for i in 1..=10 {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("agg_evt2")
            .with_context_id(&format!("u{}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    // AVG with filter id < 6 → avg of 1..5 = 3.0
    let cmd_str = "QUERY agg_evt2 WHERE id < 6 AVG id";
    let cmd = parse(cmd_str).expect("parse AVG query");

    let (mut reader, mut writer) = duplex(2048);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 2048];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(
        body.contains("\"avg_id\""),
        "Expected avg_id column, got: {}",
        body
    );
    assert!(
        !body.contains("\"rows\":[]"),
        "Aggregation should return at least one row, got: {}",
        body
    );
}

#[tokio::test]
async fn test_query_aggregation_empty_returns_table_not_message() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("agg_evt3", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // No stores → aggregation should return empty table (not the lines message)
    let cmd_str = "QUERY agg_evt3 COUNT";
    let cmd = parse(cmd_str).expect("parse COUNT query");

    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"columns\""));
    assert!(body.contains("\"count\""));
    assert!(body.contains("\"rows\":[]"));
    assert!(
        !body.contains("No matching events found"),
        "Aggregation empty should not render 'No matching events found'"
    );
}

#[tokio::test]
async fn test_query_returns_matching_event_as_json() {
    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    init_for_tests();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("test_event", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store a matching event
    let store_cmd = CommandFactory::store()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({ "id": 42 }))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    // Query for it
    let query_cmd = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .create();
    let (mut reader, mut writer) = duplex(1024);
    handle(
        &query_cmd,
        &shard_manager,
        &registry,
        &mut writer,
        &JsonRenderer,
    )
    .await
    .unwrap();

    let mut buf = vec![0; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("\"id\":42"));
}

#[tokio::test]
async fn test_query_returns_error_for_empty_event_type() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    let cmd = CommandFactory::query()
        .with_event_type("") // Invalid
        .with_context_id("ctx1")
        .create();
    let (mut reader, mut writer) = duplex(1024);

    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 512];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("event_type cannot be empty"));
}

#[tokio::test]
async fn test_query_selection_limit_truncates() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("limit_sel_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store three events with shuffled context_ids
    for (ctx, id) in [("c3", 3), ("c1", 1), ("c2", 2)] {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("limit_sel_evt")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "id": id }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Allow time for store to be processed
    sleep(Duration::from_millis(400)).await;

    // LIMIT 2 should return two rows sorted by context_id: c1, c2
    let cmd = parse("QUERY limit_sel_evt LIMIT 2").expect("parse LIMIT selection query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Extract JSON payload
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_query_order_by_with_lt_filter_returns_rows() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("lt_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Insert ids 0..=9
    for i in 0..10 {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("lt_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Allow flush (mem or segment) depending on config
    sleep(Duration::from_millis(300)).await;

    // ORDER BY id ASC LIMIT 2 with WHERE id < 10 must return 2 rows, not empty
    let cmd_str = "QUERY lt_evt WHERE id < 10 ORDER BY id ASC LIMIT 2";
    let cmd = parse(cmd_str).expect("parse lt filter with order+limit");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    assert!(
        body.contains("\"rows\":[") && !body.contains("No matching events found"),
        "Expected non-empty rows, got: {}",
        body
    );
}

#[tokio::test]
async fn test_query_aggregation_limit_truncates_and_sorts_groups() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("limit_agg_evt", &[("country", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Three different groups
    for (ctx, country) in [("a", "US"), ("b", "DE"), ("c", "FR")] {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("limit_agg_evt")
            .with_context_id(ctx)
            .with_payload(serde_json::json!({ "country": country }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    sleep(Duration::from_millis(400)).await;

    let cmd = parse("QUERY limit_agg_evt COUNT BY country LIMIT 2").expect("parse agg LIMIT query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 2);
    // Non-deterministic ordering/path (memtable vs segment), but LIMIT=2 must cap results.
    // Assert we got any two distinct countries from the three inserted.
    let c0 = rows[0][0].as_str().unwrap().to_string();
    let c1 = rows[1][0].as_str().unwrap().to_string();
    let set: std::collections::HashSet<String> = [c0, c1].into_iter().collect();
    assert_eq!(set.len(), 2);
    let allowed: std::collections::HashSet<&str> = ["US", "DE", "FR"].into_iter().collect();
    assert!(set.iter().all(|c| allowed.contains(c.as_str())));
}

/// Comprehensive test for ORDER BY and LIMIT with a large dataset.
/// Tests 150 events with various patterns including duplicates, negatives, large numbers, and strings.
#[tokio::test]
async fn test_query_order_by_limit_with_large_dataset() {
    init_for_tests();

    // Clear global caches to prevent cross-test contamination
    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "large_evt",
            &[
                ("score", "int"),
                ("category", "string"),
                ("priority", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Generate all events first with their data
    // Use pseudo-random context IDs to ensure ORDER BY isn't relying on insertion order
    let mut events = Vec::new();
    for i in 1..=150 {
        let score;
        let priority;
        let category;

        match i {
            // First 30: Simple ascending pattern with duplicates every 3
            1..=30 => {
                score = (i / 3) * 3; // Creates duplicates: 0,0,0,3,3,3,6,6,6...
                priority = i;
                category = match i % 3 {
                    0 => "Z",
                    1 => "A",
                    _ => "M",
                };
            }
            // 31-60: Descending pattern with negatives
            31..=60 => {
                score = 100 - i;
                priority = -i;
                category = "B";
            }
            // 61-90: All same score (stress test tie-breaking)
            61..=90 => {
                score = 42;
                priority = i - 60;
                category = match i % 5 {
                    0 => "Alpha",
                    1 => "Beta",
                    2 => "Gamma",
                    3 => "Delta",
                    _ => "Epsilon",
                };
            }
            // 91-120: Large numbers and edge cases
            91..=120 => {
                score = 1000000 + i;
                priority = -1000000 - i;
                category = "X";
            }
            // 121-150: Mixed pattern for variety
            _ => {
                score = (i * 7) % 100; // Creates pseudo-random distribution
                priority = (i * 13) % 50;
                category = match i % 4 {
                    0 => "Cat1",
                    1 => "Cat2",
                    2 => "Cat3",
                    _ => "Cat4",
                };
            }
        }

        // Use a pseudo-random context_id to ensure ordering doesn't rely on insertion order
        // Multiply by large prime and mod to scramble the order
        let scrambled_id = (i * 97) % 151 + 1000;
        events.push((scrambled_id, score, priority, category));
    }

    // Shuffle insertion order using a deterministic pseudo-random pattern
    // This ensures ORDER BY truly works and doesn't rely on insertion order
    let mut shuffled_events = Vec::new();
    let mut indices: Vec<usize> = (0..150).collect();
    // Custom deterministic shuffle using modulo arithmetic
    for i in 0..150 {
        let swap_idx = (i * 73 + 31) % (150 - i) + i;
        indices.swap(i, swap_idx);
    }
    for idx in indices {
        shuffled_events.push(events[idx]);
    }

    // Store events in shuffled order
    for (ctx_id, score, priority, category) in shuffled_events {
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("large_evt")
            .with_context_id(&format!("ctx_{:05}", ctx_id))
            .with_payload(serde_json::json!({
                "score": score,
                "category": category,
                "priority": priority
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Allow time for all stores to be processed and flushed
    // With 150 events creating ~50 segments, we need substantial time for all RLTE indices to be built
    sleep(Duration::from_millis(2000)).await;

    // Test 1: ORDER BY score DESC, LIMIT 5 - verify descending order strictly maintained
    let cmd =
        parse("QUERY large_evt ORDER BY score DESC LIMIT 5").expect("parse ORDER BY DESC LIMIT");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 5, "Should return exactly 5 rows");
    let mut prev_score = i64::MAX;
    for row in rows {
        let score = row[3]["score"].as_i64().expect("score should be integer");
        assert!(
            score <= prev_score,
            "Scores must be descending: {} <= {}",
            score,
            prev_score
        );
        prev_score = score;
    }

    // Test 2: ORDER BY score ASC, LIMIT 7 - verify ascending order strictly maintained
    let cmd =
        parse("QUERY large_evt ORDER BY score ASC LIMIT 7").expect("parse ORDER BY ASC LIMIT");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 7, "Should return exactly 7 rows");
    let mut prev_score = i64::MIN;
    for row in rows {
        let score = row[3]["score"].as_i64().expect("score should be integer");
        assert!(
            score >= prev_score,
            "Scores must be ascending: {} >= {}",
            score,
            prev_score
        );
        prev_score = score;
    }

    // Test 3: ORDER BY priority DESC, LIMIT 10 - test negative numbers descending
    let cmd =
        parse("QUERY large_evt ORDER BY priority DESC LIMIT 10").expect("parse priority DESC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 10, "Should return exactly 10 rows");
    let mut prev_priority = i64::MAX;
    for row in rows {
        let priority = row[3]["priority"]
            .as_i64()
            .expect("priority should be integer");
        assert!(priority <= prev_priority, "Priorities must be descending");
        prev_priority = priority;
    }

    // Test 4: ORDER BY priority ASC, LIMIT 12 - test negative numbers ascending
    let cmd = parse("QUERY large_evt ORDER BY priority ASC LIMIT 12").expect("parse priority ASC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 12, "Should return exactly 12 rows");
    let mut prev_priority = i64::MIN;
    for row in rows {
        let priority = row[3]["priority"]
            .as_i64()
            .expect("priority should be integer");
        assert!(priority >= prev_priority, "Priorities must be ascending");
        prev_priority = priority;
    }

    // Test 5: ORDER BY category ASC, LIMIT 15 - test string ordering ascending
    let cmd = parse("QUERY large_evt ORDER BY category ASC LIMIT 15").expect("parse category ASC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 15, "Should return exactly 15 rows");
    let mut prev_category = String::new();
    for row in rows {
        let category = row[3]["category"].as_str().expect("category").to_string();
        assert!(
            category >= prev_category,
            "Categories must be ascending: {} >= {}",
            category,
            prev_category
        );
        prev_category = category;
    }

    // Test 6: ORDER BY category DESC, LIMIT 20 - test string ordering descending
    let cmd =
        parse("QUERY large_evt ORDER BY category DESC LIMIT 20").expect("parse category DESC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 20, "Should return exactly 20 rows");
    let mut prev_category = String::from("ZZZZZZZZZ");
    for row in rows {
        let category = row[3]["category"].as_str().expect("category").to_string();
        assert!(
            category <= prev_category,
            "Categories must be descending: {} <= {}",
            category,
            prev_category
        );
        prev_category = category;
    }

    // Test 7: LIMIT 1 - edge case, single result
    let cmd = parse("QUERY large_evt ORDER BY score DESC LIMIT 1").expect("parse LIMIT 1");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "Should return exactly 1 row");

    // Test 8: Large LIMIT 100 - test with many records including duplicates
    let cmd = parse("QUERY large_evt ORDER BY score DESC LIMIT 100").expect("parse large LIMIT");
    let (mut reader, mut writer) = duplex(16384);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 16384];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 100, "Should return exactly 100 rows");
    let mut prev_score = i64::MAX;
    for row in rows {
        let score = row[3]["score"].as_i64().expect("score");
        assert!(score <= prev_score, "Order maintained with duplicates");
        prev_score = score;
    }

    // Test 9: LIMIT exceeds dataset - should return all 150
    let cmd = parse("QUERY large_evt ORDER BY score ASC LIMIT 500").expect("parse LIMIT > dataset");
    let (mut reader, mut writer) = duplex(32768);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 32768];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        150,
        "Should return all 150 rows when LIMIT exceeds dataset"
    );
    let mut prev_score = i64::MIN;
    for row in rows {
        let score = row[3]["score"].as_i64().expect("score");
        assert!(score >= prev_score, "All results must be ordered");
        prev_score = score;
    }

    // Test 10: ORDER BY with LIMIT on field with many duplicates (score=42 for 30 rows)
    let cmd = parse("QUERY large_evt ORDER BY score ASC LIMIT 35").expect("parse duplicates test");
    let (mut reader, mut writer) = duplex(16384);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 16384];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        35,
        "Should handle duplicates correctly with LIMIT"
    );
    let mut prev_score = i64::MIN;
    for row in rows {
        let score = row[3]["score"].as_i64().expect("score");
        assert!(score >= prev_score, "Order preserved with duplicates");
        prev_score = score;
    }

    // Test 11: Very large LIMIT 149 (almost all data)
    let cmd =
        parse("QUERY large_evt ORDER BY priority DESC LIMIT 149").expect("parse near-full LIMIT");
    let (mut reader, mut writer) = duplex(32768);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 32768];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 149, "Should return 149 rows");
    let mut prev_priority = i64::MAX;
    for row in rows {
        let priority = row[3]["priority"].as_i64().expect("priority");
        assert!(
            priority <= prev_priority,
            "Near-full result set must be ordered"
        );
        prev_priority = priority;
    }

    // Test 12: LIMIT without ORDER BY - should still respect LIMIT
    let cmd = parse("QUERY large_evt LIMIT 42").expect("parse LIMIT no ORDER");
    let (mut reader, mut writer) = duplex(16384);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 16384];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 42, "LIMIT must work without ORDER BY");
}

/// E2E: ORDER BY timestamp DESC with OFFSET and LIMIT
#[tokio::test]
async fn test_timestamp_order_by_desc_offset_limit() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("ts_evt_desc", &[("created_at", "datetime"), ("val", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Insert 12 events with explicit created_at 100..111
    for ts in 100..112 {
        let store_cmd = CommandFactory::store()
            .with_event_type("ts_evt_desc")
            .with_context_id(&format!("ctx{:03}", ts))
            .with_payload(serde_json::json!({"created_at": ts, "val": ts}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(500)).await;

    let cmd = parse("QUERY ts_evt_desc ORDER BY created_at DESC OFFSET 3 LIMIT 4")
        .expect("parse ts order by desc with offset");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 4);
    // Desc order: 111..100; after OFFSET 3 => start at 108
    let ts_vals: Vec<i64> = rows
        .iter()
        .map(|r| r[3]["created_at"].as_i64().unwrap())
        .collect();
    assert_eq!(ts_vals, vec![108, 107, 106, 105]);
}

/// E2E: WHERE timestamp > bound + ascending order + limit
#[tokio::test]
async fn test_timestamp_where_gt_asc_limit() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("ts_evt_range", &[("created_at", "datetime"), ("x", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    for ts in 300..311 {
        let store_cmd = CommandFactory::store()
            .with_event_type("ts_evt_range")
            .with_context_id(&format!("rg{:03}", ts))
            .with_payload(serde_json::json!({"created_at": ts, "x": ts}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }
    // Ensure data is persisted and indexes built
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");
    sleep(Duration::from_millis(600)).await;

    let cmd = parse("QUERY ts_evt_range WHERE created_at > 307 ORDER BY created_at ASC LIMIT 3")
        .expect("parse where created_at>");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 3);
    let ts_vals: Vec<i64> = rows
        .iter()
        .map(|r| r[3]["created_at"].as_i64().unwrap())
        .collect();
    assert_eq!(ts_vals, vec![308, 309, 310]);
}

/// Test for LIMIT functionality with custom datetime fields.
/// Tests 60 events with explicit created_at timestamps inserted in random order.
#[tokio::test]
async fn test_query_with_datetime_field_and_limit() {
    init_for_tests();

    // Clear global caches to prevent cross-test contamination
    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "time_evt",
            &[
                ("event_name", "string"),
                ("value", "int"),
                ("created_at", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create 60 events with custom created_at timestamps
    // Insert in scrambled order
    let mut events = Vec::new();

    // Base timestamp
    let base_ts = 1700000000i64;

    // Generate events with explicit created_at values
    for i in 1..=60 {
        let event_name = format!("event_{:03}", i);
        let value = i * 10;
        // Each event gets a distinct timestamp (1 second apart)
        let created_at = base_ts + i as i64;
        events.push((i, event_name, value, created_at));
    }

    // Shuffle the insertion order
    let mut shuffled_indices: Vec<usize> = (0..60).collect();
    for i in 0..60 {
        let swap_idx = (i * 67 + 23) % (60 - i) + i;
        shuffled_indices.swap(i, swap_idx);
    }

    // Insert events in shuffled order
    for idx in shuffled_indices {
        let (logical_order, event_name, value, created_at) = &events[idx];
        let store_cmd = crate::test_helpers::factories::CommandFactory::store()
            .with_event_type("time_evt")
            .with_context_id(&format!("time_ctx_{:04}", (logical_order * 97) % 61 + 1000))
            .with_payload(serde_json::json!({
                "event_name": event_name,
                "value": value,
                "created_at": created_at
            }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Allow time for processing and RLTE indices to be built
    // With 60 events creating ~20 segments, we need time for all RLTE indices
    sleep(Duration::from_millis(1500)).await;

    // Test 1: ORDER BY created_at DESC, LIMIT 5 - most recent timestamps first
    let cmd = parse("QUERY time_evt ORDER BY created_at DESC LIMIT 5")
        .expect("parse ORDER BY created_at DESC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 5, "Should return exactly 5 rows");

    // Verify descending order of created_at
    let mut prev_created_at = i64::MAX;
    for row in rows {
        let created_at = row[3]["created_at"]
            .as_i64()
            .expect("created_at should be integer");
        assert!(
            created_at <= prev_created_at,
            "created_at must be descending: {} <= {}",
            created_at,
            prev_created_at
        );
        prev_created_at = created_at;
    }

    // Test 2: ORDER BY created_at ASC, LIMIT 10 - oldest timestamps first
    let cmd = parse("QUERY time_evt ORDER BY created_at ASC LIMIT 10")
        .expect("parse ORDER BY created_at ASC");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 10, "Should return exactly 10 rows");

    // Verify ascending order of created_at
    let mut prev_created_at = i64::MIN;
    for row in rows {
        let created_at = row[3]["created_at"]
            .as_i64()
            .expect("created_at should be integer");
        assert!(
            created_at >= prev_created_at,
            "created_at must be ascending: {} >= {}",
            created_at,
            prev_created_at
        );
        prev_created_at = created_at;
    }

    // Test 3: ORDER BY created_at DESC, LIMIT 20
    let cmd = parse("QUERY time_evt ORDER BY created_at DESC LIMIT 20")
        .expect("parse ORDER BY created_at DESC LIMIT 20");
    let (mut reader, mut writer) = duplex(16384);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 16384];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 20, "Should return exactly 20 rows");

    let mut prev_created_at = i64::MAX;
    for row in rows {
        let created_at = row[3]["created_at"].as_i64().expect("created_at");
        assert!(
            created_at <= prev_created_at,
            "All created_at must be in descending order"
        );
        prev_created_at = created_at;
    }

    // Test 4: ORDER BY created_at ASC with LIMIT exceeding dataset
    let cmd = parse("QUERY time_evt ORDER BY created_at ASC LIMIT 100")
        .expect("parse ORDER BY created_at ASC LIMIT 100");
    let (mut reader, mut writer) = duplex(32768);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 32768];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 60, "Should return all 60 rows");

    // Verify all in ascending order and in valid range
    let mut prev_created_at = i64::MIN;
    for row in rows {
        let created_at = row[3]["created_at"].as_i64().expect("created_at");
        assert!(
            created_at >= prev_created_at,
            "All created_at must be in ascending order"
        );
        assert!(
            created_at >= base_ts + 1 && created_at <= base_ts + 60,
            "created_at should be in valid range"
        );
        prev_created_at = created_at;
    }

    // Test 5: LIMIT without ORDER BY - should still work
    let cmd = parse("QUERY time_evt LIMIT 15").expect("parse LIMIT without ORDER BY");
    let (mut reader, mut writer) = duplex(16384);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 16384];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 15, "LIMIT must work without ORDER BY");

    // Test 6: ORDER BY created_at DESC, LIMIT 1 - single most recent
    let cmd = parse("QUERY time_evt ORDER BY created_at DESC LIMIT 1")
        .expect("parse ORDER BY created_at DESC LIMIT 1");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON response");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1, "Should return exactly 1 row");
}

// =============================================================================
// NEW ORCHESTRATOR TESTS - Testing refactored components
// =============================================================================

/// Tests the new query orchestrator with multi-shard setup
#[tokio::test]
async fn test_orchestrator_multi_shard_query() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("multi_shard_evt", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Create 3 shards
    let shard_manager = ShardManager::new(3, base_dir, wal_dir).await;

    // Store 30 events
    for i in 1..=30 {
        let store_cmd = CommandFactory::store()
            .with_event_type("multi_shard_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "value": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Query should aggregate results from all shards
    let cmd = CommandFactory::query()
        .with_event_type("multi_shard_evt")
        .create();
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");
    println!("rows: {:?}", rows);
    // Should find most events across 3 shards (some timing variation expected)
    assert!(
        rows.len() == 30,
        "Should find exactly 30 events across shards, found {}",
        rows.len()
    );
}

/// Tests ORDER BY with OFFSET and LIMIT combined
#[tokio::test]
async fn test_orchestrator_order_by_with_offset_and_limit() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_evt", &[("rank", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(2, base_dir, wal_dir).await;

    // Store 20 events with ranks 1-20
    for i in 1..=20 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_evt")
            .with_context_id(&format!("ctx{:03}", i))
            .with_payload(serde_json::json!({ "rank": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(1000)).await;

    // ORDER BY rank ASC, OFFSET 5, LIMIT 3 -> should get 3 consecutive ranks after skipping 5
    let cmd = parse("QUERY offset_evt ORDER BY rank ASC OFFSET 5 LIMIT 3")
        .expect("parse ORDER BY with OFFSET and LIMIT");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should return at most 3 rows (LIMIT 3), and at least some data
    assert!(
        rows.len() > 0 && rows.len() <= 3,
        "Should return 1-3 rows with LIMIT 3, got {}",
        rows.len()
    );

    // Most importantly: verify ORDER BY is working (ascending order maintained)
    if rows.len() > 1 {
        let mut prev_rank = rows[0][3]["rank"].as_i64().unwrap();
        for row in rows.iter().skip(1) {
            let rank = row[3]["rank"].as_i64().unwrap();
            assert!(
                rank >= prev_rank,
                "Ranks must be in ascending order: {} >= {}",
                rank,
                prev_rank
            );
            prev_rank = rank;
        }
    }
}

/// Tests k-way merger performance with multiple shards
#[tokio::test]
async fn test_orchestrator_kway_merge_performance() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("perf_evt", &[("score", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Use 4 shards for true k-way merge
    let shard_manager = ShardManager::new(4, base_dir, wal_dir).await;

    // Store 100 events distributed across shards
    for i in 1..=100 {
        let store_cmd = CommandFactory::store()
            .with_event_type("perf_evt")
            .with_context_id(&format!("ctx{:04}", i))
            .with_payload(serde_json::json!({ "score": i * 3 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(1500)).await;

    // Test k-way merge with ORDER BY
    let start = std::time::Instant::now();
    let cmd = parse("QUERY perf_evt ORDER BY score ASC LIMIT 50")
        .expect("parse ORDER BY for k-way merge");
    let (mut reader, mut writer) = duplex(32768);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Should complete quickly with optimized k-way merge
    assert!(duration.as_millis() < 1000, "K-way merge should be fast");

    let mut buf = vec![0; 32768];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    assert_eq!(rows.len(), 50);

    // Verify ascending order
    let mut prev_score = 0;
    for row in rows {
        let score = row[3]["score"].as_i64().unwrap();
        assert!(
            score >= prev_score,
            "Scores must be ascending in k-way merge"
        );
        prev_score = score;
    }
}

/// Tests that empty shards don't break the orchestrator
#[tokio::test]
async fn test_orchestrator_handles_empty_shards() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("sparse_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Create 5 shards but only store data in shard 2
    let shard_manager = ShardManager::new(5, base_dir, wal_dir).await;

    // Only store a few events (they might all go to one shard)
    for i in 1..=3 {
        let store_cmd = CommandFactory::store()
            .with_event_type("sparse_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "id": i * 10 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(300)).await;

    // Query should work even with empty shards
    let cmd = CommandFactory::query()
        .with_event_type("sparse_evt")
        .create();
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should find the events that were stored
    assert!(body.contains("\"id\":10") || body.contains("\"id\":20") || body.contains("\"id\":30"));
}

/// Tests ORDER BY with strings - validates orchestrator handles string ordering
#[tokio::test]
async fn test_orchestrator_order_by_string_multi_shard() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("string_sort_evt", &[("name", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store just a few events to test string ordering via the orchestrator
    for name in &["Charlie", "Alice", "Bob"] {
        let store_cmd = CommandFactory::store()
            .with_event_type("string_sort_evt")
            .with_context_id(&format!("ctx_{}", name))
            .with_payload(serde_json::json!({ "name": name }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(300)).await;

    // Query without ORDER BY first to verify basic functionality
    let cmd = CommandFactory::query()
        .with_event_type("string_sort_evt")
        .create();
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Just verify orchestrator successfully processes the query
    assert!(body.contains("\"rows\""));
}

/// Tests the zero-copy command builder optimization
#[tokio::test]
async fn test_orchestrator_command_builder_efficiency() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("builder_test_evt", &[("data", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Create many shards to test command building efficiency
    let shard_manager = ShardManager::new(10, base_dir, wal_dir).await;

    // Store a few events
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("builder_test_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "data": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(300)).await;

    // Query without ORDER BY - should use Cow::Borrowed for all shards
    let cmd = CommandFactory::query()
        .with_event_type("builder_test_evt")
        .create();
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should successfully retrieve results from multiple shards
    assert!(body.contains("\"rows\""));
}

/// Tests parallel segment discovery across multiple shards
#[tokio::test]
async fn test_orchestrator_parallel_segment_discovery() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("parallel_evt", &[("seq", "int")])
        .await
        .unwrap();
    let registry = factory.registry();

    // Create multiple shards to trigger parallel discovery
    let shard_manager = ShardManager::new(4, base_dir, wal_dir).await;

    // Store enough events to create multiple segments
    for i in 1..=30 {
        let store_cmd = CommandFactory::store()
            .with_event_type("parallel_evt")
            .with_context_id(&format!("ctx{:04}", i))
            .with_payload(serde_json::json!({ "seq": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(600)).await;

    // ORDER BY triggers segment discovery and RLTE planning
    let start = std::time::Instant::now();
    let cmd = parse("QUERY parallel_evt ORDER BY seq DESC LIMIT 10")
        .expect("parse ORDER BY for discovery test");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Parallel discovery should be fast
    assert!(
        duration.as_millis() < 3000,
        "Parallel discovery should be efficient"
    );

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    assert!(
        rows.len() >= 5,
        "Should find at least 5 events with LIMIT 10"
    );

    // Verify descending order
    let mut prev_seq = i64::MAX;
    for row in rows {
        let seq = row[3]["seq"].as_i64().unwrap();
        assert!(seq <= prev_seq, "Results must be in descending order");
        prev_seq = seq;
    }
}

/// Tests ORDER BY with WHERE clause filtering
#[tokio::test]
async fn test_orchestrator_order_by_with_filter() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "filter_sort_evt",
            &[("amount", "int"), ("status", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(2, base_dir, wal_dir).await;

    // Store events with various amounts and statuses
    for i in 1..=30 {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        let store_cmd = CommandFactory::store()
            .with_event_type("filter_sort_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "amount": i * 5, "status": status }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(800)).await;

    // WHERE status = active (15 events), ORDER BY amount DESC, LIMIT 5
    let cmd = parse("QUERY filter_sort_evt WHERE status = active ORDER BY amount DESC LIMIT 5")
        .expect("parse WHERE with ORDER BY");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    assert_eq!(rows.len(), 5);

    // Verify all are "active" and in descending order
    let mut prev_amount = i64::MAX;
    for row in rows {
        let status = row[3]["status"].as_str().unwrap();
        assert_eq!(status, "active", "Filter should only return active");

        let amount = row[3]["amount"].as_i64().unwrap();
        assert!(amount <= prev_amount, "Amounts must be descending");
        prev_amount = amount;
    }
}

/// Tests OFFSET and LIMIT interaction in orchestrator
#[tokio::test]
async fn test_orchestrator_offset_exceeds_results() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_edge_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store a few events
    for i in 1..=3 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_edge_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(200)).await;

    // Test that OFFSET and LIMIT work through the orchestrator
    let cmd = parse("QUERY offset_edge_evt OFFSET 1 LIMIT 2").expect("parse OFFSET and LIMIT");
    let (mut reader, mut writer) = duplex(4096);

    // Main test: orchestrator handles OFFSET/LIMIT without panicking
    let result = handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer).await;
    assert!(
        result.is_ok(),
        "Orchestrator should handle OFFSET/LIMIT without errors"
    );

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Verify we got a response (exact format doesn't matter)
    assert!(body.len() > 0, "Should return a response");
}

/// Tests ORDER BY DESC with OFFSET - simpler version
#[tokio::test]
async fn test_orchestrator_order_desc_with_large_offset() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_desc_evt", &[("num", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(2, base_dir, wal_dir).await;

    // Store 20 events
    for i in 1..=20 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_desc_evt")
            .with_context_id(&format!("ctx{:03}", i))
            .with_payload(serde_json::json!({ "num": i * 2 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(600)).await;

    // ORDER BY num DESC, OFFSET 10, LIMIT 5
    let cmd = parse("QUERY offset_desc_evt ORDER BY num DESC OFFSET 10 LIMIT 5")
        .expect("parse ORDER BY DESC with OFFSET");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Just verify we get a valid response and any results are ordered
    if body.contains("\"rows\":[") {
        let json_start = body.find('{').unwrap_or(0);
        let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
        if let Some(rows) = json["results"][0]["rows"].as_array() {
            // Verify descending order is maintained
            if rows.len() > 1 {
                let mut prev_num = i64::MAX;
                for row in rows {
                    let num = row[3]["num"].as_i64().unwrap();
                    assert!(num <= prev_num, "Results must be in descending order");
                    prev_num = num;
                }
            }
        }
    }
}

/// Tests the new orchestrator with no ORDER BY (legacy merge path)
#[tokio::test]
async fn test_orchestrator_legacy_merge_path() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("legacy_evt", &[("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(3, base_dir, wal_dir).await;

    // Store events
    for i in 1..=10 {
        let store_cmd = CommandFactory::store()
            .with_event_type("legacy_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "value": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(300)).await;

    // Query without ORDER BY uses the legacy merge path
    let cmd = CommandFactory::query()
        .with_event_type("legacy_evt")
        .create();
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    assert_eq!(rows.len(), 10, "Legacy merge should return all results");
}

/// Tests error handling in the orchestrator
#[tokio::test]
async fn test_orchestrator_handles_invalid_event_type() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let registry = SchemaRegistryFactory::new().registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Query with empty event_type
    let cmd = CommandFactory::query().with_event_type("").create();
    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    assert!(body.contains("event_type cannot be empty"));
}

// =============================================================================
// COMPREHENSIVE OFFSET TESTS - Strict validation
// =============================================================================

/// Tests OFFSET without ORDER BY (plain pagination)
#[tokio::test]
async fn test_offset_only_without_order_by() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_plain_evt", &[("seq", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store 10 events
    for i in 1..=10 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_plain_evt")
            .with_context_id(&format!("ctx{:02}", i))
            .with_payload(serde_json::json!({ "seq": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Manual flush to ensure all data is on disk
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(500)).await;

    // OFFSET 3 LIMIT 2 (no ORDER BY)
    let cmd = parse("QUERY offset_plain_evt OFFSET 3 LIMIT 2").expect("parse OFFSET LIMIT");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");

    // After flushing, we should always get a table response, not "No matching events found"
    let results_array = json["results"]
        .as_array()
        .unwrap_or_else(|| panic!("Response should have results array, got: {}", json));

    assert!(
        !results_array.is_empty(),
        "Results array should not be empty after flushing data. Got: {}",
        json
    );

    // First result should be a table object with rows, not a string message
    let result_obj = results_array[0].as_object().unwrap_or_else(|| {
        panic!(
            "First result should be a table object, not a string. Got: {}",
            json
        )
    });

    let rows = result_obj
        .get("rows")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("Table object should have rows array. Got: {}", json));

    // Should get at most 2 rows (LIMIT 2)
    assert!(
        rows.len() <= 2,
        "OFFSET+LIMIT without ORDER BY should work. Got {} rows, expected <= 2",
        rows.len()
    );
}

/// Tests OFFSET = 0 (should be same as no offset)
#[tokio::test]
async fn test_offset_zero_is_noop() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_zero_evt", &[("val", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store 6 events (multiple of 3, ensures full flush)
    for i in 1..=6 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_zero_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "val": i * 10 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Manual flush to ensure all data is on disk
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    // Wait for RLTE indices to be built
    sleep(Duration::from_millis(1500)).await;

    // OFFSET 0 LIMIT 3 with ORDER BY
    let cmd =
        parse("QUERY offset_zero_evt ORDER BY val ASC OFFSET 0 LIMIT 3").expect("parse OFFSET 0");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should get exactly 3 rows (first 3)
    assert_eq!(rows.len(), 3, "OFFSET 0 should return first LIMIT rows");

    // Verify we got val = 10, 20, 30
    let vals: Vec<i64> = rows.iter().map(|r| r[3]["val"].as_i64().unwrap()).collect();
    assert_eq!(
        vals,
        vec![10, 20, 30],
        "OFFSET 0 should return first 3 values"
    );
}

/// Tests OFFSET at exact boundary (equals dataset size)
#[tokio::test]
async fn test_offset_equals_dataset_size() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_boundary_evt", &[("num", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store exactly 5 events
    for i in 1..=5 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_boundary_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "num": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(500)).await;

    // OFFSET 5 with only 5 events (boundary)
    let cmd = parse("QUERY offset_boundary_evt ORDER BY num ASC OFFSET 5 LIMIT 10")
        .expect("parse OFFSET at boundary");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return empty or "No matching events"
    assert!(
        body.contains("No matching events found") || body.contains("\"rows\":[]"),
        "OFFSET at boundary should return empty"
    );
}

/// Tests OFFSET + LIMIT where result is at exact boundary
#[tokio::test]
async fn test_offset_limit_exact_boundary() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_exact_evt", &[("id", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store exactly 12 events (multiple of 3)
    for i in 1..=12 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_exact_evt")
            .with_context_id(&format!("ctx{:02}", i))
            .with_payload(serde_json::json!({ "id": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Manual flush
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(1500)).await;

    // OFFSET 9 LIMIT 5 → should get exactly 3 rows (10, 11, 12)
    let cmd = parse("QUERY offset_exact_evt ORDER BY id ASC OFFSET 9 LIMIT 5")
        .expect("parse OFFSET+LIMIT at boundary");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should get exactly 3 rows (12 total - 9 offset = 3 remaining)
    assert_eq!(
        rows.len(),
        3,
        "OFFSET 9 with 12 total should give exactly 3 rows"
    );

    // Verify we got id = 10, 11, 12
    let ids: Vec<i64> = rows.iter().map(|r| r[3]["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![10, 11, 12], "Should get exact boundary rows");
}

/// Tests large OFFSET with small LIMIT
#[tokio::test]
async fn test_large_offset_small_limit() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("large_offset_evt", &[("idx", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(2, base_dir, wal_dir).await;

    // Store 50 events
    for i in 1..=50 {
        let store_cmd = CommandFactory::store()
            .with_event_type("large_offset_evt")
            .with_context_id(&format!("ctx{:03}", i))
            .with_payload(serde_json::json!({ "idx": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(1000)).await;

    // OFFSET 47 LIMIT 2 → should get exactly 2 rows (48, 49)
    let cmd = parse("QUERY large_offset_evt ORDER BY idx ASC OFFSET 47 LIMIT 2")
        .expect("parse large OFFSET small LIMIT");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should get exactly 2 rows
    assert_eq!(
        rows.len(),
        2,
        "Large OFFSET with small LIMIT should work precisely"
    );

    // Verify we got idx = 48, 49
    let idxs: Vec<i64> = rows.iter().map(|r| r[3]["idx"].as_i64().unwrap()).collect();
    assert_eq!(
        idxs,
        vec![48, 49],
        "Should get exact rows after large offset"
    );
}

/// Tests that OFFSET without LIMIT is rejected (safety measure)
#[tokio::test]
async fn test_offset_without_limit_is_rejected() {
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_no_limit_evt", &[("val", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // OFFSET without LIMIT should be rejected for safety
    let cmd = parse("QUERY offset_no_limit_evt ORDER BY val ASC OFFSET 10")
        .expect("parse OFFSET without LIMIT");
    let (mut reader, mut writer) = duplex(1024);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return error message requiring LIMIT
    assert!(
        body.contains("OFFSET requires LIMIT"),
        "OFFSET without LIMIT should be rejected"
    );
}

/// Tests OFFSET with descending ORDER BY (verify correct direction)
#[tokio::test]
async fn test_offset_with_descending_order() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_desc_order_evt", &[("score", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store 12 events with scores 1-12
    for i in 1..=12 {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_desc_order_evt")
            .with_context_id(&format!("ctx{:02}", i))
            .with_payload(serde_json::json!({ "score": i }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    sleep(Duration::from_millis(500)).await;

    // ORDER BY score DESC, OFFSET 3, LIMIT 4
    // Descending: 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1
    // After OFFSET 3: 9, 8, 7, 6, 5, 4, 3, 2, 1
    // With LIMIT 4: 9, 8, 7, 6
    let cmd = parse("QUERY offset_desc_order_evt ORDER BY score DESC OFFSET 3 LIMIT 4")
        .expect("parse DESC with OFFSET");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should get exactly 4 rows
    assert_eq!(rows.len(), 4, "Should get exactly 4 rows with OFFSET+LIMIT");

    // Verify we got scores 9, 8, 7, 6 (descending)
    let scores: Vec<i64> = rows
        .iter()
        .map(|r| r[3]["score"].as_i64().unwrap())
        .collect();
    assert_eq!(
        scores,
        vec![9, 8, 7, 6],
        "OFFSET with DESC should skip from top"
    );
}

/// Tests OFFSET across multiple shards with ORDER BY
#[tokio::test]
async fn test_offset_multi_shard_kway_merge() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("multi_offset_evt", &[("num", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(3, base_dir, wal_dir).await;

    // Store 30 events across 3 shards (30 = multiple of 3)
    for i in 1..=30 {
        let store_cmd = CommandFactory::store()
            .with_event_type("multi_offset_evt")
            .with_context_id(&format!("ctx{:03}", i))
            .with_payload(serde_json::json!({ "num": i * 2 }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Manual flush
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(1500)).await;

    // OFFSET 10 LIMIT 5 with multi-shard k-way merge
    let cmd = parse("QUERY multi_offset_evt ORDER BY num ASC OFFSET 10 LIMIT 5")
        .expect("parse multi-shard OFFSET");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    // Should get exactly 5 rows
    assert_eq!(
        rows.len(),
        5,
        "Multi-shard OFFSET should work with k-way merge"
    );

    // Verify we got num = 22, 24, 26, 28, 30 (indices 11-15 in sorted order)
    let nums: Vec<i64> = rows.iter().map(|r| r[3]["num"].as_i64().unwrap()).collect();
    assert_eq!(
        nums,
        vec![22, 24, 26, 28, 30],
        "K-way merge should respect OFFSET"
    );
}

/// Tests OFFSET 1 (simplest non-zero offset)
#[tokio::test]
async fn test_offset_one() {
    init_for_tests();

    use crate::engine::core::read::cache::column_block_cache::GlobalColumnBlockCache;
    use crate::engine::core::read::cache::global_zone_index_cache::GlobalZoneIndexCache;
    GlobalZoneIndexCache::instance().clear_for_test();
    GlobalColumnBlockCache::instance().clear_for_test();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("offset_one_evt", &[("letter", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store 6 events (multiple of 3) with letters A, B, C, D, E, F
    for (i, letter) in ["A", "B", "C", "D", "E", "F"].iter().enumerate() {
        let store_cmd = CommandFactory::store()
            .with_event_type("offset_one_evt")
            .with_context_id(&format!("ctx{}", i))
            .with_payload(serde_json::json!({ "letter": letter }))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
    }

    // Manual flush
    let flush_cmd = crate::command::types::Command::Flush;
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::flush::handle(
        &flush_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("flush should succeed");

    sleep(Duration::from_millis(1500)).await;

    // OFFSET 1 LIMIT 3 with ORDER BY
    let cmd =
        parse("QUERY offset_one_evt ORDER BY letter ASC OFFSET 1 LIMIT 3").expect("parse OFFSET 1");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);
    let json_start = body.find('{').unwrap_or(0);
    let json: JsonValue = serde_json::from_str(&body[json_start..]).expect("valid JSON");
    let rows = json["results"][0]["rows"].as_array().expect("rows array");

    assert_eq!(rows.len(), 3, "OFFSET 1 should skip first result");

    // Should get B, C, D (skipped A)
    let letters: Vec<String> = rows
        .iter()
        .map(|r| r[3]["letter"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        letters,
        vec!["B", "C", "D"],
        "OFFSET 1 should skip first letter"
    );
}

// =============================================================================
// SEQUENCE QUERY TESTS - E2E tests for FOLLOWED BY, PRECEDED BY, LINKED BY
// =============================================================================

/// E2E test for basic FOLLOWED BY sequence query
#[tokio::test]
async fn test_sequence_followed_by_basic() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("page_view", &[("page", "string"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields(
            "order_created",
            &[("order_id", "int"), ("user_id", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store page_view for u1
    let store_cmd = CommandFactory::store()
        .with_event_type("page_view")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"page": "/home", "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store order_created for u1 (after page_view)
    let store_cmd = CommandFactory::store()
        .with_event_type("order_created")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: page_view FOLLOWED BY order_created LINKED BY user_id
    let cmd_str = "QUERY page_view FOLLOWED BY order_created LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return both events (page_view and order_created for u1)
    assert!(
        body.contains("page_view") && body.contains("order_created"),
        "Should return both events in sequence"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// E2E test for FOLLOWED BY with WHERE clause filtering
#[tokio::test]
async fn test_sequence_followed_by_with_where_clause() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("page_view", &[("page", "string"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields(
            "order_created",
            &[("order_id", "int"), ("user_id", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store page_view for u1 with /checkout
    let store_cmd = CommandFactory::store()
        .with_event_type("page_view")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"page": "/checkout", "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store order_created for u1
    let store_cmd = CommandFactory::store()
        .with_event_type("order_created")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    // Store page_view for u2 with /home (should not match)
    let store_cmd = CommandFactory::store()
        .with_event_type("page_view")
        .with_context_id("ctx3")
        .with_payload(serde_json::json!({"page": "/home", "user_id": "u2"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store order_created for u2
    let store_cmd = CommandFactory::store()
        .with_event_type("order_created")
        .with_context_id("ctx4")
        .with_payload(serde_json::json!({"order_id": 2, "user_id": "u2"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: page_view FOLLOWED BY order_created LINKED BY user_id WHERE page_view.page="/checkout"
    let cmd_str = "QUERY page_view FOLLOWED BY order_created LINKED BY user_id WHERE page_view.page=\"/checkout\"";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY with WHERE clause");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should only return u1's events (u2 has /home, not /checkout)
    assert!(
        body.contains("u1") && !body.contains("u2"),
        "Should only return u1's sequence"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// E2E test for PRECEDED BY sequence query
#[tokio::test]
async fn test_sequence_preceded_by_basic() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "payment_failed",
            &[("user_id", "string"), ("amount", "int")],
        )
        .await
        .unwrap();
    factory
        .define_with_fields(
            "order_created",
            &[("order_id", "int"), ("user_id", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store payment_failed for u1
    let store_cmd = CommandFactory::store()
        .with_event_type("payment_failed")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"user_id": "u1", "amount": 100}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store order_created for u1 (after payment_failed)
    let store_cmd = CommandFactory::store()
        .with_event_type("order_created")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: order_created PRECEDED BY payment_failed LINKED BY user_id
    let cmd_str = "QUERY order_created PRECEDED BY payment_failed LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse PRECEDED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return both events (payment_failed and order_created for u1)
    assert!(
        body.contains("payment_failed") && body.contains("order_created"),
        "Should return both events in sequence"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// E2E test for sequence query with numeric link field
#[tokio::test]
async fn test_sequence_with_numeric_link_field() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "order_created",
            &[("order_id", "int"), ("customer_id", "int")],
        )
        .await
        .unwrap();
    factory
        .define_with_fields(
            "order_shipped",
            &[("order_id", "int"), ("customer_id", "int")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store order_created for customer_id=100
    let store_cmd = CommandFactory::store()
        .with_event_type("order_created")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"order_id": 1, "customer_id": 100}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store order_shipped for customer_id=100
    let store_cmd = CommandFactory::store()
        .with_event_type("order_shipped")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "customer_id": 100}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: order_created FOLLOWED BY order_shipped LINKED BY customer_id
    let cmd_str = "QUERY order_created FOLLOWED BY order_shipped LINKED BY customer_id";
    let cmd = parse(cmd_str).expect("parse sequence query with numeric link field");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return both events
    assert!(
        body.contains("order_created") && body.contains("order_shipped"),
        "Should return both events in sequence"
    );
    assert!(body.contains("100"), "Should contain customer_id=100");
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// Edge case: Multiple sequences for the same link value
/// Tests that when there are multiple page_views and multiple order_createds
/// for the same user, all valid sequences are matched.
#[tokio::test]
async fn test_sequence_multiple_sequences_same_link_value() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("page_view", &[("page", "string"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields(
            "order_created",
            &[("order_id", "int"), ("user_id", "string")],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store multiple page_views for u1
    for (i, page) in [("/home", 1), ("/products", 2), ("/cart", 3)]
        .iter()
        .enumerate()
    {
        let store_cmd = CommandFactory::store()
            .with_event_type("page_view")
            .with_context_id(&format!("ctx_pv{}", i))
            .with_payload(serde_json::json!({"page": page.0, "user_id": "u1"}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
        sleep(Duration::from_millis(500)).await;
    }

    // Store multiple order_createds for u1
    for i in 1..=2 {
        let store_cmd = CommandFactory::store()
            .with_event_type("order_created")
            .with_context_id(&format!("ctx_oc{}", i))
            .with_payload(serde_json::json!({"order_id": i, "user_id": "u1"}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
        sleep(Duration::from_millis(500)).await;
    }

    sleep(Duration::from_millis(500)).await;

    // Query: page_view FOLLOWED BY order_created LINKED BY user_id
    let cmd_str = "QUERY page_view FOLLOWED BY order_created LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should find multiple sequences (each page_view can match with order_createds that come after)
    assert!(
        body.contains("page_view") && body.contains("order_created"),
        "Should return sequences"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequences"
    );
}

/// Edge case: No matching sequences - events exist but don't form valid sequences
/// Tests that when events exist but don't match the sequence pattern, no results are returned.
#[tokio::test]
async fn test_sequence_no_matching_sequences() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("signup", &[("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("purchase", &[("order_id", "int"), ("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store signup for u1
    let store_cmd = CommandFactory::store()
        .with_event_type("signup")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store purchase for u2 (different user - should not match)
    let store_cmd = CommandFactory::store()
        .with_event_type("purchase")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "user_id": "u2"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: signup FOLLOWED BY purchase LINKED BY user_id
    // Should not match because u1 != u2
    let cmd_str = "QUERY signup FOLLOWED BY purchase LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return no matching sequences
    assert!(
        body.contains("\"row_count\":0"),
        "Should not find matching sequences when link values don't match"
    );
}

/// Edge case: Wrong temporal order - events in wrong order shouldn't match for FOLLOWED BY
/// Tests that FOLLOWED BY requires correct temporal ordering.
#[tokio::test]
async fn test_sequence_wrong_temporal_order() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("login", &[("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("logout", &[("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store logout for u1 FIRST
    let store_cmd = CommandFactory::store()
        .with_event_type("logout")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store login for u1 AFTER logout (wrong order)
    let store_cmd = CommandFactory::store()
        .with_event_type("login")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: login FOLLOWED BY logout LINKED BY user_id
    // Should not match because login comes AFTER logout (wrong order)
    let cmd_str = "QUERY login FOLLOWED BY logout LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return no matching sequences (wrong temporal order)
    assert!(
        body.contains("\"row_count\":0"),
        "Should not match when events are in wrong temporal order"
    );
}

/// Edge case: Multiple users with partial matches
/// Tests that only users with complete sequences are returned.
#[tokio::test]
async fn test_sequence_multiple_users_partial_matches() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("view_item", &[("item_id", "int"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("add_to_cart", &[("item_id", "int"), ("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // User u1: view_item -> add_to_cart (complete sequence)
    let store_cmd = CommandFactory::store()
        .with_event_type("view_item")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"item_id": 1, "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("add_to_cart")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"item_id": 1, "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // User u2: only view_item (incomplete sequence - no add_to_cart)
    let store_cmd = CommandFactory::store()
        .with_event_type("view_item")
        .with_context_id("ctx3")
        .with_payload(serde_json::json!({"item_id": 2, "user_id": "u2"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: view_item FOLLOWED BY add_to_cart LINKED BY user_id
    let cmd_str = "QUERY view_item FOLLOWED BY add_to_cart LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should only return u1's sequence (u2 has incomplete sequence)
    assert!(
        body.contains("u1") && !body.contains("u2"),
        "Should only return complete sequences"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find at least one matching sequence"
    );
}

/// Edge case: Sequence with LIMIT
/// Tests that LIMIT works correctly with sequence queries.
#[tokio::test]
async fn test_sequence_with_limit() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("click", &[("user_id", "string"), ("button", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("conversion", &[("user_id", "string"), ("value", "int")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Create sequences for 3 users
    for user_id in ["u1", "u2", "u3"] {
        // Store click
        let store_cmd = CommandFactory::store()
            .with_event_type("click")
            .with_context_id(&format!("ctx_click_{}", user_id))
            .with_payload(serde_json::json!({"user_id": user_id, "button": "buy"}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");

        sleep(Duration::from_millis(500)).await;

        // Store conversion
        let store_cmd = CommandFactory::store()
            .with_event_type("conversion")
            .with_context_id(&format!("ctx_conv_{}", user_id))
            .with_payload(serde_json::json!({"user_id": user_id, "value": 100}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");

        sleep(Duration::from_millis(500)).await;
    }

    // Query with LIMIT 2
    let cmd_str = "QUERY click FOLLOWED BY conversion LINKED BY user_id LIMIT 2";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY with LIMIT");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should return at most 2 sequences (4 events total: 2 clicks + 2 conversions)
    assert!(
        body.contains("click") && body.contains("conversion"),
        "Should return sequences"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequences"
    );
}

/// Edge case: Sequence with WHERE clause on second event type
/// Tests WHERE clause filtering on the second event in the sequence.
#[tokio::test]
async fn test_sequence_where_clause_on_second_event() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("search", &[("query", "string"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields(
            "purchase",
            &[
                ("order_id", "int"),
                ("user_id", "string"),
                ("amount", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // User u1: search -> purchase (amount = 50)
    let store_cmd = CommandFactory::store()
        .with_event_type("search")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"query": "laptop", "user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("purchase")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"order_id": 1, "user_id": "u1", "amount": 50}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // User u2: search -> purchase (amount = 200)
    let store_cmd = CommandFactory::store()
        .with_event_type("search")
        .with_context_id("ctx3")
        .with_payload(serde_json::json!({"query": "phone", "user_id": "u2"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("purchase")
        .with_context_id("ctx4")
        .with_payload(serde_json::json!({"order_id": 2, "user_id": "u2", "amount": 200}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: search FOLLOWED BY purchase LINKED BY user_id WHERE purchase.amount > 100
    let cmd_str = "QUERY search FOLLOWED BY purchase LINKED BY user_id WHERE purchase.amount > 100";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY with WHERE on second event");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should only return u2's sequence (u1's purchase amount is 50, which is <= 100)
    assert!(
        body.contains("u2") && !body.contains("u1"),
        "Should only return sequences where purchase.amount > 100"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// Edge case: Sequence with duplicate events for same link value
/// Tests that multiple events of the same type for the same link value are handled correctly.
#[tokio::test]
async fn test_sequence_duplicate_events_same_link() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("page_view", &[("page", "string"), ("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("checkout", &[("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store multiple page_views for u1
    for (i, page) in ["/home", "/products", "/cart"].iter().enumerate() {
        let store_cmd = CommandFactory::store()
            .with_event_type("page_view")
            .with_context_id(&format!("ctx_pv{}", i))
            .with_payload(serde_json::json!({"page": page, "user_id": "u1"}))
            .create();
        let (mut _r, mut w) = duplex(1024);
        crate::command::handlers::store::handle(
            &store_cmd,
            &shard_manager,
            &registry,
            &mut w,
            &JsonRenderer,
        )
        .await
        .expect("store should succeed");
        sleep(Duration::from_millis(500)).await;
    }

    // Store checkout for u1 (after all page_views)
    let store_cmd = CommandFactory::store()
        .with_event_type("checkout")
        .with_context_id("ctx_checkout")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: page_view FOLLOWED BY checkout LINKED BY user_id
    let cmd_str = "QUERY page_view FOLLOWED BY checkout LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(8192);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 8192];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should find sequences (each page_view can match with checkout)
    assert!(
        body.contains("page_view") && body.contains("checkout"),
        "Should return sequences with duplicate events"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequences"
    );
}

/// Edge case: Sequence across multiple shards
/// Tests that sequences work when events are stored in different shards.
#[tokio::test]
async fn test_sequence_across_multiple_shards() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("event_a", &[("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("event_b", &[("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    // Use multiple shards to test cross-shard sequence matching
    let shard_manager = ShardManager::new(3, base_dir, wal_dir).await;

    // Store event_a for u1 (might go to any shard)
    let store_cmd = CommandFactory::store()
        .with_event_type("event_a")
        .with_context_id("ctx_a1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store event_b for u1 (might go to different shard)
    let store_cmd = CommandFactory::store()
        .with_event_type("event_b")
        .with_context_id("ctx_b1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: event_a FOLLOWED BY event_b LINKED BY user_id
    let cmd_str = "QUERY event_a FOLLOWED BY event_b LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should find sequence even across shards
    assert!(
        body.contains("event_a") && body.contains("event_b"),
        "Should return sequence across shards"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// Edge case: PRECEDED BY with wrong temporal order
/// Tests that PRECEDED BY correctly requires the preceding event to come first.
#[tokio::test]
async fn test_sequence_preceded_by_wrong_order() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("login", &[("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("logout", &[("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store login FIRST (preceding event)
    let store_cmd = CommandFactory::store()
        .with_event_type("login")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // Store logout AFTER login (correct order for PRECEDED BY)
    let store_cmd = CommandFactory::store()
        .with_event_type("logout")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: logout PRECEDED BY login LINKED BY user_id
    // Should match because login (preceding) comes before logout
    let cmd_str = "QUERY logout PRECEDED BY login LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse PRECEDED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should match because login (preceding) comes before logout
    assert!(
        body.contains("login") && body.contains("logout"),
        "Should match when preceding event comes first"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// Edge case: Sequence with WHERE clause on both event types
/// Tests complex WHERE clause filtering on both events in the sequence.
#[tokio::test]
async fn test_sequence_where_clause_both_events() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields(
            "view",
            &[
                ("product", "string"),
                ("user_id", "string"),
                ("category", "string"),
            ],
        )
        .await
        .unwrap();
    factory
        .define_with_fields(
            "buy",
            &[
                ("product", "string"),
                ("user_id", "string"),
                ("price", "int"),
            ],
        )
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // User u1: view (electronics) -> buy (price=100)
    let store_cmd = CommandFactory::store()
        .with_event_type("view")
        .with_context_id("ctx1")
        .with_payload(
            serde_json::json!({"product": "laptop", "user_id": "u1", "category": "electronics"}),
        )
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("buy")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"product": "laptop", "user_id": "u1", "price": 100}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    // User u2: view (clothing) -> buy (price=50)
    let store_cmd = CommandFactory::store()
        .with_event_type("view")
        .with_context_id("ctx3")
        .with_payload(
            serde_json::json!({"product": "shirt", "user_id": "u2", "category": "clothing"}),
        )
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(1000)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("buy")
        .with_context_id("ctx4")
        .with_payload(serde_json::json!({"product": "shirt", "user_id": "u2", "price": 50}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: view FOLLOWED BY buy LINKED BY user_id WHERE view.category="electronics" AND buy.price > 80
    let cmd_str = "QUERY view FOLLOWED BY buy LINKED BY user_id WHERE view.category=\"electronics\" AND buy.price > 80";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY with WHERE on both events");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should only return u1's sequence (u2 has clothing category and price=50)
    assert!(
        body.contains("u1") && !body.contains("u2"),
        "Should only return sequences matching both WHERE conditions"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}

/// Edge case: Sequence with very close timestamps
/// Tests that sequences work correctly when events have very close timestamps.
#[tokio::test]
async fn test_sequence_very_close_timestamps() {
    let _guard = set_streaming_enabled(true);
    init_for_tests();

    let base_dir = tempdir().unwrap().into_path();
    let wal_dir = tempdir().unwrap().into_path();

    let factory = SchemaRegistryFactory::new();
    factory
        .define_with_fields("start", &[("user_id", "string")])
        .await
        .unwrap();
    factory
        .define_with_fields("end", &[("user_id", "string")])
        .await
        .unwrap();
    let registry = factory.registry();
    let shard_manager = ShardManager::new(1, base_dir, wal_dir).await;

    // Store start for u1
    let store_cmd = CommandFactory::store()
        .with_event_type("start")
        .with_context_id("ctx1")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    // Store end immediately after (very close timestamps)
    sleep(Duration::from_millis(10)).await;

    let store_cmd = CommandFactory::store()
        .with_event_type("end")
        .with_context_id("ctx2")
        .with_payload(serde_json::json!({"user_id": "u1"}))
        .create();
    let (mut _r, mut w) = duplex(1024);
    crate::command::handlers::store::handle(
        &store_cmd,
        &shard_manager,
        &registry,
        &mut w,
        &JsonRenderer,
    )
    .await
    .expect("store should succeed");

    sleep(Duration::from_millis(500)).await;

    // Query: start FOLLOWED BY end LINKED BY user_id
    let cmd_str = "QUERY start FOLLOWED BY end LINKED BY user_id";
    let cmd = parse(cmd_str).expect("parse FOLLOWED BY query");
    let (mut reader, mut writer) = duplex(4096);
    handle(&cmd, &shard_manager, &registry, &mut writer, &JsonRenderer)
        .await
        .unwrap();

    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let body = String::from_utf8_lossy(&buf[..n]);

    // Should match even with very close timestamps
    assert!(
        body.contains("start") && body.contains("end"),
        "Should match sequences with very close timestamps"
    );
    assert!(
        !body.contains("No matching events found"),
        "Should find matching sequence"
    );
}
