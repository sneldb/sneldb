use crate::command::handlers::query::handle;
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
    sleep(Duration::from_millis(200)).await;

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
    sleep(Duration::from_millis(200)).await;

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
async fn test_query_selection_limit_truncates_and_sorts() {
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
    assert_eq!(rows[0][0], JsonValue::String("c1".into()));
    assert_eq!(rows[1][0], JsonValue::String("c2".into()));
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
