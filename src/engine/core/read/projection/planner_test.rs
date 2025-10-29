use crate::command::types::{CompareOp, Expr, TimeGranularity};
use crate::engine::core::read::projection::ProjectionPlanner;
use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn projection_default_includes_all_payload_and_core() {
    crate::logging::init_for_tests();

    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields(
            "order",
            &[("country", "string"), ("plan", "string"), ("amount", "int")],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query().with_event_type("order").create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"context_id".to_string()));
    assert!(cols.contains(&"event_type".to_string()));
    assert!(cols.contains(&"timestamp".to_string()));
    assert!(cols.contains(&"event_id".to_string()));
    // all payload fields
    assert!(cols.contains(&"country".to_string()));
    assert!(cols.contains(&"plan".to_string()));
    assert!(cols.contains(&"amount".to_string()));
}

#[tokio::test]
async fn projection_aggregate_minimal_for_count_only() {
    crate::logging::init_for_tests();

    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("x", "int"), ("y", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // COUNT-only should force timestamp so we can count events per zone without payload
    assert!(cols.contains(&"timestamp".to_string()));
    assert!(cols.contains(&"event_id".to_string()));
    // But should not include unrelated payload columns by default
    assert!(!cols.contains(&"x".to_string()));
    assert!(!cols.contains(&"y".to_string()));
}

#[tokio::test]
async fn projection_aggregate_includes_bucket_groupby_filters_and_args() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields(
            "order",
            &[
                ("country", "string"),
                ("status", "string"),
                ("amount", "int"),
            ],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("order")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("ok"),
        })
        .add_avg("amount")
        .add_min("amount")
        .add_max("amount")
        .add_total("amount")
        .with_time_bucket(TimeGranularity::Month)
        .with_group_by(vec!["country"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // filter column
    assert!(cols.contains(&"status".to_string()));
    // time bucket requires timestamp
    assert!(cols.contains(&"timestamp".to_string()));
    assert!(cols.contains(&"event_id".to_string()));
    // group by field
    assert!(cols.contains(&"country".to_string()));
    // agg argument field
    assert!(cols.contains(&"amount".to_string()));
}

#[tokio::test]
async fn projection_aggregate_count_unique_includes_unique_field() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("login", &[("user_id", "string"), ("device", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("login")
        .add_count_unique("user_id")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // COUNT UNIQUE must include the unique field
    assert!(cols.contains(&"user_id".to_string()));
    // Should not include unrelated payload by default
    assert!(!cols.contains(&"device".to_string()));
}

#[tokio::test]
async fn projection_empty_list_behaves_like_all() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("subscription", &[("country", "string"), ("plan", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("subscription")
        .with_return_fields(vec![])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"country".to_string()));
    assert!(cols.contains(&"plan".to_string()));
    assert!(cols.contains(&"event_id".to_string()));
}

#[tokio::test]
async fn projection_specific_fields_only_plus_core_and_filters() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("login", &[("device", "string"), ("ip", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("login")
        .with_where_clause(Expr::Compare {
            field: "device".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("ios"),
        })
        .with_return_fields(vec!["ip"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // core
    assert!(cols.contains(&"context_id".to_string()));
    assert!(cols.contains(&"event_type".to_string()));
    assert!(cols.contains(&"timestamp".to_string()));
    // filter column
    assert!(cols.contains(&"device".to_string()));
    // projected field
    assert!(cols.contains(&"ip".to_string()));
    // excluded field
    assert!(!cols.contains(&"not_exists".to_string()));
}

#[tokio::test]
async fn projection_payload_keyword_is_ignored_and_not_included() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("payment", &[("amount", "int"), ("currency", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("payment")
        .with_return_fields(vec!["payload"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // Since "payload" is not a valid column, it should not trigger inclusion of all payload
    assert!(!cols.contains(&"amount".to_string()));
    assert!(!cols.contains(&"currency".to_string()));
}

#[tokio::test]
async fn projection_unknown_field_is_excluded() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("foo", &[("a", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("foo")
        .with_return_fields(vec!["bar"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(!cols.contains(&"bar".to_string()));
}

#[tokio::test]
async fn projection_excludes_unreferenced_payload_fields() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields(
            "product",
            &[("name", "string"), ("price", "int"), ("color", "string")],
        )
        .await
        .unwrap();

    // RETURN only name, filter on price; color should not be included
    let cmd = CommandFactory::query()
        .with_event_type("product")
        .with_where_clause(Expr::Compare {
            field: "price".into(),
            op: CompareOp::Gt,
            value: serde_json::json!(10),
        })
        .with_return_fields(vec!["name"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // core
    assert!(cols.contains(&"context_id".to_string()));
    assert!(cols.contains(&"event_type".to_string()));
    assert!(cols.contains(&"timestamp".to_string()));
    // filter col
    assert!(cols.contains(&"price".to_string()));
    // projected
    assert!(cols.contains(&"name".to_string()));
    // not in filters nor projection -> excluded
    assert!(!cols.contains(&"color".to_string()));
}

#[tokio::test]
async fn projection_aggregate_count_only_with_payload_filter_includes_filter_column() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("kind", "string"), ("v", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .with_where_clause(Expr::Compare {
            field: "kind".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("a"),
        })
        .add_count()
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"kind".to_string()));
    assert!(!cols.contains(&"context_id".to_string()));
    assert!(!cols.contains(&"event_type".to_string()));
}

#[tokio::test]
async fn projection_aggregate_count_only_with_time_bucket_and_group_by() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("country", "string"), ("v", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .with_time_bucket(TimeGranularity::Month)
        .with_group_by(vec!["country"])
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"timestamp".to_string()));
    assert!(cols.contains(&"country".to_string()));
}

#[tokio::test]
async fn projection_aggregate_ignores_return_fields_when_aggs_present() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("a", "int"), ("b", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .with_return_fields(vec!["a"])
        .add_total("b")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // Should include only fields required by aggregation path (here: b)
    assert!(cols.contains(&"b".to_string()));
    assert!(!cols.contains(&"a".to_string()));
}

#[tokio::test]
async fn projection_aggregate_multiple_ops_include_all_arg_fields() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("x", "int"), ("y", "int"), ("z", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_avg("x")
        .add_min("y")
        .add_max("z")
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"x".to_string()));
    assert!(cols.contains(&"y".to_string()));
    assert!(cols.contains(&"z".to_string()));
}
