use crate::command::types::{Command, CompareOp, Expr};
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
    // all payload fields
    assert!(cols.contains(&"country".to_string()));
    assert!(cols.contains(&"plan".to_string()));
    assert!(cols.contains(&"amount".to_string()));
}

#[tokio::test]
async fn projection_empty_list_behaves_like_all() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("subscription", &[("country", "string"), ("plan", "string")])
        .await
        .unwrap();

    let cmd = Command::Query {
        event_type: "subscription".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: Some(vec![]),
    };

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    assert!(cols.contains(&"country".to_string()));
    assert!(cols.contains(&"plan".to_string()));
}

#[tokio::test]
async fn projection_specific_fields_only_plus_core_and_filters() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("login", &[("device", "string"), ("ip", "string")])
        .await
        .unwrap();

    let cmd = Command::Query {
        event_type: "login".into(),
        context_id: None,
        since: None,
        where_clause: Some(Expr::Compare {
            field: "device".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("ios"),
        }),
        limit: None,
        return_fields: Some(vec!["ip".into()]),
    };

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

    let cmd = Command::Query {
        event_type: "payment".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: Some(vec!["payload".into()]),
    };

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

    let cmd = Command::Query {
        event_type: "foo".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: Some(vec!["bar".into()]),
    };

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
    let cmd = Command::Query {
        event_type: "product".into(),
        context_id: None,
        since: None,
        where_clause: Some(Expr::Compare {
            field: "price".into(),
            op: CompareOp::Gt,
            value: serde_json::json!(10),
        }),
        limit: None,
        return_fields: Some(vec!["name".into()]),
    };

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
