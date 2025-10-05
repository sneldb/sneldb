use crate::command::types::{AggSpec, Command, CompareOp, Expr, TimeGranularity};
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
async fn projection_aggregate_minimal_for_count_only() {
    crate::logging::init_for_tests();

    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("x", "int"), ("y", "int")])
        .await
        .unwrap();

    let cmd = Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Count { unique_field: None }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let cols = ProjectionPlanner::new(&plan).columns_to_load().await;
    // COUNT-only should force timestamp so we can count events per zone without payload
    assert!(cols.contains(&"timestamp".to_string()));
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

    let cmd = Command::Query {
        event_type: "order".into(),
        context_id: None,
        since: None,
        where_clause: Some(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("ok"),
        }),
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![
            AggSpec::Avg {
                field: "amount".into(),
            },
            AggSpec::Min {
                field: "amount".into(),
            },
            AggSpec::Max {
                field: "amount".into(),
            },
            AggSpec::Total {
                field: "amount".into(),
            },
        ]),
        time_bucket: Some(TimeGranularity::Month),
        group_by: Some(vec!["country".into()]),
        event_sequence: None,
    };

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

    let cmd = Command::Query {
        event_type: "login".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Count {
            unique_field: Some("user_id".into()),
        }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

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

    let cmd = Command::Query {
        event_type: "subscription".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: Some(vec![]),
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
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
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
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
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
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
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
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
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
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

#[tokio::test]
async fn projection_aggregate_count_only_with_payload_filter_includes_filter_column() {
    let factory = SchemaRegistryFactory::new();
    let registry = factory.registry();
    factory
        .define_with_fields("evt", &[("kind", "string"), ("v", "int")])
        .await
        .unwrap();

    let cmd = Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: Some(Expr::Compare {
            field: "kind".into(),
            op: CompareOp::Eq,
            value: serde_json::json!("a"),
        }),
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Count { unique_field: None }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

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

    let cmd = Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Count { unique_field: None }]),
        time_bucket: Some(TimeGranularity::Month),
        group_by: Some(vec!["country".into()]),
        event_sequence: None,
    };

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

    let cmd = Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: Some(vec!["a".into()]),
        link_field: None,
        aggs: Some(vec![AggSpec::Total { field: "b".into() }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

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

    let cmd = Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: None,
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![
            AggSpec::Avg { field: "x".into() },
            AggSpec::Min { field: "y".into() },
            AggSpec::Max { field: "z".into() },
        ]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

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
