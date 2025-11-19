use super::context::ProjectionContext;
use crate::command::types::CompareOp;
use crate::engine::core::read::query_plan::QueryPlan;
use crate::test_helpers::factories::{
    CommandFactory, FilterGroupFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn core_fields_and_is_core_field() {
    // Build a minimal QueryPlan via factory (registry/event type aren't used in these checks)
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let cmd = CommandFactory::query().with_event_type("evt").create();
    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let ctx = ProjectionContext { plan: &plan };

    let set = ctx.core_fields();
    assert_eq!(
        set,
        vec![
            "context_id".to_string(),
            "event_type".to_string(),
            "timestamp".to_string(),
            "event_id".to_string(),
        ]
    );

    assert!(ProjectionContext::is_core_field("context_id"));
    assert!(ProjectionContext::is_core_field("event_type"));
    assert!(ProjectionContext::is_core_field("timestamp"));
    assert!(ProjectionContext::is_core_field("event_id"));
    assert!(!ProjectionContext::is_core_field("not_core"));
}

#[tokio::test]
async fn filter_columns_includes_only_active_filters_and_dedupes() {
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    let cmd = CommandFactory::query().with_event_type("evt").create();
    let mut plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;
    plan.filter_groups = vec![
        FilterGroupFactory::new()
            .with_column("a")
            .with_operation(CompareOp::Eq)
            .create(),
        FilterGroupFactory::new().with_column("b").create(), // operation None -> excluded
        FilterGroupFactory::new()
            .with_column("timestamp")
            .with_operation(CompareOp::Gt)
            .create(),
        FilterGroupFactory::new()
            .with_column("a")
            .with_operation(CompareOp::Eq)
            .create(), // duplicate, should dedupe
    ];

    let ctx = ProjectionContext { plan: &plan };
    let set = ctx.filter_columns();
    assert_eq!(set, vec!["a".to_string(), "timestamp".to_string()]);
}

#[tokio::test]
async fn payload_fields_returns_defined_schema_fields() {
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("order", &[("country", "string"), ("amount", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query().with_event_type("order").create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let ctx = ProjectionContext { plan: &plan };
    let fields = ctx.payload_fields().await;
    assert_eq!(fields, vec!["amount".to_string(), "country".to_string()]);
}

#[tokio::test]
async fn payload_fields_missing_schema_returns_empty() {
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    // Intentionally do not define schema for "unknown"

    let cmd = CommandFactory::query().with_event_type("unknown").create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let ctx = ProjectionContext { plan: &plan };
    let fields = ctx.payload_fields().await;
    assert!(fields.is_empty());
}

#[tokio::test]
async fn payload_fields_wildcard_includes_all_schemas() {
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("event1", &[("field1", "string"), ("field2", "int")])
        .await
        .unwrap();
    schema_factory
        .define_with_fields("event2", &[("field2", "int"), ("field3", "bool")])
        .await
        .unwrap();

    let cmd = CommandFactory::query().with_event_type("*").create();
    let plan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let ctx = ProjectionContext::new(&plan);
    let fields = ctx.payload_fields().await;
    // Should include all unique fields from all schemas: field1, field2, field3
    assert_eq!(fields.len(), 3);
    assert!(fields.contains(&"field1".to_string()));
    assert!(fields.contains(&"field2".to_string()));
    assert!(fields.contains(&"field3".to_string()));
}
