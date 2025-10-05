use super::context::ProjectionContext;
use crate::command::types::CompareOp;
use crate::engine::core::read::query_plan::QueryPlan;
use crate::test_helpers::factories::{
    CommandFactory, FilterPlanFactory, QueryPlanFactory, SchemaRegistryFactory,
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
    assert_eq!(set.len(), 3);
    assert!(set.contains("context_id"));
    assert!(set.contains("event_type"));
    assert!(set.contains("timestamp"));

    assert!(ProjectionContext::is_core_field("context_id"));
    assert!(ProjectionContext::is_core_field("event_type"));
    assert!(ProjectionContext::is_core_field("timestamp"));
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
    plan.filter_plans = vec![
        FilterPlanFactory::new()
            .with_column("a")
            .with_operation(CompareOp::Eq)
            .create(),
        FilterPlanFactory::new().with_column("b").create(), // operation None -> excluded
        FilterPlanFactory::new()
            .with_column("timestamp")
            .with_operation(CompareOp::Gt)
            .create(),
        FilterPlanFactory::new()
            .with_column("a")
            .with_operation(CompareOp::Eq)
            .create(), // duplicate, should dedupe
    ];

    let ctx = ProjectionContext { plan: &plan };
    let set = ctx.filter_columns();
    assert_eq!(set.len(), 2);
    assert!(set.contains("a"));
    assert!(set.contains("timestamp"));
    assert!(!set.contains("b"));
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
    assert_eq!(fields.len(), 2);
    assert!(fields.contains("country"));
    assert!(fields.contains("amount"));
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
