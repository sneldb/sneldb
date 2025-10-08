use super::strategies::{AggregationProjection, ProjectionStrategy, SelectionProjection};
use crate::command::types::{CompareOp, Expr, TimeGranularity};
use crate::engine::core::read::query_plan::QueryPlan;
use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::tempdir;

fn to_set(v: Vec<String>) -> HashSet<String> {
    v.into_iter().collect()
}

#[tokio::test]
async fn selection_includes_core_filters_and_all_payload_by_default() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields(
            "order",
            &[("country", "string"), ("plan", "string"), ("amount", "int")],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query().with_event_type("order").create();
    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let s = SelectionProjection { plan: &plan };
    let out = to_set(s.compute().await.into_vec());

    // core
    assert!(out.contains("context_id"));
    assert!(out.contains("event_type"));
    assert!(out.contains("timestamp"));
    // all payload
    assert!(out.contains("country"));
    assert!(out.contains("plan"));
    assert!(out.contains("amount"));
}

#[tokio::test]
async fn selection_specific_fields_and_filters() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
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

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let s = SelectionProjection { plan: &plan };
    let out = to_set(s.compute().await.into_vec());

    // core
    assert!(out.contains("context_id"));
    assert!(out.contains("event_type"));
    assert!(out.contains("timestamp"));
    // filter col
    assert!(out.contains("device"));
    // projected
    assert!(out.contains("ip"));
    // not requested
    assert!(!out.contains("not_exists"));
}

#[tokio::test]
async fn selection_empty_return_fields_means_all_payload() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("subscription", &[("country", "string"), ("plan", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("subscription")
        .with_return_fields(vec![])
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let s = SelectionProjection { plan: &plan };
    let out = to_set(s.compute().await.into_vec());
    assert!(out.contains("country"));
    assert!(out.contains("plan"));
}

#[tokio::test]
async fn selection_unknown_fields_ignored() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("foo", &[("a", "string")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("foo")
        .with_return_fields(vec!["bar"])
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let s = SelectionProjection { plan: &plan };
    let out = to_set(s.compute().await.into_vec());
    assert!(!out.contains("bar"));
}

#[tokio::test]
async fn aggregation_count_only_minimal_timestamp() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("evt", &[("x", "int"), ("y", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    assert!(out.contains("timestamp"));
    assert!(!out.contains("x"));
    assert!(!out.contains("y"));
}

#[tokio::test]
async fn aggregation_count_only_with_payload_filter_includes_filter_column() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
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

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    assert!(out.contains("kind"));
}

#[tokio::test]
async fn aggregation_time_bucket_and_group_by_included() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("evt", &[("country", "string"), ("v", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_count()
        .with_time_bucket(TimeGranularity::Month)
        .with_group_by(vec!["country"])
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    assert!(out.contains("timestamp"));
    assert!(out.contains("country"));
}

#[tokio::test]
async fn aggregation_time_bucket_uses_selected_time_field() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields(
            "evt_time",
            &[("created_at", "datetime"), ("country", "string")],
        )
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt_time")
        .add_count()
        .with_time_bucket(TimeGranularity::Month)
        .with_time_field("created_at")
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    // Should include created_at (selected time field) for bucketing, not necessarily core timestamp
    assert!(out.contains("created_at"));
}

#[tokio::test]
async fn aggregation_multiple_ops_include_all_arg_fields() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("evt", &[("x", "int"), ("y", "int"), ("z", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .add_avg("x")
        .add_min("y")
        .add_max("z")
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    assert!(out.contains("x"));
    assert!(out.contains("y"));
    assert!(out.contains("z"));
}

#[tokio::test]
async fn aggregation_filter_on_core_is_ignored_but_fallback_timestamp_applies() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("evt", &[("x", "int")])
        .await
        .unwrap();

    let cmd = CommandFactory::query()
        .with_event_type("evt")
        .with_where_clause(Expr::Compare {
            field: "timestamp".into(),
            op: CompareOp::Gt,
            value: serde_json::json!(0),
        })
        .add_count()
        .create();

    let plan: QueryPlan = QueryPlanFactory::new()
        .with_command(cmd)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(tempdir().unwrap().path())
        .create()
        .await;

    let agg = plan.aggregate_plan.as_ref().unwrap();
    let s = AggregationProjection { plan: &plan, agg };
    let out = to_set(s.compute().await.into_vec());
    // timestamp filter is a core field and ignored during filter collection, but
    // fallback should still add timestamp since otherwise set would be empty
    assert!(out.contains("timestamp"));
}
