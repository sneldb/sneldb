use super::strategies::{AggregationProjection, ProjectionStrategy, SelectionProjection};
use crate::command::types::{AggSpec, CompareOp, Expr, TimeGranularity};
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

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
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
async fn aggregation_multiple_ops_include_all_arg_fields() {
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    schema
        .define_with_fields("evt", &[("x", "int"), ("y", "int"), ("z", "int")])
        .await
        .unwrap();

    let cmd = crate::command::types::Command::Query {
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

    let cmd = crate::command::types::Command::Query {
        event_type: "evt".into(),
        context_id: None,
        since: None,
        where_clause: Some(Expr::Compare {
            field: "timestamp".into(),
            op: CompareOp::Gt,
            value: serde_json::json!(0),
        }),
        limit: None,
        return_fields: None,
        link_field: None,
        aggs: Some(vec![AggSpec::Count { unique_field: None }]),
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

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
