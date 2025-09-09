use crate::command::types::{CompareOp, Expr};
use crate::engine::core::{FilterPlan, QueryPlan};
use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn builds_query_plan_and_generates_expected_filter_plans() {
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    // Build the command: Query with AND clause: id > 1 AND plan = "basic"
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Gt,
            value: json!(1),
        }),
        Box::new(Expr::Compare {
            field: "plan".to_string(),
            op: CompareOp::Eq,
            value: json!("basic"),
        }),
    );

    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx1")
        .with_where_clause(expr)
        .create();

    // Use factory to build QueryPlan
    let plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(registry.clone())
        .create()
        .await;

    // Assert event_type and context_id
    assert_eq!(plan.event_type(), "test_event");
    assert_eq!(plan.context_id(), Some("ctx1"));

    // Assert filter plan contains expected filters
    let mut column_map = plan
        .filter_plans
        .iter()
        .map(|fp| (fp.column.clone(), fp.clone()))
        .collect::<std::collections::HashMap<_, _>>();

    let id_filter = column_map.remove("id").expect("missing 'id' filter");
    assert_eq!(id_filter.operation, Some(CompareOp::Gt));
    assert_eq!(id_filter.value, Some(json!(1)));

    let plan_filter = column_map.remove("plan").expect("missing 'plan' filter");
    assert_eq!(plan_filter.operation, Some(CompareOp::Eq));
    assert_eq!(plan_filter.value, Some(json!("basic")));

    let context_id_filter = column_map
        .remove("context_id")
        .expect("missing 'context_id' filter");
    assert_eq!(context_id_filter.value, Some(json!("ctx1")));

    let event_type_filter = column_map
        .remove("event_type")
        .expect("missing 'event_type' filter");
    assert_eq!(event_type_filter.value, Some(json!("test_event")));

    let timestamp_filter = column_map
        .remove("timestamp")
        .expect("missing 'timestamp' filter");
    assert_eq!(timestamp_filter.operation, Some(CompareOp::Gte));
    assert_eq!(timestamp_filter.value, None);
}

#[tokio::test]
async fn builds_query_plan_with_since_timestamp() {
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();

    let command = CommandFactory::query()
        .with_context_id("ctx1")
        .with_event_type("test_event")
        .with_since("1234567890")
        .with_where_clause(Expr::And(
            Box::new(Expr::Compare {
                field: "id".into(),
                op: CompareOp::Gt,
                value: json!(42),
            }),
            Box::new(Expr::Compare {
                field: "plan".into(),
                op: CompareOp::Eq,
                value: json!("premium"),
            }),
        ))
        .create();

    let plan = QueryPlan::build(&command, Arc::clone(&registry)).await;

    let mut column_map: HashMap<String, FilterPlan> = plan
        .filter_plans
        .into_iter()
        .map(|fp| (fp.column.clone(), fp))
        .collect();

    let id_filter = column_map.remove("id").expect("missing 'id' filter");
    assert_eq!(id_filter.operation, Some(CompareOp::Gt));
    assert_eq!(id_filter.value, Some(json!(42)));

    let plan_filter = column_map.remove("plan").expect("missing 'plan' filter");
    assert_eq!(plan_filter.operation, Some(CompareOp::Eq));
    assert_eq!(plan_filter.value, Some(json!("premium")));

    let context_id_filter = column_map
        .remove("context_id")
        .expect("missing 'context_id' filter");
    assert_eq!(context_id_filter.value, Some(json!("ctx1")));

    let event_type_filter = column_map
        .remove("event_type")
        .expect("missing 'event_type' filter");
    assert_eq!(event_type_filter.value, Some(json!("test_event")));

    let timestamp_filter = column_map
        .remove("timestamp")
        .expect("missing 'timestamp' filter");
    assert_eq!(timestamp_filter.operation, Some(CompareOp::Gte));
    assert_eq!(timestamp_filter.value, Some(json!("1234567890")));

    assert!(
        column_map.is_empty(),
        "unexpected extra filters: {:?}",
        column_map
    );
}
