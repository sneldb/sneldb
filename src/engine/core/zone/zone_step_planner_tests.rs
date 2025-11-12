use crate::command::types::{CompareOp, Expr};
use crate::engine::core::ExecutionStep;
use crate::engine::core::zone::zone_step_planner::ZoneStepPlanner;
use crate::test_helpers::factories::{
    CommandFactory, FilterGroupFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
// AND + context present: planner puts context step first
async fn planner_places_context_first_on_and() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Schema and plan
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "login";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx-1")
        .with_where_clause(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: json!(1),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    // Build two steps: id and context_id
    let uid = plan.event_type_uid().await.expect("uid");
    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(json!("ctx-1"))
        .with_uid(&uid)
        .create();
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let step_ctx = ExecutionStep::new(fp_ctx, &plan);
    let step_id = ExecutionStep::new(fp_id, &plan);
    let steps = vec![step_id, step_ctx]; // Intentionally out of desired order

    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);

    // Expect the first planned index to be the context step (original index 1)
    assert_eq!(order.first().unwrap().0, 1);
}

#[tokio::test]
// No context filter: planner preserves original order
async fn planner_keeps_order_when_no_context() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "orders";
    schema
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: json!(1),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into(), "00002".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();
    let fp_other = FilterGroupFactory::new()
        .with_column("timestamp")
        .with_operation(CompareOp::Gt)
        .with_value(json!(0))
        .with_uid(&uid)
        .create();

    let steps = vec![
        ExecutionStep::new(fp_id, &plan),
        ExecutionStep::new(fp_other, &plan),
    ];
    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);

    assert_eq!(
        order.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[tokio::test]
// OR logic: planner keeps order even if context exists
async fn planner_keeps_order_on_or_even_with_context() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "login";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    // where: id = 1 OR id = 2 (LogicalOp::Or)
    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx-1")
        .with_where_clause(Expr::Or(
            Box::new(Expr::Compare {
                field: "id".into(),
                op: CompareOp::Eq,
                value: serde_json::json!(1),
            }),
            Box::new(Expr::Compare {
                field: "id".into(),
                op: CompareOp::Eq,
                value: serde_json::json!(2),
            }),
        ))
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");
    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!("ctx-1"))
        .with_uid(&uid)
        .create();
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!(1))
        .with_uid(&uid)
        .create();

    // Context first in given order; planner should keep order (no AND)
    let steps = vec![
        ExecutionStep::new(fp_ctx, &plan),
        ExecutionStep::new(fp_id, &plan),
    ];
    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);
    assert_eq!(
        order.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        vec![0, 1]
    );
    // subsets should all be None
    assert!(order.iter().all(|(_, subset)| subset.is_none()));
}

#[tokio::test]
// NOT logic: planner preserves order (no special reordering)
async fn planner_keeps_order_on_not() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "login";
    schema
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    // where: NOT(id = 1)
    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(Expr::Not(Box::new(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: serde_json::json!(1),
        })))
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!(1))
        .with_uid(&uid)
        .create();

    let steps = vec![ExecutionStep::new(fp_id, &plan)];
    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);
    assert_eq!(order.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![0]);
}

#[tokio::test]
// No steps: planner returns empty plan
async fn planner_returns_empty_for_no_steps() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "login";
    schema
        .define_with_fields(event_type, &[("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query().with_event_type(event_type).create();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let steps: Vec<ExecutionStep> = Vec::new();
    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);
    assert!(order.is_empty());
}

#[tokio::test]
// Context already first: planner keeps order
async fn planner_preserves_when_context_already_first() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "login";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .with_where_clause(Expr::Compare {
            field: "id".into(),
            op: CompareOp::Eq,
            value: serde_json::json!(1),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");
    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!("ctx1"))
        .with_uid(&uid)
        .create();
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(CompareOp::Eq)
        .with_value(serde_json::json!(1))
        .with_uid(&uid)
        .create();

    let steps = vec![
        ExecutionStep::new(fp_ctx, &plan),
        ExecutionStep::new(fp_id, &plan),
    ];
    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);
    assert_eq!(
        order.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        vec![0, 1]
    );
}
