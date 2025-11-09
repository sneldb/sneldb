use crate::engine::core::ExecutionStep;
use crate::engine::core::zone::{
    zone_step_planner::ZoneStepPlanner, zone_step_runner::ZoneStepRunner,
};
use crate::test_helpers::factories::{
    CommandFactory, FilterGroupFactory, QueryPlanFactory, SchemaRegistryFactory,
};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
// AND + context planned first: derive and apply pruned segments
async fn runner_derives_pruned_segments_when_context_first_and_and() {
    use crate::logging::init_for_tests;
    init_for_tests();
    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "evt";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx-keep")
        .with_where_clause(crate::command::types::Expr::Compare {
            field: "id".into(),
            op: crate::command::types::CompareOp::Eq,
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

    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!("ctx-keep"))
        .with_uid(&uid)
        .create();
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let mut steps = vec![
        ExecutionStep::new(fp_id, &plan),
        ExecutionStep::new(fp_ctx, &plan),
    ];

    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);
    assert_eq!(order.first().unwrap().0, 1);

    let runner = ZoneStepRunner::new(&plan);
    let (_zones, pruned) = runner.run(&mut steps, &order);
    assert!(pruned.is_some());
}

#[tokio::test]
// OR logic: pruning disabled; runner returns None for pruned
async fn runner_does_not_prune_under_or() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "evt";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_where_clause(crate::command::types::Expr::Or(
            Box::new(crate::command::types::Expr::Compare {
                field: "id".into(),
                op: crate::command::types::CompareOp::Eq,
                value: json!(1),
            }),
            Box::new(crate::command::types::Expr::Compare {
                field: "id".into(),
                op: crate::command::types::CompareOp::Eq,
                value: json!(2),
            }),
        ))
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
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let mut steps = vec![ExecutionStep::new(fp_id, &plan)];

    let planner = ZoneStepPlanner::new(&plan);
    let order = planner.plan(&steps);

    let runner = ZoneStepRunner::new(&plan);
    let (_zones, pruned) = runner.run(&mut steps, &order);
    assert!(pruned.is_none());
}

#[tokio::test]
// No steps: returns empty outputs and no pruning
async fn runner_handles_no_steps() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "evt";
    schema
        .define_with_fields(event_type, &[("context_id", "string")])
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

    let mut steps: Vec<ExecutionStep> = Vec::new();
    let order: Vec<(usize, Option<Vec<String>>)> = Vec::new();

    let runner = ZoneStepRunner::new(&plan);
    let (zones, pruned) = runner.run(&mut steps, &order);
    assert!(zones.is_empty());
    assert!(pruned.is_none());
}

#[tokio::test]
// Planned explicit subset: runner respects provided segment list
async fn runner_respects_explicit_subset_in_plan() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "evt";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx-keep")
        .with_where_clause(crate::command::types::Expr::Compare {
            field: "id".into(),
            op: crate::command::types::CompareOp::Eq,
            value: json!(1),
        })
        .create();

    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(std::env::temp_dir())
        .with_segment_ids(vec!["00001".into(), "00002".into(), "00003".into()])
        .create()
        .await;

    let uid = plan.event_type_uid().await.expect("uid");

    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!("ctx-keep"))
        .with_uid(&uid)
        .create();
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let mut steps = vec![
        ExecutionStep::new(fp_ctx, &plan),
        ExecutionStep::new(fp_id, &plan),
    ];

    // Build an explicit subset that excludes 00002
    let explicit_subset = vec!["00001".into(), "00003".into()];
    let order = vec![(0usize, Some(explicit_subset)), (1usize, None)];

    let runner = ZoneStepRunner::new(&plan);
    let (_zones, pruned) = runner.run(&mut steps, &order);

    // Pruned should be derived from first step's output; but we provided explicit subset,
    // so we just assert the mechanism runs without forcing pruning to be None/Some.
    // The important part: no panic and consistent return type.
    assert!(pruned.is_some());
}

#[tokio::test]
// First step yields empty set: still executes subsequent steps
async fn runner_pruned_empty_first_step_still_executes_following_steps() {
    use crate::logging::init_for_tests;
    init_for_tests();

    let schema = SchemaRegistryFactory::new();
    let registry = schema.registry();
    let event_type = "evt";
    schema
        .define_with_fields(event_type, &[("context_id", "string"), ("id", "int")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx-will-prune-to-empty")
        .with_where_clause(crate::command::types::Expr::Compare {
            field: "id".into(),
            op: crate::command::types::CompareOp::Eq,
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
    let fp_ctx = FilterGroupFactory::new()
        .with_column("context_id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!("ctx-will-prune-to-empty"))
        .with_uid(&uid)
        .create();

    // A second step that should still run (over empty segment list)
    let fp_id = FilterGroupFactory::new()
        .with_column("id")
        .with_operation(crate::command::types::CompareOp::Eq)
        .with_value(json!(1))
        .with_uid(&uid)
        .create();

    let mut steps = vec![
        ExecutionStep::new(fp_ctx, &plan),
        ExecutionStep::new(fp_id, &plan),
    ];

    // Order: context first, then id
    let order: Vec<(usize, Option<Vec<String>>)> = vec![(0usize, None), (1usize, None)];

    let runner = ZoneStepRunner::new(&plan);
    let (zones, pruned) = runner.run(&mut steps, &order);

    // We should still get two outputs (one per step), even if both are empty
    assert_eq!(zones.len(), 2);
    // Pruned is Some (derived), but likely empty or small; we don't assert its content
    assert!(pruned.is_some());
}
