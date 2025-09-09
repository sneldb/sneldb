use crate::command::types::{CompareOp, Expr};
use crate::engine::core::ExecutionStep;
use crate::test_helpers::factories::{
    CommandFactory, FieldXorFilterFactory, FilterPlanFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};
use serde_json::json;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn execution_step_resolves_candidate_zones_from_filter_and_plan() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: Create a temporary segment directory for the test
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let segment_path = temp_dir.path().join("seg1");
    fs::create_dir_all(&segment_path).expect("Failed to create segment directory");

    // Step 1: Prepare a dummy schema registry
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "ex_step_event";
    registry_factory
        .define_with_fields(event_type, &[("plan", "string")])
        .await
        .expect("Schema definition failed");

    // Step 2: Create a Command::Query using CommandFactory
    let where_clause = Expr::Compare {
        field: "plan".to_string(),
        op: CompareOp::Eq,
        value: json!("premium"),
    };
    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found for 'ex_step_event'");

    let command = CommandFactory::query()
        .with_event_type(event_type)
        .with_context_id("ctx1")
        .with_limit(5)
        .with_where_clause(where_clause.clone())
        .create();

    // Step 3: Build the QueryPlan
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(&temp_dir)
        .with_segment_ids(vec!["seg1".to_string()])
        .create()
        .await;

    // Step 4: Create FilterPlan
    let filter = FilterPlanFactory::new()
        .with_column("plan")
        .with_operation(CompareOp::Eq)
        .with_value(json!("premium"))
        .with_uid(&uid)
        .with_priority(2)
        .create();

    let xor_filter = FieldXorFilterFactory::new().with(json!("premium")).build();

    let xf_path = segment_path.join(format!("{}_plan.xf", uid));
    xor_filter
        .save(&xf_path)
        .expect("Failed to save xor filter");

    // Step 5: Create ExecutionStep and resolve candidate zones
    let mut step = ExecutionStep::new(filter, &plan);
    step.get_candidate_zones();

    // Step 6: Validate
    assert!(
        !step.candidate_zones.is_empty(),
        "Expected candidate zones, but found none"
    );

    assert_eq!(
        step.candidate_zones[0].segment_id, "seg1",
        "Expected segment_id to be 'seg1'"
    );
}
