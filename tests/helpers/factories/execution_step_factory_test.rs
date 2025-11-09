use crate::command::types::{CompareOp, Expr};
use crate::test_helpers::factories::{
    CommandFactory, ExecutionStepFactory, FilterGroupFactory, QueryPlanFactory,
    SchemaRegistryFactory,
};

use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
#[tokio::test]
async fn builds_execution_step_with_valid_filter_and_plan() {
    use crate::logging::init_for_tests;
    init_for_tests();

    // Setup: Schema registry
    let schema_factory = SchemaRegistryFactory::new();
    let registry = schema_factory.registry();
    schema_factory
        .define_with_fields("test_event", &[("status", "string")])
        .await
        .unwrap();
    let uid = registry.read().await.get_uid("test_event").unwrap();

    // Create Command
    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_where_clause(Expr::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: json!("ok"),
        })
        .create();

    // Create QueryPlan
    let temp_dir = tempdir().unwrap();
    let plan = QueryPlanFactory::new()
        .with_command(command)
        .with_registry(Arc::clone(&registry))
        .with_segment_base_dir(temp_dir.path())
        .create()
        .await;

    // Create FilterPlan
    let filter = FilterGroupFactory::new()
        .with_column("status")
        .with_operation(CompareOp::Eq)
        .with_value(json!("ok"))
        .with_uid(&uid)
        .create();

    // Build ExecutionStep using the factory
    let step = ExecutionStepFactory::new()
        .with_filter(filter.clone())
        .with_plan(&plan)
        .create();

    // Verify
    assert_eq!(step.filter.column(), Some("status"));
    match &step.filter {
        crate::engine::core::filter::filter_group::FilterGroup::Filter { uid: filter_uid, .. } => {
            assert_eq!(filter_uid.as_deref(), Some(uid.as_str()));
        }
        _ => panic!("Expected Filter variant"),
    }
    assert!(step.candidate_zones.is_empty());
}
