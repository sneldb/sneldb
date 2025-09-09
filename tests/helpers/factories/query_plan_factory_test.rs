use crate::test_helpers::factories::{CommandFactory, QueryPlanFactory, SchemaRegistryFactory};

#[tokio::test]
async fn test_query_plan_factory_builds_query_plan_correctly() {
    // Setup schema registry with test schema
    let schema_factory = SchemaRegistryFactory::new();
    schema_factory
        .define_with_fields("test_event", &[("id", "int"), ("name", "string")])
        .await
        .expect("Failed to define schema");

    // Create a query command
    let command = CommandFactory::query()
        .with_event_type("test_event")
        .with_context_id("ctx42")
        .with_limit(5)
        .create();

    // Build query plan using the factory
    let query_plan = QueryPlanFactory::new()
        .with_command(command.clone())
        .with_registry(schema_factory.registry())
        .with_segment_base_dir("/tmp/queryplan_test")
        .with_segment_ids(vec!["seg-A".into(), "seg-B".into()])
        .create()
        .await;

    // Assertions
    assert_eq!(query_plan.event_type(), "test_event");
    assert_eq!(query_plan.context_id(), Some("ctx42"));
    assert_eq!(
        query_plan.segment_base_dir.to_str().unwrap(),
        "/tmp/queryplan_test"
    );

    let ids = query_plan.segment_ids.read().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], "seg-A");
    assert!(query_plan.metadata.is_empty());
}
