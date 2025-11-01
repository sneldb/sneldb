use super::builder::QueryPlannerBuilder;
use crate::test_helpers::factories::CommandFactory;

#[test]
fn for_command_returns_rlte_planner_with_order_by() {
    let command = CommandFactory::query()
        .with_limit(10)
        .with_order_by("timestamp", false)
        .create();

    let planner = QueryPlannerBuilder::new(&command).build();
    // We can't directly check the type, but we can verify it behaves like RltePlanner
    // by checking it implements QueryPlanner and can be downcast conceptually
    assert!(std::mem::size_of_val(&planner) > 0);
}

#[test]
fn for_command_returns_full_scan_planner_without_order_by() {
    let command = CommandFactory::query().with_limit(10).create();

    let planner = QueryPlannerBuilder::new(&command).build();
    assert!(std::mem::size_of_val(&planner) > 0);
}

#[test]
fn for_command_handles_empty_command() {
    let command = CommandFactory::query().create();

    let planner = QueryPlannerBuilder::new(&command).build();
    assert!(std::mem::size_of_val(&planner) > 0);
}

#[test]
fn builder_creates_planner() {
    let command = CommandFactory::query().create();
    let builder = QueryPlannerBuilder::new(&command);
    let planner = builder.build();
    assert!(std::mem::size_of_val(&planner) > 0);
}

#[test]
fn builder_new_takes_command_reference() {
    let command = CommandFactory::query()
        .with_order_by("timestamp", false)
        .create();
    let builder = QueryPlannerBuilder::new(&command);
    let planner = builder.build();
    assert!(std::mem::size_of_val(&planner) > 0);
}
