use crate::command::types::CompareOp;
use crate::test_helpers::factories::FilterPlanFactory;
use serde_json::json;

#[test]
fn creates_filter_plan_with_expected_fields() {
    let filter = FilterPlanFactory::new()
        .with_column("amount")
        .with_operation(CompareOp::Gte)
        .with_value(json!(100))
        .with_priority(2)
        .with_uid("uid123")
        .create();

    assert_eq!(filter.column, "amount");
    assert_eq!(filter.operation, Some(CompareOp::Gte));
    assert_eq!(filter.value, Some(json!(100)));
    assert_eq!(filter.priority, 2);
    assert_eq!(filter.uid.as_deref(), Some("uid123"));
}
