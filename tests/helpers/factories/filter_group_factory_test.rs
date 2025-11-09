use crate::command::types::CompareOp;
use crate::engine::core::filter::filter_group::FilterGroup;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::FilterGroupFactory;
use serde_json::json;

#[test]
fn creates_filter_group_with_expected_fields() {
    let filter = FilterGroupFactory::new()
        .with_column("amount")
        .with_operation(CompareOp::Gte)
        .with_value(json!(100))
        .with_priority(2)
        .with_uid("uid123")
        .create();

    match filter {
        FilterGroup::Filter {
            column,
            operation,
            value,
            priority,
            uid,
            ..
        } => {
            assert_eq!(column, "amount");
            assert_eq!(operation, Some(CompareOp::Gte));
            assert_eq!(value, Some(ScalarValue::from(json!(100))));
            assert_eq!(priority, 2);
            assert_eq!(uid.as_deref(), Some("uid123"));
        }
        _ => panic!("Expected Filter variant"),
    }
}
