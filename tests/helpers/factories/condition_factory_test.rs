use crate::engine::core::filter::condition::LogicalOp;
use crate::test_helpers::factories::ConditionFactory;
use std::collections::HashMap;

#[test]
fn builds_and_evaluates_complex_condition() {
    let numeric = ConditionFactory::new_numeric()
        .with("field", "count")
        .with("op", "Gt")
        .with("value", 10)
        .create();

    let string = ConditionFactory::new_string()
        .with("field", "type")
        .with("op", "Eq")
        .with("value", "event")
        .create();

    let logical = ConditionFactory::new_logical(LogicalOp::And)
        .with_conditions(vec![numeric, string])
        .create();

    let mut values = HashMap::new();
    values.insert("count".into(), vec!["42".into()]);
    values.insert("type".into(), vec!["event".into()]);

    assert!(logical.evaluate(&values));
}
