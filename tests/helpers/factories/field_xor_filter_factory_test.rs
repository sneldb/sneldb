use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::FieldXorFilterFactory;
use serde_json::json;

#[test]
fn builds_filter_and_checks_membership() {
    // Build filter with default + custom values
    let filter = FieldXorFilterFactory::new()
        .with(json!("kiwi"))
        .with(json!(100))
        .build();

    // These should exist
    assert!(filter.contains_value(&ScalarValue::from(json!("apple"))));
    assert!(filter.contains_value(&ScalarValue::from(json!("banana"))));
    assert!(filter.contains_value(&ScalarValue::from(json!(42))));
    assert!(filter.contains_value(&ScalarValue::from(json!(true))));
    assert!(filter.contains_value(&ScalarValue::from(json!("kiwi"))));
    assert!(filter.contains_value(&ScalarValue::from(json!(100))));
}
