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
    assert!(filter.contains_value(&json!("apple")));
    assert!(filter.contains_value(&json!("banana")));
    assert!(filter.contains_value(&json!(42)));
    assert!(filter.contains_value(&json!(true)));
    assert!(filter.contains_value(&json!("kiwi")));
    assert!(filter.contains_value(&json!(100)));
}
