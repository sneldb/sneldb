use crate::engine::core::filter::condition::CompareOp;
use crate::engine::core::filter::direct_event_accessor::DirectEventAccessor;
use crate::engine::core::{
    Condition, ConditionEvaluator, LogicalCondition, LogicalOp, NumericCondition, StringCondition,
};
use crate::test_helpers::factories::EventFactory;
use serde_json::json;

#[test]
fn direct_accessor_gets_field_values_correctly() {
    let event = EventFactory::new()
        .with("context_id", "ctx123")
        .with("event_type", "login")
        .with("timestamp", 1234567890_u64)
        .with("payload", json!({"user": "alice", "score": 100}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    assert_eq!(accessor.get_field_value("context_id"), "ctx123");
    assert_eq!(accessor.get_field_value("event_type"), "login");
    assert_eq!(accessor.get_field_value("timestamp"), "1234567890");
    assert_eq!(accessor.get_field_value("user"), "alice");
    assert_eq!(accessor.get_field_value("score"), "100");
    assert_eq!(accessor.get_field_value("nonexistent"), "");
}

#[test]
fn direct_accessor_gets_i64_values_correctly() {
    let event = EventFactory::new()
        .with("timestamp", 9999_u64)
        .with(
            "payload",
            json!({"score": 42, "level": "5", "name": "test"}),
        )
        .create();

    let accessor = DirectEventAccessor::new(&event);

    assert_eq!(accessor.get_field_as_i64("timestamp"), Some(9999));
    assert_eq!(accessor.get_field_as_i64("score"), Some(42));
    assert_eq!(accessor.get_field_as_i64("level"), Some(5)); // string parsed to int
    assert_eq!(accessor.get_field_as_i64("name"), None); // can't parse
    assert_eq!(accessor.get_field_as_i64("missing"), None);
}

#[test]
fn numeric_condition_evaluate_event_direct_works() {
    let event = EventFactory::new()
        .with("payload", json!({"id": 50, "age": 25}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let gt = NumericCondition::new("id".into(), CompareOp::Gt, 40);
    let lt = NumericCondition::new("id".into(), CompareOp::Lt, 40);
    let eq = NumericCondition::new("age".into(), CompareOp::Eq, 25);

    assert!(gt.evaluate_event_direct(&accessor));
    assert!(!lt.evaluate_event_direct(&accessor));
    assert!(eq.evaluate_event_direct(&accessor));
}

#[test]
fn string_condition_evaluate_event_direct_works() {
    let event = EventFactory::new()
        .with("event_type", "signup")
        .with("payload", json!({"plan": "pro", "region": "us"}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let eq_type = StringCondition::new("event_type".into(), CompareOp::Eq, "signup".into());
    let eq_plan = StringCondition::new("plan".into(), CompareOp::Eq, "pro".into());
    let neq_region = StringCondition::new("region".into(), CompareOp::Neq, "eu".into());

    assert!(eq_type.evaluate_event_direct(&accessor));
    assert!(eq_plan.evaluate_event_direct(&accessor));
    assert!(neq_region.evaluate_event_direct(&accessor));
}

#[test]
fn logical_condition_evaluate_event_direct_and_works() {
    let event = EventFactory::new()
        .with("payload", json!({"score": 85, "status": "active"}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let cond1 = Box::new(NumericCondition::new("score".into(), CompareOp::Gte, 80));
    let cond2 = Box::new(StringCondition::new(
        "status".into(),
        CompareOp::Eq,
        "active".into(),
    ));

    let and = LogicalCondition::new(vec![cond1, cond2], LogicalOp::And);
    assert!(and.evaluate_event_direct(&accessor));
}

#[test]
fn logical_condition_evaluate_event_direct_or_works() {
    let event = EventFactory::new()
        .with("payload", json!({"vip": false, "score": 95}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let cond1 = Box::new(StringCondition::new(
        "vip".into(),
        CompareOp::Eq,
        "true".into(),
    )); // false
    let cond2 = Box::new(NumericCondition::new("score".into(), CompareOp::Gt, 90)); // true

    let or = LogicalCondition::new(vec![cond1, cond2], LogicalOp::Or);
    assert!(or.evaluate_event_direct(&accessor));
}

#[test]
fn logical_condition_evaluate_event_direct_not_works() {
    let event = EventFactory::new()
        .with("payload", json!({"banned": true}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let cond = Box::new(StringCondition::new(
        "banned".into(),
        CompareOp::Eq,
        "false".into(),
    ));
    let not = LogicalCondition::new(vec![cond], LogicalOp::Not);

    assert!(not.evaluate_event_direct(&accessor));
}

#[test]
fn condition_evaluator_evaluate_event_uses_direct_accessor() {
    let event = EventFactory::new()
        .with("payload", json!({"id": 100, "active": true}))
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_numeric_condition("id".into(), CompareOp::Lt, 150);
    evaluator.add_string_condition("active".into(), CompareOp::Eq, "true".into());

    // Should use DirectEventAccessor internally
    assert!(evaluator.evaluate_event(&event));
}

#[test]
fn condition_evaluator_evaluate_event_short_circuits() {
    let event = EventFactory::new()
        .with(
            "payload",
            json!({"id": 50, "name": "test", "extra": "data"}),
        )
        .create();

    let mut evaluator = ConditionEvaluator::new();
    // First condition fails
    evaluator.add_numeric_condition("id".into(), CompareOp::Gt, 100); // false
    // Second condition would pass, but shouldn't be evaluated due to short-circuit
    evaluator.add_string_condition("name".into(), CompareOp::Eq, "test".into());

    assert!(!evaluator.evaluate_event(&event));
}

#[test]
fn condition_evaluator_evaluate_event_with_missing_fields() {
    let event = EventFactory::new()
        .with("payload", json!({"existing": "value"}))
        .create();

    let mut evaluator = ConditionEvaluator::new();
    // Condition on missing field should return empty string, which won't parse as number
    evaluator.add_numeric_condition("missing_field".into(), CompareOp::Eq, 42);

    assert!(!evaluator.evaluate_event(&event));
}

#[test]
fn direct_accessor_handles_boolean_values() {
    let event = EventFactory::new()
        .with("payload", json!({"active": true, "deleted": false}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    assert_eq!(accessor.get_field_value("active"), "true");
    assert_eq!(accessor.get_field_value("deleted"), "false");
}

#[test]
fn direct_accessor_handles_null_values() {
    let event = EventFactory::new()
        .with("payload", json!({"optional": null}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    assert_eq!(accessor.get_field_value("optional"), "null");
}

#[test]
fn direct_accessor_handles_nested_objects() {
    let event = EventFactory::new()
        .with("payload", json!({"user": {"name": "alice"}}))
        .create();

    let accessor = DirectEventAccessor::new(&event);

    // Nested objects are serialized as JSON strings
    assert_eq!(accessor.get_field_value("user"), r#"{"name":"alice"}"#);
}

#[test]
fn numeric_condition_direct_eval_with_string_numbers() {
    let event = EventFactory::new()
        .with("payload", json!({"count": "123"})) // string number
        .create();

    let accessor = DirectEventAccessor::new(&event);

    let cond = NumericCondition::new("count".into(), CompareOp::Eq, 123);
    assert!(cond.evaluate_event_direct(&accessor)); // should parse "123" to 123
}
