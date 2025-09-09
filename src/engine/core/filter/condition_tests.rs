use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use crate::engine::core::Condition;
use crate::engine::core::filter::condition::CompareOp;
use crate::engine::core::{LogicalCondition, LogicalOp, NumericCondition, StringCondition};
use std::collections::HashMap;

#[test]
fn numeric_condition_evaluates_correctly() {
    let mut values = HashMap::new();
    values.insert("age".to_string(), vec!["18".to_string(), "25".to_string()]);

    let gt = NumericCondition::new("age".into(), CompareOp::Gt, 20);
    let lte = NumericCondition::new("age".into(), CompareOp::Lte, 18);
    let neq = NumericCondition::new("age".into(), CompareOp::Neq, 25);

    assert!(gt.evaluate(&values)); // 25 > 20
    assert!(lte.evaluate(&values)); // 18 <= 18
    assert!(neq.evaluate(&values)); // 18 != 25
}

#[test]
fn numeric_condition_returns_false_on_non_numeric_values() {
    let mut values = HashMap::new();
    values.insert("score".into(), vec!["NaN".into()]);

    let cond = NumericCondition::new("score".into(), CompareOp::Gt, 10);
    assert!(!cond.evaluate(&values));
}

#[test]
fn string_condition_evaluates_correctly() {
    let mut values = HashMap::new();
    values.insert("status".into(), vec!["active".into(), "pending".into()]);

    let eq = StringCondition::new("status".into(), CompareOp::Eq, "active".into());
    let neq = StringCondition::new("status".into(), CompareOp::Neq, "inactive".into());

    assert!(eq.evaluate(&values));
    assert!(neq.evaluate(&values));
}

#[test]
fn logical_condition_and_works() {
    let mut values = HashMap::new();
    values.insert("a".into(), vec!["1".into()]);
    values.insert("b".into(), vec!["2".into()]);

    let cond1 = Box::new(NumericCondition::new("a".into(), CompareOp::Eq, 1));
    let cond2 = Box::new(NumericCondition::new("b".into(), CompareOp::Eq, 2));

    let and = LogicalCondition::new(vec![cond1, cond2], LogicalOp::And);
    assert!(and.evaluate(&values));
}

#[test]
fn logical_condition_or_works() {
    let mut values = HashMap::new();
    values.insert("a".into(), vec!["1".into()]);
    values.insert("b".into(), vec!["5".into()]);

    let cond1 = Box::new(NumericCondition::new("a".into(), CompareOp::Eq, 99)); // false
    let cond2 = Box::new(NumericCondition::new("b".into(), CompareOp::Eq, 5)); // true

    let or = LogicalCondition::new(vec![cond1, cond2], LogicalOp::Or);
    assert!(or.evaluate(&values));
}

#[test]
fn logical_condition_not_works() {
    let mut values = HashMap::new();
    values.insert("score".into(), vec!["100".into()]);

    let cond = Box::new(NumericCondition::new("score".into(), CompareOp::Eq, 100));
    let not = LogicalCondition::new(vec![cond], LogicalOp::Not);

    assert!(!not.evaluate(&values));
}

#[test]
fn logical_op_from_expr_and_requires_special_handling() {
    let op = LogicalOp::from_expr(Some(&Expr::Not(Box::new(Expr::Compare {
        field: "x".into(),
        op: CommandCompareOp::Eq,
        value: "42".into(),
    }))));

    assert_eq!(op, LogicalOp::Not);
    assert!(op.requires_special_handling());
}
