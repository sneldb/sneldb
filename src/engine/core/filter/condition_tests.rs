use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use crate::engine::core::Condition;
use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::filter::condition::CompareOp;
use crate::engine::core::filter::condition::PreparedAccessor;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::{LogicalCondition, LogicalOp, NumericCondition, StringCondition};
use std::collections::HashMap;
use std::sync::Arc;

fn build_column_values_map(values: &HashMap<String, Vec<String>>) -> HashMap<String, ColumnValues> {
    let mut out = HashMap::new();
    for (field, vec_str) in values.iter() {
        // Encode as [u16 len][bytes] per value
        let mut bytes: Vec<u8> = Vec::new();
        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(vec_str.len());
        for s in vec_str {
            let b = s.as_bytes();
            let len = b.len() as u16;
            let start = bytes.len();
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(b);
            ranges.push((start + 2, b.len()));
        }
        let block = Arc::new(DecompressedBlock::from_bytes(bytes));
        out.insert(field.clone(), ColumnValues::new(block, ranges));
    }
    out
}

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

#[test]
fn numeric_condition_evaluate_mask_works() {
    let mut values = HashMap::new();
    values.insert(
        "age".to_string(),
        vec!["18".to_string(), "25".to_string(), "30".to_string()],
    );

    let cv_map = build_column_values_map(&values);
    let accessor = PreparedAccessor::new(&cv_map);
    let cond = NumericCondition::new("age".into(), CompareOp::Gte, 20);
    let mask = cond.evaluate_mask(&accessor);
    assert_eq!(mask, vec![false, true, true]);
}

#[test]
fn string_condition_evaluate_mask_works() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec![
            "active".to_string(),
            "pending".to_string(),
            "active".to_string(),
        ],
    );

    let cv_map = build_column_values_map(&values);
    let accessor = PreparedAccessor::new(&cv_map);
    let cond = StringCondition::new("status".into(), CompareOp::Eq, "active".into());
    let mask = cond.evaluate_mask(&accessor);
    assert_eq!(mask, vec![true, false, true]);
}

#[test]
fn logical_condition_evaluate_mask_works() {
    let mut values = HashMap::new();
    values.insert(
        "age".to_string(),
        vec!["18".to_string(), "25".to_string(), "30".to_string()],
    );
    values.insert(
        "status".to_string(),
        vec![
            "active".to_string(),
            "pending".to_string(),
            "active".to_string(),
        ],
    );

    let cv_map = build_column_values_map(&values);
    let accessor = PreparedAccessor::new(&cv_map);
    let c1 = Box::new(NumericCondition::new("age".into(), CompareOp::Gte, 20));
    let c2 = Box::new(StringCondition::new(
        "status".into(),
        CompareOp::Eq,
        "active".into(),
    ));
    let and = LogicalCondition::new(vec![c1, c2], LogicalOp::And);

    let mask = and.evaluate_mask(&accessor);
    assert_eq!(mask, vec![false, false, true]);
}
