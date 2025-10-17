use crate::command::types::CompareOp as CommandCompareOp;
use crate::command::types::Expr;
use crate::engine::core::ColumnValues;
use crate::engine::core::Condition;
use crate::engine::core::filter::condition::{CompareOp, FieldAccessor, PreparedAccessor};
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::{LogicalCondition, LogicalOp, NumericCondition, StringCondition};
use crate::test_helpers::factories::candidate_zone_factory::CandidateZoneFactory;
use std::collections::HashMap;
use std::sync::Arc;

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

// ---------------- Additional edge-case tests reflecting SIMD-aware changes ----------------

fn make_typed_u64_column(values: &[u64]) -> ColumnValues {
    let mut buf: Vec<u8> = Vec::with_capacity(values.len() * 8);
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    let block = Arc::new(DecompressedBlock::from_bytes(buf));
    ColumnValues::new_typed_u64(block, 0, values.len(), None)
}

#[test]
fn numeric_condition_evaluate_at_prefers_u64_and_handles_negative() {
    // Build a single-zone with typed u64 id values
    let mut zone = crate::engine::core::CandidateZone::new(0, "seg".into());
    let mut cols = std::collections::HashMap::new();
    cols.insert("id".to_string(), make_typed_u64_column(&[1, 10, 25]));
    zone.set_values(cols);

    let accessor = PreparedAccessor::new(&zone.values);

    let cond_eq_10 = NumericCondition::new("id".into(), CompareOp::Eq, 10);
    assert!(cond_eq_10.evaluate_at(&accessor, 1));
    assert!(!cond_eq_10.evaluate_at(&accessor, 0));

    // Negative RHS cannot match u64 lhs
    let cond_neg = NumericCondition::new("id".into(), CompareOp::Gt, -1);
    assert!(!cond_neg.evaluate_at(&accessor, 0));
    assert!(!cond_neg.evaluate_at(&accessor, 1));
}

#[test]
fn numeric_condition_collects_fields_and_is_numeric() {
    let nc = NumericCondition::new("score".into(), CompareOp::Gte, 100);
    assert!(nc.is_numeric());
    let mut set = std::collections::HashSet::new();
    nc.collect_numeric_fields(&mut set);
    assert!(set.contains("score"));

    let sc = StringCondition::new("kind".into(), CompareOp::Eq, "x".into());
    assert!(!sc.is_numeric());
    let mut set2 = std::collections::HashSet::new();
    sc.collect_numeric_fields(&mut set2);
    assert!(set2.is_empty());
}

#[test]
fn string_condition_only_eq_neq_true_other_ops_false() {
    let mut values = std::collections::HashMap::new();
    values.insert("s".into(), vec!["a".into()]);
    let eq = StringCondition::new("s".into(), CompareOp::Eq, "a".into());
    let neq = StringCondition::new("s".into(), CompareOp::Neq, "b".into());
    let gt = StringCondition::new("s".into(), CompareOp::Gt, "a".into());
    let lt = StringCondition::new("s".into(), CompareOp::Lt, "a".into());
    assert!(eq.evaluate(&values));
    assert!(neq.evaluate(&values));
    assert!(!gt.evaluate(&values));
    assert!(!lt.evaluate(&values));
}

#[test]
fn prepared_accessor_warm_numeric_cache_and_parse_i64() {
    // Build zone via factory with numeric strings
    let mut m = std::collections::HashMap::new();
    m.insert("x".into(), vec!["10".into(), "20".into(), "30".into()]);
    let zone = CandidateZoneFactory::new().with_values(m).create();
    let accessor = PreparedAccessor::new(&zone.values);

    // Warm cache for column x
    let mut fields = std::collections::HashSet::new();
    fields.insert("x".to_string());
    accessor.warm_numeric_cache(&fields);

    // Ensure parsed numeric reads work
    assert_eq!(accessor.get_i64_at("x", 0), Some(10));
    assert_eq!(accessor.get_i64_at("x", 2), Some(30));
}

#[test]
fn logical_collect_numeric_fields_accumulates_nested() {
    let a = Box::new(NumericCondition::new("a".into(), CompareOp::Gt, 1));
    let b = Box::new(NumericCondition::new("b".into(), CompareOp::Lt, 9));
    let c = Box::new(StringCondition::new("c".into(), CompareOp::Eq, "z".into()));
    let and = LogicalCondition::new(vec![a, b, c], LogicalOp::And);
    let mut set = std::collections::HashSet::new();
    and.collect_numeric_fields(&mut set);
    assert!(set.contains("a"));
    assert!(set.contains("b"));
    assert!(!set.contains("c"));
}

#[test]
fn evaluate_event_direct_numeric_and_string() {
    use crate::engine::core::Event;
    use crate::engine::core::filter::direct_event_accessor::DirectEventAccessor;

    // Build an Event via EventBuilder-like fields
    let mut eb = crate::engine::core::event::event_builder::EventBuilder::new();
    eb.add_field("event_type", "login");
    eb.add_field("context_id", "abc");
    eb.add_field("timestamp", "123");
    eb.add_field("score", "42");
    eb.add_field("kind", "x");
    let ev: Event = eb.build();

    let acc = DirectEventAccessor::new(&ev);

    let n_ok = NumericCondition::new("score".into(), CompareOp::Gte, 40);
    let n_no = NumericCondition::new("score".into(), CompareOp::Gt, 100);
    let s_ok = StringCondition::new("kind".into(), CompareOp::Eq, "x".into());
    let s_no = StringCondition::new("kind".into(), CompareOp::Neq, "x".into());

    assert!(n_ok.evaluate_event_direct(&acc));
    assert!(!n_no.evaluate_event_direct(&acc));
    assert!(s_ok.evaluate_event_direct(&acc));
    assert!(!s_no.evaluate_event_direct(&acc));
}

#[test]
fn logical_not_nested_with_mixed_types() {
    // Build zone with numeric string and plain string
    let mut values = HashMap::new();
    values.insert(
        "ts".to_string(),
        vec!["100".into(), "200".into(), "300".into()],
    );
    values.insert("mode".to_string(), vec!["a".into(), "b".into(), "c".into()]);
    let zone = CandidateZoneFactory::new().with_values(values).create();
    let accessor = PreparedAccessor::new(&zone.values);

    // NOT( ts < 250 AND mode == b )  -> rows that are NOT (true AND true)
    let and = LogicalCondition::new(
        vec![
            Box::new(NumericCondition::new("ts".into(), CompareOp::Lt, 250)),
            Box::new(StringCondition::new(
                "mode".into(),
                CompareOp::Eq,
                "b".into(),
            )),
        ],
        LogicalOp::And,
    );
    let not = LogicalCondition::new(vec![Box::new(and)], LogicalOp::Not);

    // row 0: 100,a => inner AND false (mode!=b) -> NOT(true? no) => overall true
    // row 1: 200,b => inner AND true -> NOT => false
    // row 2: 300,c => inner AND false (ts<250 false) -> NOT => true
    assert!(not.evaluate_at(&accessor, 0));
    assert!(!not.evaluate_at(&accessor, 1));
    assert!(not.evaluate_at(&accessor, 2));
}
