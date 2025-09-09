use crate::command::types::CompareOp;
use crate::test_helpers::factories::CompareOpFactory;

#[test]
fn test_compare_op_factory_default() {
    let op = CompareOpFactory::new().create();
    assert_eq!(op, CompareOp::Eq);
}

#[test]
fn test_compare_op_factory_with_custom_op() {
    let op = CompareOpFactory::new().with_op(CompareOp::Gt).create();
    assert_eq!(op, CompareOp::Gt);
}

#[test]
fn test_compare_op_factory_all_variants() {
    let all_ops = CompareOpFactory::all();

    let expected = vec![
        CompareOp::Eq,
        CompareOp::Neq,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::Lt,
        CompareOp::Lte,
    ];

    assert_eq!(all_ops, expected);
}
