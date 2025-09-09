use crate::command::types::{CompareOp, Expr};
use crate::test_helpers::factories::ExprFactory;
use serde_json::json;

#[test]
fn test_expr_factory_default_compare() {
    let expr = ExprFactory::new().create();
    let expected = Expr::Compare {
        field: "foo".to_string(),
        op: CompareOp::Eq,
        value: json!("bar"),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_expr_factory_custom_compare() {
    let expr = ExprFactory::new()
        .with_field("age")
        .with_op(CompareOp::Gt)
        .with_value(json!(30))
        .create();

    let expected = Expr::Compare {
        field: "age".to_string(),
        op: CompareOp::Gt,
        value: json!(30),
    };
    assert_eq!(expr, expected);
}

#[test]
fn test_expr_factory_and_expr() {
    let left = ExprFactory::new().with_field("a").create();
    let right = ExprFactory::new().with_field("b").create();
    let expr = ExprFactory::and(left.clone(), right.clone());

    assert_eq!(expr, Expr::And(Box::new(left), Box::new(right)));
}

#[test]
fn test_expr_factory_or_expr() {
    let left = ExprFactory::new().with_field("x").create();
    let right = ExprFactory::new().with_field("y").create();
    let expr = ExprFactory::or(left.clone(), right.clone());

    assert_eq!(expr, Expr::Or(Box::new(left), Box::new(right)));
}

#[test]
fn test_expr_factory_not_expr() {
    let inner = ExprFactory::new().with_field("z").create();
    let expr = ExprFactory::not(inner.clone());

    assert_eq!(expr, Expr::Not(Box::new(inner)));
}
