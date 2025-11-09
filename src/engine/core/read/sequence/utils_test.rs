use crate::command::types::{CompareOp, Expr};
use crate::engine::core::read::sequence::utils::{
    parse_event_field, scalar_to_string, transform_where_clause_for_event_type,
};
use crate::engine::types::ScalarValue;
use serde_json::json;

#[test]
fn scalar_to_string_utf8() {
    let value = ScalarValue::Utf8("hello".to_string());
    assert_eq!(scalar_to_string(&value), "hello");
}

#[test]
fn scalar_to_string_int64() {
    let value = ScalarValue::Int64(42);
    assert_eq!(scalar_to_string(&value), "42");
}

#[test]
fn scalar_to_string_float64() {
    let value = ScalarValue::Float64(3.14);
    assert_eq!(scalar_to_string(&value), "3.14");
}

#[test]
fn scalar_to_string_boolean() {
    assert_eq!(scalar_to_string(&ScalarValue::Boolean(true)), "true");
    assert_eq!(scalar_to_string(&ScalarValue::Boolean(false)), "false");
}

#[test]
fn scalar_to_string_timestamp() {
    let value = ScalarValue::Timestamp(1234567890);
    assert_eq!(scalar_to_string(&value), "1234567890");
}

#[test]
fn scalar_to_string_binary() {
    let value = ScalarValue::Binary(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello" in bytes
    let result = scalar_to_string(&value);
    // Base64 encoding of "Hello"
    assert_eq!(result, "SGVsbG8=");
}

#[test]
fn scalar_to_string_null() {
    assert_eq!(scalar_to_string(&ScalarValue::Null), "null");
}

#[test]
fn parse_event_field_with_prefix() {
    let result = parse_event_field("purchase.amount");
    assert_eq!(result, Some(("purchase".to_string(), "amount".to_string())));
}

#[test]
fn parse_event_field_with_underscore_event_type() {
    let result = parse_event_field("page_view.page");
    assert_eq!(result, Some(("page_view".to_string(), "page".to_string())));
}

#[test]
fn parse_event_field_without_prefix() {
    let result = parse_event_field("timestamp");
    assert_eq!(result, None);
}

#[test]
fn parse_event_field_without_prefix_common_field() {
    let result = parse_event_field("user_id");
    assert_eq!(result, None);
}

#[test]
fn parse_event_field_multiple_dots() {
    // Takes first dot as separator
    let result = parse_event_field("event.field.subfield");
    assert_eq!(result, Some(("event".to_string(), "field.subfield".to_string())));
}

#[test]
fn transform_where_clause_simple_compare_matching_event() {
    let expr = Expr::Compare {
        field: "purchase.amount".to_string(),
        op: CompareOp::Gt,
        value: json!(100),
    };

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    if let Some(Expr::Compare { field, op, value }) = result {
        assert_eq!(field, "amount");
        assert_eq!(op, CompareOp::Gt);
        assert_eq!(value, json!(100));
    } else {
        panic!("Expected Compare expression");
    }
}

#[test]
fn transform_where_clause_simple_compare_non_matching_event() {
    let expr = Expr::Compare {
        field: "purchase.amount".to_string(),
        op: CompareOp::Gt,
        value: json!(100),
    };

    let result = transform_where_clause_for_event_type(&expr, "page_view");
    assert_eq!(result, None);
}

#[test]
fn transform_where_clause_common_field() {
    let expr = Expr::Compare {
        field: "timestamp".to_string(),
        op: CompareOp::Gt,
        value: json!(1000),
    };

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    if let Some(Expr::Compare { field, op, value }) = result {
        assert_eq!(field, "timestamp");
        assert_eq!(op, CompareOp::Gt);
        assert_eq!(value, json!(1000));
    } else {
        panic!("Expected Compare expression");
    }
}

#[test]
fn transform_where_clause_and_both_matching() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "purchase.status".to_string(),
            op: CompareOp::Eq,
            value: json!("completed"),
        }),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    if let Some(Expr::And(left, right)) = result {
        if let Expr::Compare { field: field1, .. } = *left {
            assert_eq!(field1, "amount");
        } else {
            panic!("Expected Compare in left");
        }
        if let Expr::Compare { field: field2, .. } = *right {
            assert_eq!(field2, "status");
        } else {
            panic!("Expected Compare in right");
        }
    } else {
        panic!("Expected And expression");
    }
}

#[test]
fn transform_where_clause_and_one_matching() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "page_view.page".to_string(),
            op: CompareOp::Eq,
            value: json!("/checkout"),
        }),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    // Should return only the matching condition
    if let Some(Expr::Compare { field, .. }) = result {
        assert_eq!(field, "amount");
    } else {
        panic!("Expected single Compare expression");
    }
}

#[test]
fn transform_where_clause_and_none_matching() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "page_view.page".to_string(),
            op: CompareOp::Eq,
            value: json!("/checkout"),
        }),
        Box::new(Expr::Compare {
            field: "page_view.user_id".to_string(),
            op: CompareOp::Eq,
            value: json!("u1"),
        }),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert_eq!(result, None);
}

#[test]
fn transform_where_clause_or_both_matching() {
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "purchase.status".to_string(),
            op: CompareOp::Eq,
            value: json!("completed"),
        }),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    if let Some(Expr::Or(left, right)) = result {
        if let Expr::Compare { field: field1, .. } = *left {
            assert_eq!(field1, "amount");
        } else {
            panic!("Expected Compare in left");
        }
        if let Expr::Compare { field: field2, .. } = *right {
            assert_eq!(field2, "status");
        } else {
            panic!("Expected Compare in right");
        }
    } else {
        panic!("Expected Or expression");
    }
}

#[test]
fn transform_where_clause_or_one_matching_removes_entire_or() {
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "page_view.page".to_string(),
            op: CompareOp::Eq,
            value: json!("/checkout"),
        }),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    // OR with one side removed: keep the remaining side (A OR nothing = A)
    // So we get: purchase.amount > 100 OR purchase.status = "completed" (simplified)
    assert!(result.is_some());
    // Result should be the matching side: purchase.amount > 100
    if let Some(Expr::Compare { field, .. }) = result {
        assert_eq!(field, "amount");
    } else {
        panic!("Expected single Compare expression");
    }
}

#[test]
fn transform_where_clause_not_matching() {
    let expr = Expr::Not(Box::new(Expr::Compare {
        field: "purchase.amount".to_string(),
        op: CompareOp::Lt,
        value: json!(50),
    }));

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    if let Some(Expr::Not(inner)) = result {
        if let Expr::Compare { field, .. } = *inner {
            assert_eq!(field, "amount");
        } else {
            panic!("Expected Compare inside Not");
        }
    } else {
        panic!("Expected Not expression");
    }
}

#[test]
fn transform_where_clause_not_non_matching() {
    let expr = Expr::Not(Box::new(Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    }));

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert_eq!(result, None);
}

#[test]
fn transform_where_clause_nested_and_or() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Or(
            Box::new(Expr::Compare {
                field: "purchase.status".to_string(),
                op: CompareOp::Eq,
                value: json!("completed"),
            }),
            Box::new(Expr::Compare {
                field: "page_view.page".to_string(),
                op: CompareOp::Eq,
                value: json!("/checkout"),
            }),
        )),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    // AND(purchase.amount > 100, OR(purchase.status = "completed", page_view.page = "/checkout"))
    // Transforms to: AND(amount > 100, status = "completed") because OR keeps the matching side
    assert!(result.is_some());
    match result {
        Some(Expr::And(left, right)) => {
            // Both sides should be Compare expressions
            if let (Expr::Compare { field: field_left, .. }, Expr::Compare { field: field_right, .. }) = (*left, *right) {
                assert!(field_left == "amount" || field_right == "amount", "Should have amount field");
                assert!(field_left == "status" || field_right == "status", "Should have status field");
    } else {
                panic!("Expected both sides to be Compare expressions");
            }
        }
        _ => panic!("Expected AND expression with both conditions"),
    }
}

#[test]
fn transform_where_clause_complex_nested() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "purchase.amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "purchase.status".to_string(),
                op: CompareOp::Eq,
                value: json!("completed"),
            }),
            Box::new(Expr::Compare {
                field: "timestamp".to_string(), // Common field
                op: CompareOp::Gt,
                value: json!(1000),
            }),
        )),
    );

    let result = transform_where_clause_for_event_type(&expr, "purchase");
    assert!(result.is_some());
    // Should preserve all three conditions
    if let Some(Expr::And(left, right)) = result {
        if let Expr::Compare { field: field1, .. } = *left {
            assert_eq!(field1, "amount");
        } else {
            panic!("Expected Compare in left");
        }
        if let Some(Expr::And(inner_left, inner_right)) = Some(*right) {
            if let Expr::Compare { field: field2, .. } = *inner_left {
                assert_eq!(field2, "status");
            } else {
                panic!("Expected Compare in inner left");
            }
            if let Expr::Compare { field: field3, .. } = *inner_right {
                assert_eq!(field3, "timestamp");
            } else {
                panic!("Expected Compare in inner right");
            }
        } else {
            panic!("Expected And in right");
        }
    } else {
        panic!("Expected And expression");
    }
}

