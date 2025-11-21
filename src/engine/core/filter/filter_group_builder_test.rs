use crate::command::types::{CompareOp, Expr};
use crate::engine::core::filter::filter_group::{FilterGroup, FilterPriority};
use crate::engine::core::filter::filter_group_builder::FilterGroupBuilder;
use crate::engine::core::filter::in_expansion::InExpansion;
use crate::engine::types::ScalarValue;
use crate::test_helpers::factories::{CommandFactory, SchemaRegistryFactory};
use serde_json::json;

/// Helper function to extract all individual filters from a FilterGroup tree
fn extract_all_filters(group: &FilterGroup) -> Vec<&FilterGroup> {
    match group {
        FilterGroup::Filter { .. } => vec![group],
        FilterGroup::And(children) | FilterGroup::Or(children) => {
            children.iter().flat_map(extract_all_filters).collect()
        }
        FilterGroup::Not(child) => extract_all_filters(child),
    }
}

/// Helper function to count filters in a FilterGroup tree
#[allow(dead_code)]
fn count_filters(group: &FilterGroup) -> usize {
    match group {
        FilterGroup::Filter { .. } => 1,
        FilterGroup::And(children) | FilterGroup::Or(children) => {
            children.iter().map(count_filters).sum()
        }
        FilterGroup::Not(child) => count_filters(child),
    }
}

// ============================================================================
// FILTER PRIORITY TESTS
// ============================================================================

#[test]
fn filter_priority_constants_are_correct() {
    assert_eq!(FilterPriority::EVENT_TYPE, 0);
    assert_eq!(FilterPriority::CONTEXT_ID, 0);
    assert_eq!(FilterPriority::TIMESTAMP, 1);
    assert_eq!(FilterPriority::DEFAULT, 2);
    assert_eq!(FilterPriority::FALLBACK, 3);
}

#[test]
fn filter_priority_for_field_calculates_correctly() {
    assert_eq!(
        FilterPriority::for_field("event_type"),
        FilterPriority::EVENT_TYPE
    );
    assert_eq!(
        FilterPriority::for_field("context_id"),
        FilterPriority::CONTEXT_ID
    );
    assert_eq!(
        FilterPriority::for_field("timestamp"),
        FilterPriority::TIMESTAMP
    );
    assert_eq!(
        FilterPriority::for_field("custom_field"),
        FilterPriority::DEFAULT
    );
    assert_eq!(
        FilterPriority::for_field("another_field"),
        FilterPriority::DEFAULT
    );
}

// ============================================================================
// FILTERGROUP FACTORY METHOD TESTS
// ============================================================================

#[test]
fn filter_group_new_filter_uses_correct_priority() {
    let filter = FilterGroup::new_filter(
        "timestamp".to_string(),
        Some(CompareOp::Eq),
        Some(ScalarValue::from(json!(1000))),
        None,
    );

    match filter {
        FilterGroup::Filter {
            priority, column, ..
        } => {
            assert_eq!(priority, FilterPriority::TIMESTAMP);
            assert_eq!(column, "timestamp");
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn filter_group_new_equality_filter_creates_eq_filter() {
    let filter = FilterGroup::new_equality_filter(
        "id".to_string(),
        ScalarValue::from(json!(42)),
        Some("uid123".to_string()),
    );

    match filter {
        FilterGroup::Filter {
            column,
            operation,
            value,
            uid,
            ..
        } => {
            assert_eq!(column, "id");
            assert_eq!(operation, Some(CompareOp::Eq));
            assert_eq!(value, Some(ScalarValue::from(json!(42))));
            assert_eq!(uid, Some("uid123".to_string()));
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn filter_group_new_equality_filters_creates_multiple() {
    let filters =
        FilterGroup::new_equality_filters("id".to_string(), &[json!(1), json!(2), json!(3)], None);

    assert_eq!(filters.len(), 3);
    for (i, filter) in filters.iter().enumerate() {
        match filter {
            FilterGroup::Filter {
                column,
                operation,
                value,
                ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(operation, &Some(CompareOp::Eq));
                assert_eq!(value, &Some(ScalarValue::from(json!(i + 1))));
            }
            _ => panic!("Expected Filter variant"),
        }
    }
}

// ============================================================================
// IN EXPANSION TESTS
// ============================================================================

#[test]
fn in_expansion_empty_list_returns_none() {
    let result = InExpansion::expand_in("id".to_string(), &[], &None);
    assert!(result.is_none(), "Empty IN list should return None");
}

#[test]
fn in_expansion_single_value_creates_equality() {
    let result =
        InExpansion::expand_in("id".to_string(), &[json!(42)], &Some("uid123".to_string()));

    match result.unwrap() {
        FilterGroup::Filter {
            column,
            operation,
            value,
            uid,
            ..
        } => {
            assert_eq!(column, "id");
            assert_eq!(operation, Some(CompareOp::Eq));
            assert_eq!(value, Some(ScalarValue::from(json!(42))));
            assert_eq!(uid, Some("uid123".to_string()));
        }
        _ => panic!("Expected single Filter, not OR"),
    }
}

#[test]
fn in_expansion_multiple_values_creates_or() {
    let result = InExpansion::expand_in("id".to_string(), &[json!(1), json!(2), json!(3)], &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
            for (i, child) in children.iter().enumerate() {
                match child {
                    FilterGroup::Filter {
                        column,
                        operation,
                        value,
                        ..
                    } => {
                        assert_eq!(column, "id");
                        assert_eq!(operation, &Some(CompareOp::Eq));
                        assert_eq!(value, &Some(ScalarValue::from(json!(i + 1))));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn in_expansion_expand_or_equalities_creates_or() {
    let result =
        InExpansion::expand_or_equalities("id".to_string(), vec![json!(1), json!(2)], &None);

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn in_expansion_preserves_priority_for_timestamp() {
    let result =
        InExpansion::expand_in("timestamp".to_string(), &[json!(1000), json!(2000)], &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            for child in children {
                match child {
                    FilterGroup::Filter { priority, .. } => {
                        assert_eq!(priority, FilterPriority::TIMESTAMP);
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - BASIC
// ============================================================================

#[test]
fn build_compare_expression_creates_single_filter() {
    let expr = Expr::Compare {
        field: "status".to_string(),
        op: CompareOp::Eq,
        value: json!("active"),
    };

    let result = FilterGroupBuilder::build(&expr, &Some("uid123".to_string()));

    assert!(result.is_some());
    match result.unwrap() {
        FilterGroup::Filter {
            column,
            operation,
            value,
            priority,
            uid,
            ..
        } => {
            assert_eq!(column, "status");
            assert_eq!(operation, Some(CompareOp::Eq));
            assert_eq!(value, Some(ScalarValue::from(json!("active"))));
            assert_eq!(priority, FilterPriority::DEFAULT);
            assert_eq!(uid, Some("uid123".to_string()));
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_all_operators() {
    let operators = vec![
        CompareOp::Eq,
        CompareOp::Neq,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::Lt,
        CompareOp::Lte,
    ];

    for op in operators {
        let expr = Expr::Compare {
            field: "amount".to_string(),
            op: op.clone(),
            value: json!(100),
        };

        let result = FilterGroupBuilder::build(&expr, &None);
        assert!(result.is_some());
        match result.unwrap() {
            FilterGroup::Filter { operation, .. } => {
                assert_eq!(operation, Some(op));
            }
            _ => panic!("Expected Filter variant"),
        }
    }
}

#[test]
fn build_compare_with_timestamp_field_has_priority_1() {
    let expr = Expr::Compare {
        field: "timestamp".to_string(),
        op: CompareOp::Gte,
        value: json!(1000),
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Filter { priority, .. } => {
            assert_eq!(priority, FilterPriority::TIMESTAMP);
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_event_type_field_has_priority_0() {
    let expr = Expr::Compare {
        field: "event_type".to_string(),
        op: CompareOp::Eq,
        value: json!("test_event"),
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Filter { priority, .. } => {
            assert_eq!(priority, FilterPriority::EVENT_TYPE);
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_context_id_field_has_priority_0() {
    let expr = Expr::Compare {
        field: "context_id".to_string(),
        op: CompareOp::Eq,
        value: json!("ctx1"),
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Filter { priority, .. } => {
            assert_eq!(priority, FilterPriority::CONTEXT_ID);
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[tokio::test]
async fn build_all_skips_context_filter_when_none() {
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    registry_factory
        .define_with_fields("evt", &[("field", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query().with_event_type("evt").create();
    let filters = FilterGroupBuilder::build_all(&command, &registry).await;

    let has_context = filters.iter().any(|f| match f {
        FilterGroup::Filter { column, .. } if column == "context_id" => true,
        _ => false,
    });
    assert!(
        !has_context,
        "context_id filter should not be created when command lacks a context_id"
    );
}

#[tokio::test]
async fn build_all_skips_event_type_filter_for_wildcard() {
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    registry_factory
        .define_with_fields("login", &[("device", "string")])
        .await
        .unwrap();

    let command = CommandFactory::query()
        .with_event_type("*")
        .with_context_id("alice")
        .create();
    let filters = FilterGroupBuilder::build_all(&command, &registry).await;

    let has_event_type = filters.iter().any(|f| match f {
        FilterGroup::Filter { column, .. } if column == "event_type" => true,
        _ => false,
    });
    assert!(
        !has_event_type,
        "wildcard event_type should not produce an equality filter"
    );
}

#[test]
fn build_compare_with_numeric_value() {
    let expr = Expr::Compare {
        field: "amount".to_string(),
        op: CompareOp::Gte,
        value: json!(100),
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    match result {
        FilterGroup::Filter { value, .. } => {
            assert_eq!(value, Some(ScalarValue::from(json!(100))));
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_float_value() {
    let expr = Expr::Compare {
        field: "price".to_string(),
        op: CompareOp::Lt,
        value: json!(99.99),
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    match result {
        FilterGroup::Filter { value, .. } => {
            assert!(value.is_some());
            // Verify it's a float value
            if let Some(ScalarValue::Float64(f)) = value {
                assert!((f - 99.99).abs() < 0.001);
            } else {
                panic!("Expected Float64 value");
            }
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_boolean_value() {
    let expr = Expr::Compare {
        field: "is_active".to_string(),
        op: CompareOp::Eq,
        value: json!(true),
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    match result {
        FilterGroup::Filter { value, .. } => {
            assert_eq!(value, Some(ScalarValue::Boolean(true)));
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_compare_with_null_value() {
    let expr = Expr::Compare {
        field: "optional_field".to_string(),
        op: CompareOp::Eq,
        value: json!(null),
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    match result {
        FilterGroup::Filter { value, .. } => {
            assert_eq!(value, Some(ScalarValue::Null));
        }
        _ => panic!("Expected Filter variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - IN EXPRESSIONS
// ============================================================================

#[test]
fn build_in_with_empty_list_returns_none() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![],
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    assert!(result.is_none(), "Empty IN list should return None");
}

#[test]
fn build_in_with_single_value_expands_to_equality_filter() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(42)],
    };

    let result = FilterGroupBuilder::build(&expr, &Some("uid123".to_string()));

    assert!(result.is_some());
    match result.unwrap() {
        FilterGroup::Filter {
            column,
            operation,
            value,
            priority,
            uid,
            ..
        } => {
            assert_eq!(column, "id");
            assert_eq!(operation, Some(CompareOp::Eq)); // Expanded to equality
            assert_eq!(value, Some(ScalarValue::from(json!(42))));
            assert_eq!(priority, FilterPriority::DEFAULT);
            assert_eq!(uid, Some("uid123".to_string()));
        }
        _ => panic!("Expected single Filter variant, not OR"),
    }
}

#[test]
fn build_in_with_multiple_values_expands_to_or_of_equality_filters() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    };

    let result = FilterGroupBuilder::build(&expr, &Some("uid123".to_string()));

    assert!(result.is_some());
    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3, "Should have 3 equality filters");
            for (i, child) in children.iter().enumerate() {
                match child {
                    FilterGroup::Filter {
                        column,
                        operation,
                        value,
                        priority,
                        uid,
                        ..
                    } => {
                        assert_eq!(column, "id");
                        assert_eq!(operation, &Some(CompareOp::Eq));
                        assert_eq!(value, &Some(ScalarValue::from(json!(i + 1))));
                        assert_eq!(*priority, FilterPriority::DEFAULT);
                        assert_eq!(uid, &Some("uid123".to_string()));
                    }
                    _ => panic!("Expected Filter variant in OR children"),
                }
            }
        }
        _ => panic!("Expected OR variant with multiple equality filters"),
    }
}

#[test]
fn build_in_with_string_values_expands_correctly() {
    let expr = Expr::In {
        field: "status".to_string(),
        values: vec![json!("active"), json!("pending"), json!("completed")],
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
            let values: Vec<&ScalarValue> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter { value, .. } => value.as_ref(),
                    _ => None,
                })
                .collect();
            assert_eq!(values[0], &ScalarValue::from(json!("active")));
            assert_eq!(values[1], &ScalarValue::from(json!("pending")));
            assert_eq!(values[2], &ScalarValue::from(json!("completed")));
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_timestamp_field_has_priority_1() {
    let expr = Expr::In {
        field: "timestamp".to_string(),
        values: vec![json!(1000), json!(2000)],
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            for child in children {
                match child {
                    FilterGroup::Filter { priority, .. } => {
                        assert_eq!(priority, FilterPriority::TIMESTAMP);
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_duplicate_values_still_expands_all() {
    // Even with duplicates, expansion should create all filters
    // Deduplication happens later in extract_unique_filters
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(1)], // Duplicate
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(
                children.len(),
                3,
                "Should expand all values including duplicates"
            );
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_mixed_value_types() {
    // IN with mixed types should still expand (though condition evaluator will handle type checking)
    let expr = Expr::In {
        field: "value".to_string(),
        values: vec![json!(1), json!("string"), json!(3.14)],
    };

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_null_value() {
    let expr = Expr::In {
        field: "optional_field".to_string(),
        values: vec![json!(null), json!("value")],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // Check that null value is preserved
            let has_null = children.iter().any(|c| {
                matches!(
                    c,
                    FilterGroup::Filter {
                        value: Some(ScalarValue::Null),
                        ..
                    }
                )
            });
            assert!(has_null, "Should preserve null value");
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_very_large_list() {
    // Test with 100 values to ensure it scales
    let values: Vec<serde_json::Value> = (1..=100).map(|i| json!(i)).collect();
    let expr = Expr::In {
        field: "id".to_string(),
        values,
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 100);
            for (i, child) in children.iter().enumerate() {
                match child {
                    FilterGroup::Filter {
                        column,
                        operation,
                        value,
                        ..
                    } => {
                        assert_eq!(column, "id");
                        assert_eq!(operation, &Some(CompareOp::Eq));
                        assert_eq!(value, &Some(ScalarValue::from(json!(i + 1))));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - OR EXPRESSIONS
// ============================================================================

#[test]
fn build_or_with_same_field_equality_expands_to_or_of_equality_filters() {
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Eq,
            value: json!(1),
        }),
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Eq,
            value: json!(2),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            let values: Vec<i64> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter {
                        operation, value, ..
                    } if operation == &Some(CompareOp::Eq) => value.as_ref()?.as_i64(),
                    _ => None,
                })
                .collect();
            assert_eq!(values, vec![1, 2]);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_different_fields_preserves_structure() {
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // Should preserve structure, not expand
            match &children[0] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "status");
                }
                _ => panic!("Expected Filter variant"),
            }
            match &children[1] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "amount");
                }
                _ => panic!("Expected Filter variant"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_nested_or_expands_correctly() {
    // (id = 1 OR id = 2) OR id = 3
    let expr = Expr::Or(
        Box::new(Expr::Or(
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(1),
            }),
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(2),
            }),
        )),
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Eq,
            value: json!(3),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3, "Should expand to 3 equality filters");
            let values: Vec<i64> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter { value, .. } => value.as_ref()?.as_i64(),
                    _ => None,
                })
                .collect();
            assert_eq!(values, vec![1, 2, 3]);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_nested_or_different_fields_preserves_structure() {
    // (id = 1 OR status = "active") OR amount > 100
    let expr = Expr::Or(
        Box::new(Expr::Or(
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(1),
            }),
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
        )),
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // First child should be OR (not expanded because fields differ)
            match &children[0] {
                FilterGroup::Or(_) => {}
                _ => panic!("Expected nested OR to be preserved"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_non_equality_operations_preserves_structure() {
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Gt,
            value: json!(100),
        }),
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Lt,
            value: json!(50),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // Should preserve structure since operations are not equality
            match &children[0] {
                FilterGroup::Filter { operation, .. } => {
                    assert_eq!(operation, &Some(CompareOp::Gt));
                }
                _ => panic!("Expected Filter variant"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_neq_operations_preserves_structure() {
    // OR with Neq operations should not expand
    let expr = Expr::Or(
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Neq,
            value: json!("inactive"),
        }),
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Neq,
            value: json!("pending"),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // Should preserve structure
            for child in children {
                match child {
                    FilterGroup::Filter { operation, .. } => {
                        assert_eq!(operation, Some(CompareOp::Neq));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - AND EXPRESSIONS
// ============================================================================

#[test]
fn build_and_preserves_structure() {
    let expr = Expr::And(
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Gte,
            value: json!(100),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::And(children) => {
            assert_eq!(children.len(), 2);
            match &children[0] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "status");
                }
                _ => panic!("Expected Filter variant"),
            }
            match &children[1] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "amount");
                }
                _ => panic!("Expected Filter variant"),
            }
        }
        _ => panic!("Expected AND variant"),
    }
}

#[test]
fn build_and_with_nested_and() {
    // (A AND B) AND C
    let expr = Expr::And(
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
            Box::new(Expr::Compare {
                field: "amount".to_string(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
        )),
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Eq,
            value: json!(42),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::And(children) => {
            assert_eq!(children.len(), 2);
            // First child should be AND
            match &children[0] {
                FilterGroup::And(and_children) => {
                    assert_eq!(and_children.len(), 2);
                }
                _ => panic!("Expected nested AND"),
            }
        }
        _ => panic!("Expected AND variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - NOT EXPRESSIONS
// ============================================================================

#[test]
fn build_not_expression_preserves_structure() {
    let expr = Expr::Not(Box::new(Expr::Compare {
        field: "status".to_string(),
        op: CompareOp::Eq,
        value: json!("active"),
    }));

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Not(child) => match child.as_ref() {
            FilterGroup::Filter { column, .. } => {
                assert_eq!(column, "status");
            }
            _ => panic!("Expected Filter variant inside NOT"),
        },
        _ => panic!("Expected NOT variant"),
    }
}

#[test]
fn build_not_with_in_expands_correctly() {
    // NOT id IN(1,2,3)
    let expr = Expr::Not(Box::new(Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    }));

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Not(child) => match child.as_ref() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 3);
            }
            _ => panic!("Expected OR variant inside NOT"),
        },
        _ => panic!("Expected NOT variant"),
    }
}

#[test]
fn build_not_with_and() {
    // NOT (A AND B)
    let expr = Expr::Not(Box::new(Expr::And(
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
        Box::new(Expr::Compare {
            field: "amount".to_string(),
            op: CompareOp::Gte,
            value: json!(100),
        }),
    )));

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Not(child) => match child.as_ref() {
            FilterGroup::And(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("Expected AND variant inside NOT"),
        },
        _ => panic!("Expected NOT variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - COMPLEX NESTED EXPRESSIONS
// ============================================================================

#[test]
fn build_complex_nested_expression_a_and_b_or_c() {
    // (A AND B) OR C
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
            Box::new(Expr::Compare {
                field: "amount".to_string(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
        )),
        Box::new(Expr::Compare {
            field: "id".to_string(),
            op: CompareOp::Eq,
            value: json!(42),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // First child should be AND
            match &children[0] {
                FilterGroup::And(and_children) => {
                    assert_eq!(and_children.len(), 2);
                }
                _ => panic!("Expected AND variant as first child"),
            }
            // Second child should be Filter
            match &children[1] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "id");
                }
                _ => panic!("Expected Filter variant as second child"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_complex_nested_expression_a_and_b_or_c_and_d() {
    // (A AND B) OR (C AND D)
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
            Box::new(Expr::Compare {
                field: "amount".to_string(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
        )),
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(42),
            }),
            Box::new(Expr::Compare {
                field: "name".to_string(),
                op: CompareOp::Eq,
                value: json!("test"),
            }),
        )),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // Both children should be AND
            for child in children {
                match child {
                    FilterGroup::And(and_children) => {
                        assert_eq!(and_children.len(), 2);
                    }
                    _ => panic!("Expected AND variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_complex_expression_with_in_and_or() {
    // id IN(1,2,3) OR status = "active"
    let expr = Expr::Or(
        Box::new(Expr::In {
            field: "id".to_string(),
            values: vec![json!(1), json!(2), json!(3)],
        }),
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 4); // 3 from IN expansion + 1 from status
            // First 3 should be id equality filters
            for i in 0..3 {
                match &children[i] {
                    FilterGroup::Filter {
                        column, operation, ..
                    } => {
                        assert_eq!(column, "id");
                        assert_eq!(operation, &Some(CompareOp::Eq));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            // Last should be status filter
            match &children[3] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "status");
                }
                _ => panic!("Expected Filter variant"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_complex_expression_a_and_b_or_c_with_in() {
    // (status = "active" AND amount >= 100) OR id IN(1,2,3)
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
            Box::new(Expr::Compare {
                field: "amount".to_string(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
        )),
        Box::new(Expr::In {
            field: "id".to_string(),
            values: vec![json!(1), json!(2), json!(3)],
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 4); // 1 AND + 3 from IN expansion
            // First child should be AND
            match &children[0] {
                FilterGroup::And(and_children) => {
                    assert_eq!(and_children.len(), 2);
                }
                _ => panic!("Expected AND variant"),
            }
            // Remaining 3 should be id equality filters
            for i in 1..4 {
                match &children[i] {
                    FilterGroup::Filter { column, .. } => {
                        assert_eq!(column, "id");
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_deeply_nested_expression() {
    // ((A AND B) OR C) AND D
    let expr = Expr::And(
        Box::new(Expr::Or(
            Box::new(Expr::And(
                Box::new(Expr::Compare {
                    field: "status".to_string(),
                    op: CompareOp::Eq,
                    value: json!("active"),
                }),
                Box::new(Expr::Compare {
                    field: "amount".to_string(),
                    op: CompareOp::Gte,
                    value: json!(100),
                }),
            )),
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(42),
            }),
        )),
        Box::new(Expr::Compare {
            field: "name".to_string(),
            op: CompareOp::Eq,
            value: json!("test"),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);

    match result.unwrap() {
        FilterGroup::And(children) => {
            assert_eq!(children.len(), 2);
            // First child should be OR
            match &children[0] {
                FilterGroup::Or(or_children) => {
                    assert_eq!(or_children.len(), 2);
                    // First OR child should be AND
                    match &or_children[0] {
                        FilterGroup::And(_) => {}
                        _ => panic!("Expected AND variant"),
                    }
                }
                _ => panic!("Expected OR variant"),
            }
            // Second child should be Filter
            match &children[1] {
                FilterGroup::Filter { column, .. } => {
                    assert_eq!(column, "name");
                }
                _ => panic!("Expected Filter variant"),
            }
        }
        _ => panic!("Expected AND variant"),
    }
}

#[test]
fn build_very_deeply_nested_expression() {
    // (((A AND B) OR C) AND D) OR E
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::Or(
                Box::new(Expr::And(
                    Box::new(Expr::Compare {
                        field: "a".to_string(),
                        op: CompareOp::Eq,
                        value: json!(1),
                    }),
                    Box::new(Expr::Compare {
                        field: "b".to_string(),
                        op: CompareOp::Eq,
                        value: json!(2),
                    }),
                )),
                Box::new(Expr::Compare {
                    field: "c".to_string(),
                    op: CompareOp::Eq,
                    value: json!(3),
                }),
            )),
            Box::new(Expr::Compare {
                field: "d".to_string(),
                op: CompareOp::Eq,
                value: json!(4),
            }),
        )),
        Box::new(Expr::Compare {
            field: "e".to_string(),
            op: CompareOp::Eq,
            value: json!(5),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None);
    assert!(result.is_some());

    // Verify structure is preserved
    match result.unwrap() {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            // First child should be AND
            match &children[0] {
                FilterGroup::And(_) => {}
                _ => panic!("Expected AND variant"),
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - EXTRACTION METHODS
// ============================================================================

#[test]
fn build_extract_individual_filters_from_in_expansion() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    let individual_filters = result.extract_individual_filters();

    assert_eq!(individual_filters.len(), 3);
    for filter in individual_filters {
        match filter {
            FilterGroup::Filter {
                column, operation, ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(operation, Some(CompareOp::Eq));
            }
            _ => panic!("Expected Filter variant"),
        }
    }
}

#[test]
fn build_extract_unique_filters_deduplicates() {
    // id IN(1,2,1) should deduplicate to 2 filters
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(1)], // Duplicate value
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    let unique_filters = result.extract_unique_filters();

    // Should have 2 unique filters (id=1 and id=2)
    assert_eq!(unique_filters.len(), 2);

    // Verify the values
    let values: Vec<i64> = unique_filters
        .iter()
        .filter_map(|f| match f {
            FilterGroup::Filter { value, .. } => value.as_ref()?.as_i64(),
            _ => None,
        })
        .collect();
    assert_eq!(values.len(), 2);
    assert!(values.contains(&1));
    assert!(values.contains(&2));
}

#[test]
fn build_extract_unique_filters_from_complex_tree() {
    // (A AND B) OR (A AND C) - A appears twice
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
            Box::new(Expr::Compare {
                field: "amount".to_string(),
                op: CompareOp::Gte,
                value: json!(100),
            }),
        )),
        Box::new(Expr::And(
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"), // Same as first
            }),
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(42),
            }),
        )),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();
    let unique_filters = result.extract_unique_filters();

    // Should have 3 unique filters: status="active", amount>=100, id=42
    assert_eq!(unique_filters.len(), 3);
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - UID HANDLING
// ============================================================================

#[test]
fn build_preserves_uid_throughout_tree() {
    let uid = Some("test_uid".to_string());
    let expr = Expr::And(
        Box::new(Expr::In {
            field: "id".to_string(),
            values: vec![json!(1), json!(2)],
        }),
        Box::new(Expr::Compare {
            field: "status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &uid).unwrap();
    let all_filters = extract_all_filters(&result);

    for filter in all_filters {
        match filter {
            FilterGroup::Filter {
                uid: filter_uid, ..
            } => {
                assert_eq!(filter_uid, &uid);
            }
            _ => {}
        }
    }
}

#[test]
fn build_handles_none_uid() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    let all_filters = extract_all_filters(&result);
    for filter in all_filters {
        match filter {
            FilterGroup::Filter { uid, .. } => {
                assert_eq!(uid, &None);
            }
            _ => {}
        }
    }
}

// ============================================================================
// FILTER GROUP BUILDER TESTS - EDGE CASES
// ============================================================================

#[test]
fn build_in_with_single_null_value() {
    let expr = Expr::In {
        field: "optional_field".to_string(),
        values: vec![json!(null)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Filter { value, .. } => {
            assert_eq!(value, Some(ScalarValue::Null));
        }
        _ => panic!("Expected single Filter for single value"),
    }
}

#[test]
fn build_in_with_all_null_values() {
    let expr = Expr::In {
        field: "optional_field".to_string(),
        values: vec![json!(null), json!(null)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
            for child in children {
                match child {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, Some(ScalarValue::Null));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_or_with_many_equalities_expands() {
    // id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5
    let expr = Expr::Or(
        Box::new(Expr::Or(
            Box::new(Expr::Or(
                Box::new(Expr::Compare {
                    field: "id".to_string(),
                    op: CompareOp::Eq,
                    value: json!(1),
                }),
                Box::new(Expr::Compare {
                    field: "id".to_string(),
                    op: CompareOp::Eq,
                    value: json!(2),
                }),
            )),
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(3),
            }),
        )),
        Box::new(Expr::Or(
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(4),
            }),
            Box::new(Expr::Compare {
                field: "id".to_string(),
                op: CompareOp::Eq,
                value: json!(5),
            }),
        )),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 5, "Should expand all nested ORs");
            let values: Vec<i64> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter { value, .. } => value.as_ref()?.as_i64(),
                    _ => None,
                })
                .collect();
            assert_eq!(values, vec![1, 2, 3, 4, 5]);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_complex_with_in_nested_in_and() {
    // (id IN(1,2) AND status = "active") OR id IN(3,4)
    let expr = Expr::Or(
        Box::new(Expr::And(
            Box::new(Expr::In {
                field: "id".to_string(),
                values: vec![json!(1), json!(2)],
            }),
            Box::new(Expr::Compare {
                field: "status".to_string(),
                op: CompareOp::Eq,
                value: json!("active"),
            }),
        )),
        Box::new(Expr::In {
            field: "id".to_string(),
            values: vec![json!(3), json!(4)],
        }),
    );

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            // Should have: 1 AND + 2 from second IN = 3 total
            // The first IN is nested inside AND, so it stays as OR(id=1, id=2) inside the AND
            assert_eq!(children.len(), 3);

            // First child should be AND
            match &children[0] {
                FilterGroup::And(and_children) => {
                    assert_eq!(and_children.len(), 2);
                    // First child of AND should be OR(id=1, id=2)
                    match &and_children[0] {
                        FilterGroup::Or(or_children) => {
                            assert_eq!(or_children.len(), 2);
                        }
                        _ => panic!("Expected OR variant for nested IN"),
                    }
                }
                _ => panic!("Expected AND variant"),
            }

            // Last 2 should be id=3 and id=4 (flattened from second IN)
            for i in 1..3 {
                match &children[i] {
                    FilterGroup::Filter { column, .. } => {
                        assert_eq!(column, "id");
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_handles_very_long_field_names() {
    let long_field = "a".repeat(100);
    let expr = Expr::Compare {
        field: long_field.clone(),
        op: CompareOp::Eq,
        value: json!("value"),
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Filter { column, .. } => {
            assert_eq!(column, long_field);
        }
        _ => panic!("Expected Filter variant"),
    }
}

#[test]
fn build_handles_special_characters_in_values() {
    let expr = Expr::In {
        field: "name".to_string(),
        values: vec![
            json!("test@example.com"),
            json!("user-name"),
            json!("value with spaces"),
            json!("value\"with\"quotes"),
        ],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 4);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_negative_numbers() {
    let expr = Expr::In {
        field: "temperature".to_string(),
        values: vec![json!(-10), json!(0), json!(10)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
            let values: Vec<i64> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter { value, .. } => value.as_ref()?.as_i64(),
                    _ => None,
                })
                .collect();
            assert_eq!(values, vec![-10, 0, 10]);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_very_large_numbers() {
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(i64::MAX), json!(i64::MIN)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 2);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_in_with_float_precision() {
    let expr = Expr::In {
        field: "price".to_string(),
        values: vec![json!(0.1), json!(0.2), json!(0.3)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
        }
        _ => panic!("Expected OR variant"),
    }
}

#[test]
fn build_preserves_order_of_in_values() {
    // Verify that IN expansion preserves the order of values
    let expr = Expr::In {
        field: "id".to_string(),
        values: vec![json!(3), json!(1), json!(2)],
    };

    let result = FilterGroupBuilder::build(&expr, &None).unwrap();

    match result {
        FilterGroup::Or(children) => {
            assert_eq!(children.len(), 3);
            let values: Vec<i64> = children
                .iter()
                .filter_map(|c| match c {
                    FilterGroup::Filter { value, .. } => value.as_ref()?.as_i64(),
                    _ => None,
                })
                .collect();
            // Order should be preserved
            assert_eq!(values, vec![3, 1, 2]);
        }
        _ => panic!("Expected OR variant"),
    }
}
