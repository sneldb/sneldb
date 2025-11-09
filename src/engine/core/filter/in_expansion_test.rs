use crate::command::types::CompareOp;
use crate::engine::core::filter::filter_group::{FilterGroup, FilterPriority};
use crate::engine::core::filter::in_expansion::InExpansion;
use crate::engine::types::ScalarValue;
use serde_json::json;

#[cfg(test)]
mod in_expansion_tests {
    use super::*;

    // ─────────────────────────────
    // expand_in Tests
    // ─────────────────────────────

    #[test]
    fn test_expand_in_empty_list_returns_none() {
        let result = InExpansion::expand_in("status".to_string(), &[], &None);
        assert!(result.is_none(), "Empty IN list should return None");
    }

    #[test]
    fn test_expand_in_empty_list_with_uid_returns_none() {
        let result = InExpansion::expand_in(
            "status".to_string(),
            &[],
            &Some("uid123".to_string()),
        );
        assert!(result.is_none(), "Empty IN list should return None even with UID");
    }

    #[test]
    fn test_expand_in_single_value_creates_equality_filter() {
        let result = InExpansion::expand_in(
            "status".to_string(),
            &[json!("active")],
            &None,
        );

        assert!(result.is_some());
        match result.unwrap() {
            FilterGroup::Filter {
                column,
                operation,
                value,
                uid,
                ..
            } => {
                assert_eq!(column, "status");
                assert_eq!(operation, Some(CompareOp::Eq));
                assert_eq!(value, Some(ScalarValue::Utf8("active".to_string())));
                assert_eq!(uid, None);
            }
            _ => panic!("Expected single Filter variant, not OR"),
        }
    }

    #[test]
    fn test_expand_in_single_value_with_uid() {
        let result = InExpansion::expand_in(
            "status".to_string(),
            &[json!("active")],
            &Some("uid456".to_string()),
        );

        match result.unwrap() {
            FilterGroup::Filter { uid, .. } => {
                assert_eq!(uid, Some("uid456".to_string()));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_single_int_value() {
        let result = InExpansion::expand_in("id".to_string(), &[json!(42)], &None);

        match result.unwrap() {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, Some(ScalarValue::Int64(42)));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_single_float_value() {
        let result = InExpansion::expand_in("price".to_string(), &[json!(3.14)], &None);

        match result.unwrap() {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, Some(ScalarValue::Float64(3.14)));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_single_bool_value() {
        let result = InExpansion::expand_in("active".to_string(), &[json!(true)], &None);

        match result.unwrap() {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, Some(ScalarValue::Boolean(true)));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_single_null_value() {
        let result = InExpansion::expand_in("field".to_string(), &[json!(null)], &None);

        match result.unwrap() {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, Some(ScalarValue::Null));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_multiple_values_creates_or_group() {
        let result = InExpansion::expand_in(
            "status".to_string(),
            &[json!("active"), json!("pending"), json!("completed")],
            &None,
        );

        assert!(result.is_some());
        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 3);
                for child in &children {
                    assert!(matches!(child, FilterGroup::Filter { .. }));
                }
            }
            _ => panic!("Expected OR variant for multiple values"),
        }
    }

    #[test]
    fn test_expand_in_multiple_values_creates_correct_filters() {
        let result = InExpansion::expand_in(
            "id".to_string(),
            &[json!(1), json!(2), json!(3)],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
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
                            assert_eq!(value, &Some(ScalarValue::Int64((i + 1) as i64)));
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_multiple_values_with_uid() {
        let result = InExpansion::expand_in(
            "status".to_string(),
            &[json!("active"), json!("pending")],
            &Some("uid789".to_string()),
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { uid, .. } => {
                            assert_eq!(uid, &Some("uid789".to_string()));
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_mixed_value_types() {
        let result = InExpansion::expand_in(
            "value".to_string(),
            &[json!(42), json!("text"), json!(3.14), json!(true)],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 4);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Int64(42)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[1] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Utf8("text".to_string())));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[2] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Float64(3.14)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[3] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Boolean(true)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_large_list() {
        let values: Vec<_> = (1..=100).map(|i| json!(i)).collect();
        let result = InExpansion::expand_in("id".to_string(), &values, &None);

        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 100);
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_preserves_priority_for_event_type() {
        let result = InExpansion::expand_in(
            "event_type".to_string(),
            &[json!("order_created"), json!("order_updated")],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { priority, .. } => {
                            assert_eq!(*priority, FilterPriority::EVENT_TYPE);
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_preserves_priority_for_context_id() {
        let result = InExpansion::expand_in(
            "context_id".to_string(),
            &[json!("user-1"), json!("user-2")],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { priority, .. } => {
                            assert_eq!(*priority, FilterPriority::CONTEXT_ID);
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_preserves_priority_for_timestamp() {
        let result = InExpansion::expand_in(
            "timestamp".to_string(),
            &[json!(1000), json!(2000)],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { priority, .. } => {
                            assert_eq!(*priority, FilterPriority::TIMESTAMP);
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_single_value_preserves_priority() {
        let result = InExpansion::expand_in(
            "timestamp".to_string(),
            &[json!(1000)],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Filter { priority, .. } => {
                assert_eq!(priority, FilterPriority::TIMESTAMP);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_expand_in_negative_numbers() {
        let result = InExpansion::expand_in(
            "balance".to_string(),
            &[json!(-100), json!(-50), json!(0)],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 3);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Int64(-100)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_unicode_strings() {
        let result = InExpansion::expand_in(
            "name".to_string(),
            &[json!("café"), json!("naïve")],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 2);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Utf8("café".to_string())));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_in_empty_strings() {
        let result = InExpansion::expand_in(
            "description".to_string(),
            &[json!(""), json!("non-empty")],
            &None,
        );

        match result.unwrap() {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 2);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Utf8("".to_string())));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    // ─────────────────────────────
    // expand_or_equalities Tests
    // ─────────────────────────────

    #[test]
    fn test_expand_or_equalities_empty_list() {
        let result = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 0);
            }
            _ => panic!("Expected OR variant even for empty list"),
        }
    }

    #[test]
    fn test_expand_or_equalities_single_value() {
        let result = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![json!("active")],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 1);
                match &children[0] {
                    FilterGroup::Filter {
                        column,
                        operation,
                        value,
                        ..
                    } => {
                        assert_eq!(column, "status");
                        assert_eq!(operation, &Some(CompareOp::Eq));
                        assert_eq!(value, &Some(ScalarValue::Utf8("active".to_string())));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_multiple_values() {
        let result = InExpansion::expand_or_equalities(
            "id".to_string(),
            vec![json!(1), json!(2), json!(3)],
            &None,
        );

        match result {
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
                            assert_eq!(value, &Some(ScalarValue::Int64((i + 1) as i64)));
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_with_uid() {
        let result = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![json!("active"), json!("pending")],
            &Some("uid999".to_string()),
        );

        match result {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { uid, .. } => {
                            assert_eq!(uid, &Some("uid999".to_string()));
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_mixed_types() {
        let result = InExpansion::expand_or_equalities(
            "value".to_string(),
            vec![json!(42), json!("text"), json!(false)],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 3);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Int64(42)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[1] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Utf8("text".to_string())));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[2] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Boolean(false)));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_preserves_priority() {
        let result = InExpansion::expand_or_equalities(
            "event_type".to_string(),
            vec![json!("order_created"), json!("order_updated")],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                for child in &children {
                    match child {
                        FilterGroup::Filter { priority, .. } => {
                            assert_eq!(*priority, FilterPriority::EVENT_TYPE);
                        }
                        _ => panic!("Expected Filter variant"),
                    }
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_large_list() {
        let values: Vec<_> = (1..=50).map(|i| json!(i)).collect();
        let result = InExpansion::expand_or_equalities("id".to_string(), values, &None);

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 50);
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_always_returns_or() {
        // Even for single value, expand_or_equalities returns OR (unlike expand_in)
        let result = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![json!("active")],
            &None,
        );

        assert!(matches!(result, FilterGroup::Or(_)));
    }

    #[test]
    fn test_expand_or_equalities_duplicate_values() {
        let result = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![json!("active"), json!("active"), json!("pending")],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 3); // Duplicates are preserved
            }
            _ => panic!("Expected OR variant"),
        }
    }

    #[test]
    fn test_expand_or_equalities_with_null() {
        let result = InExpansion::expand_or_equalities(
            "field".to_string(),
            vec![json!(null), json!("value")],
            &None,
        );

        match result {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 2);
                match &children[0] {
                    FilterGroup::Filter { value, .. } => {
                        assert_eq!(value, &Some(ScalarValue::Null));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected OR variant"),
        }
    }

    // ─────────────────────────────
    // Comparison Tests
    // ─────────────────────────────

    #[test]
    fn test_expand_in_vs_expand_or_equalities_single_value() {
        // expand_in returns single Filter for single value
        let result_in = InExpansion::expand_in("status".to_string(), &[json!("active")], &None);
        assert!(matches!(result_in.unwrap(), FilterGroup::Filter { .. }));

        // expand_or_equalities returns OR even for single value
        let result_or = InExpansion::expand_or_equalities(
            "status".to_string(),
            vec![json!("active")],
            &None,
        );
        assert!(matches!(result_or, FilterGroup::Or(_)));
    }

    #[test]
    fn test_expand_in_vs_expand_or_equalities_empty_list() {
        // expand_in returns None for empty list
        let result_in = InExpansion::expand_in("status".to_string(), &[], &None);
        assert!(result_in.is_none());

        // expand_or_equalities returns OR with empty children for empty list
        let result_or = InExpansion::expand_or_equalities("status".to_string(), vec![], &None);
        match result_or {
            FilterGroup::Or(children) => {
                assert_eq!(children.len(), 0);
            }
            _ => panic!("Expected OR variant"),
        }
    }
}

