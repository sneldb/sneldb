use crate::command::types::CompareOp;
use crate::engine::core::filter::condition::LogicalOp;
use crate::engine::core::filter::filter_group::{FilterGroup, FilterPriority, filter_key};
use crate::engine::core::read::index_strategy::IndexStrategy;
use crate::engine::types::ScalarValue;
use serde_json::json;

#[cfg(test)]
mod filter_group_tests {
    use super::*;

    // ─────────────────────────────
    // FilterPriority Tests
    // ─────────────────────────────

    #[test]
    fn test_filter_priority_constants() {
        assert_eq!(FilterPriority::EVENT_TYPE, 0);
        assert_eq!(FilterPriority::CONTEXT_ID, 0);
        assert_eq!(FilterPriority::TIMESTAMP, 1);
        assert_eq!(FilterPriority::DEFAULT, 2);
        assert_eq!(FilterPriority::FALLBACK, 3);
    }

    #[test]
    fn test_filter_priority_for_field_event_type() {
        assert_eq!(
            FilterPriority::for_field("event_type"),
            FilterPriority::EVENT_TYPE
        );
    }

    #[test]
    fn test_filter_priority_for_field_context_id() {
        assert_eq!(
            FilterPriority::for_field("context_id"),
            FilterPriority::CONTEXT_ID
        );
    }

    #[test]
    fn test_filter_priority_for_field_timestamp() {
        assert_eq!(
            FilterPriority::for_field("timestamp"),
            FilterPriority::TIMESTAMP
        );
    }

    #[test]
    fn test_filter_priority_for_field_default() {
        assert_eq!(FilterPriority::for_field("status"), FilterPriority::DEFAULT);
        assert_eq!(FilterPriority::for_field("amount"), FilterPriority::DEFAULT);
        assert_eq!(
            FilterPriority::for_field("custom_field"),
            FilterPriority::DEFAULT
        );
    }

    #[test]
    fn test_filter_priority_for_field_case_sensitive() {
        // Should be case-sensitive
        assert_eq!(
            FilterPriority::for_field("Event_Type"),
            FilterPriority::DEFAULT
        );
        assert_eq!(
            FilterPriority::for_field("TIMESTAMP"),
            FilterPriority::DEFAULT
        );
    }

    // ─────────────────────────────
    // filter_key Function Tests
    // ─────────────────────────────

    #[test]
    fn test_filter_key_with_none_operation_and_value() {
        let key = filter_key("status", &None, &None);
        assert_eq!(key, "status:None:None");
    }

    #[test]
    fn test_filter_key_with_all_compare_operations() {
        let value = Some(ScalarValue::Int64(42));
        let ops = vec![
            (CompareOp::Eq, "Eq"),
            (CompareOp::Neq, "Neq"),
            (CompareOp::Gt, "Gt"),
            (CompareOp::Gte, "Gte"),
            (CompareOp::Lt, "Lt"),
            (CompareOp::Lte, "Lte"),
            (CompareOp::In, "In"),
        ];

        for (op, expected_str) in ops {
            let key = filter_key("amount", &Some(op), &value);
            assert_eq!(key, format!("amount:{}:Int64(42)", expected_str));
        }
    }

    #[test]
    fn test_filter_key_with_null_value() {
        let key = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Null));
        assert_eq!(key, "field:Eq:Null");
    }

    #[test]
    fn test_filter_key_with_boolean_values() {
        let key_true = filter_key(
            "active",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Boolean(true)),
        );
        assert_eq!(key_true, "active:Eq:Bool(true)");

        let key_false = filter_key(
            "active",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Boolean(false)),
        );
        assert_eq!(key_false, "active:Eq:Bool(false)");
    }

    #[test]
    fn test_filter_key_with_int64_value() {
        let key = filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::Int64(12345)));
        assert_eq!(key, "id:Eq:Int64(12345)");

        let key_negative = filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::Int64(-42)));
        assert_eq!(key_negative, "id:Eq:Int64(-42)");
    }

    #[test]
    fn test_filter_key_with_float64_value() {
        let key = filter_key(
            "price",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Float64(3.14)),
        );
        assert_eq!(key, "price:Eq:Float64(3.14)");

        let key_negative = filter_key(
            "price",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Float64(-99.99)),
        );
        assert_eq!(key_negative, "price:Eq:Float64(-99.99)");
    }

    #[test]
    fn test_filter_key_with_float64_nan() {
        let key = filter_key(
            "value",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Float64(f64::NAN)),
        );
        assert_eq!(key, "value:Eq:Float64(NaN)");
    }

    #[test]
    fn test_filter_key_with_timestamp_value() {
        let key = filter_key(
            "created_at",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Timestamp(1609459200)),
        );
        assert_eq!(key, "created_at:Eq:Timestamp(1609459200)");
    }

    #[test]
    fn test_filter_key_with_utf8_value() {
        let key = filter_key(
            "name",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Utf8("John".to_string())),
        );
        assert_eq!(key, "name:Eq:Utf8(John)");
    }

    #[test]
    fn test_filter_key_with_utf8_value_special_chars() {
        let key = filter_key(
            "path",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Utf8("/path/to/file".to_string())),
        );
        assert_eq!(key, "path:Eq:Utf8(/path/to/file)");
    }

    #[test]
    fn test_filter_key_with_binary_value() {
        let binary_data = vec![0x01, 0x02, 0x03];
        let key = filter_key(
            "data",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Binary(binary_data)),
        );
        // Binary format includes debug representation
        assert!(key.starts_with("data:Eq:Binary("));
        assert!(key.contains("1, 2, 3"));
    }

    #[test]
    fn test_filter_key_deduplication() {
        // Same filter should produce same key
        let key1 = filter_key(
            "status",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Utf8("active".to_string())),
        );
        let key2 = filter_key(
            "status",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Utf8("active".to_string())),
        );
        assert_eq!(key1, key2);

        // Different values should produce different keys
        let key3 = filter_key(
            "status",
            &Some(CompareOp::Eq),
            &Some(ScalarValue::Utf8("inactive".to_string())),
        );
        assert_ne!(key1, key3);
    }

    // ─────────────────────────────
    // FilterGroup::new_filter Tests
    // ─────────────────────────────

    #[test]
    fn test_new_filter_basic() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        match filter {
            FilterGroup::Filter {
                column,
                operation,
                value,
                priority,
                uid,
                index_strategy,
            } => {
                assert_eq!(column, "status");
                assert_eq!(operation, Some(CompareOp::Eq));
                assert_eq!(value, Some(ScalarValue::Utf8("active".to_string())));
                assert_eq!(priority, FilterPriority::DEFAULT);
                assert_eq!(uid, None);
                assert!(index_strategy.is_none());
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_with_event_type_uid() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            Some("uid123".to_string()),
        );

        match filter {
            FilterGroup::Filter { uid, .. } => {
                assert_eq!(uid, Some("uid123".to_string()));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_sets_priority_for_event_type() {
        let filter = FilterGroup::new_filter(
            "event_type".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("order_created".to_string())),
            None,
        );

        match filter {
            FilterGroup::Filter { priority, .. } => {
                assert_eq!(priority, FilterPriority::EVENT_TYPE);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_sets_priority_for_context_id() {
        let filter = FilterGroup::new_filter(
            "context_id".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("user-123".to_string())),
            None,
        );

        match filter {
            FilterGroup::Filter { priority, .. } => {
                assert_eq!(priority, FilterPriority::CONTEXT_ID);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_sets_priority_for_timestamp() {
        let filter = FilterGroup::new_filter(
            "timestamp".to_string(),
            Some(CompareOp::Gt),
            Some(ScalarValue::Timestamp(1609459200)),
            None,
        );

        match filter {
            FilterGroup::Filter { priority, .. } => {
                assert_eq!(priority, FilterPriority::TIMESTAMP);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_with_none_operation() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            None,
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        match filter {
            FilterGroup::Filter { operation, .. } => {
                assert_eq!(operation, None);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_filter_with_none_value() {
        let filter = FilterGroup::new_filter("status".to_string(), Some(CompareOp::Eq), None, None);

        match filter {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, None);
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    // ─────────────────────────────
    // FilterGroup::new_equality_filter Tests
    // ─────────────────────────────

    #[test]
    fn test_new_equality_filter_basic() {
        let filter = FilterGroup::new_equality_filter(
            "status".to_string(),
            ScalarValue::Utf8("active".to_string()),
            None,
        );

        match filter {
            FilterGroup::Filter {
                column,
                operation,
                value,
                ..
            } => {
                assert_eq!(column, "status");
                assert_eq!(operation, Some(CompareOp::Eq));
                assert_eq!(value, Some(ScalarValue::Utf8("active".to_string())));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_equality_filter_with_int64() {
        let filter =
            FilterGroup::new_equality_filter("id".to_string(), ScalarValue::Int64(42), None);

        match filter {
            FilterGroup::Filter {
                operation, value, ..
            } => {
                assert_eq!(operation, Some(CompareOp::Eq));
                assert_eq!(value, Some(ScalarValue::Int64(42)));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_equality_filter_with_uid() {
        let filter = FilterGroup::new_equality_filter(
            "status".to_string(),
            ScalarValue::Utf8("active".to_string()),
            Some("uid456".to_string()),
        );

        match filter {
            FilterGroup::Filter { uid, .. } => {
                assert_eq!(uid, Some("uid456".to_string()));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    // ─────────────────────────────
    // FilterGroup::new_equality_filters Tests
    // ─────────────────────────────

    #[test]
    fn test_new_equality_filters_empty() {
        let filters = FilterGroup::new_equality_filters("status".to_string(), &[], None);
        assert!(filters.is_empty());
    }

    #[test]
    fn test_new_equality_filters_single() {
        let filters =
            FilterGroup::new_equality_filters("status".to_string(), &[json!("active")], None);

        assert_eq!(filters.len(), 1);
        match &filters[0] {
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

    #[test]
    fn test_new_equality_filters_multiple() {
        let filters = FilterGroup::new_equality_filters(
            "status".to_string(),
            &[json!("active"), json!("pending"), json!("completed")],
            None,
        );

        assert_eq!(filters.len(), 3);
        for filter in &filters {
            match filter {
                FilterGroup::Filter {
                    column, operation, ..
                } => {
                    assert_eq!(column, "status");
                    assert_eq!(operation, &Some(CompareOp::Eq));
                }
                _ => panic!("Expected Filter variant"),
            }
        }
    }

    #[test]
    fn test_new_equality_filters_with_uid() {
        let filters = FilterGroup::new_equality_filters(
            "status".to_string(),
            &[json!("active")],
            Some("uid789".to_string()),
        );

        match &filters[0] {
            FilterGroup::Filter { uid, .. } => {
                assert_eq!(uid, &Some("uid789".to_string()));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_new_equality_filters_mixed_types() {
        let filters = FilterGroup::new_equality_filters(
            "value".to_string(),
            &[json!(42), json!(3.14), json!(true), json!("text")],
            None,
        );

        assert_eq!(filters.len(), 4);
        // Verify each filter has correct value type
        match &filters[0] {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, &Some(ScalarValue::Int64(42)));
            }
            _ => panic!("Expected Filter variant"),
        }
        match &filters[1] {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, &Some(ScalarValue::Float64(3.14)));
            }
            _ => panic!("Expected Filter variant"),
        }
        match &filters[2] {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, &Some(ScalarValue::Boolean(true)));
            }
            _ => panic!("Expected Filter variant"),
        }
        match &filters[3] {
            FilterGroup::Filter { value, .. } => {
                assert_eq!(value, &Some(ScalarValue::Utf8("text".to_string())));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    // ─────────────────────────────
    // extract_individual_filters Tests
    // ─────────────────────────────

    #[test]
    fn test_extract_individual_filters_single() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 1);
        assert!(matches!(extracted[0], FilterGroup::Filter { .. }));
    }

    #[test]
    fn test_extract_individual_filters_and_group() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "a".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(1)),
                None,
            ),
            FilterGroup::new_filter(
                "b".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(2)),
                None,
            ),
            FilterGroup::new_filter(
                "c".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(3)),
                None,
            ),
        ]);

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 3);
        for f in &extracted {
            assert!(matches!(f, FilterGroup::Filter { .. }));
        }
    }

    #[test]
    fn test_extract_individual_filters_or_group() {
        let filter = FilterGroup::Or(vec![
            FilterGroup::new_filter(
                "x".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(1)),
                None,
            ),
            FilterGroup::new_filter(
                "y".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(2)),
                None,
            ),
        ]);

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 2);
    }

    #[test]
    fn test_extract_individual_filters_not_group() {
        let filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("inactive".to_string())),
            None,
        )));

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 1);
    }

    #[test]
    fn test_extract_individual_filters_nested() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "a".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(1)),
                None,
            ),
            FilterGroup::Or(vec![
                FilterGroup::new_filter(
                    "b".to_string(),
                    Some(CompareOp::Eq),
                    Some(ScalarValue::Int64(2)),
                    None,
                ),
                FilterGroup::new_filter(
                    "c".to_string(),
                    Some(CompareOp::Eq),
                    Some(ScalarValue::Int64(3)),
                    None,
                ),
            ]),
            FilterGroup::new_filter(
                "d".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(4)),
                None,
            ),
        ]);

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 4);
    }

    #[test]
    fn test_extract_individual_filters_deeply_nested() {
        let filter = FilterGroup::And(vec![
            FilterGroup::Or(vec![
                FilterGroup::Not(Box::new(FilterGroup::new_filter(
                    "a".to_string(),
                    Some(CompareOp::Eq),
                    Some(ScalarValue::Int64(1)),
                    None,
                ))),
                FilterGroup::new_filter(
                    "b".to_string(),
                    Some(CompareOp::Eq),
                    Some(ScalarValue::Int64(2)),
                    None,
                ),
            ]),
            FilterGroup::new_filter(
                "c".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(3)),
                None,
            ),
        ]);

        let extracted = filter.extract_individual_filters();
        assert_eq!(extracted.len(), 3);
    }

    // ─────────────────────────────
    // extract_unique_filters Tests
    // ─────────────────────────────

    #[test]
    fn test_extract_unique_filters_single() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 1);
    }

    #[test]
    fn test_extract_unique_filters_deduplicates_identical() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "status".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Utf8("active".to_string())),
                None,
            ),
            FilterGroup::new_filter(
                "status".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Utf8("active".to_string())),
                None,
            ),
        ]);

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 1);
    }

    #[test]
    fn test_extract_unique_filters_preserves_different() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "status".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Utf8("active".to_string())),
                None,
            ),
            FilterGroup::new_filter(
                "status".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Utf8("inactive".to_string())),
                None,
            ),
        ]);

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn test_extract_unique_filters_deduplicates_across_nested_groups() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "status".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Utf8("active".to_string())),
                None,
            ),
            FilterGroup::Or(vec![
                FilterGroup::new_filter(
                    "status".to_string(),
                    Some(CompareOp::Eq),
                    Some(ScalarValue::Utf8("active".to_string())),
                    None,
                ),
                FilterGroup::new_filter(
                    "amount".to_string(),
                    Some(CompareOp::Gt),
                    Some(ScalarValue::Int64(100)),
                    None,
                ),
            ]),
        ]);

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 2); // status=active (deduplicated) + amount>100
    }

    #[test]
    fn test_extract_unique_filters_different_operations_same_column() {
        let filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "amount".to_string(),
                Some(CompareOp::Gt),
                Some(ScalarValue::Int64(100)),
                None,
            ),
            FilterGroup::new_filter(
                "amount".to_string(),
                Some(CompareOp::Lt),
                Some(ScalarValue::Int64(200)),
                None,
            ),
        ]);

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn test_extract_unique_filters_not_group() {
        let filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("inactive".to_string())),
            None,
        )));

        let unique = filter.extract_unique_filters();
        assert_eq!(unique.len(), 1);
    }

    // ─────────────────────────────
    // operator Tests
    // ─────────────────────────────

    #[test]
    fn test_operator_single_filter() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert_eq!(filter.operator(), LogicalOp::And);
    }

    #[test]
    fn test_operator_and_group() {
        let filter = FilterGroup::And(vec![]);
        assert_eq!(filter.operator(), LogicalOp::And);
    }

    #[test]
    fn test_operator_or_group() {
        let filter = FilterGroup::Or(vec![]);
        assert_eq!(filter.operator(), LogicalOp::Or);
    }

    #[test]
    fn test_operator_not_group() {
        let filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        )));
        assert_eq!(filter.operator(), LogicalOp::Not);
    }

    // ─────────────────────────────
    // is_single_filter Tests
    // ─────────────────────────────

    #[test]
    fn test_is_single_filter_true() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert!(filter.is_single_filter());
    }

    #[test]
    fn test_is_single_filter_false_and() {
        let filter = FilterGroup::And(vec![]);
        assert!(!filter.is_single_filter());
    }

    #[test]
    fn test_is_single_filter_false_or() {
        let filter = FilterGroup::Or(vec![]);
        assert!(!filter.is_single_filter());
    }

    #[test]
    fn test_is_single_filter_false_not() {
        let filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        )));
        assert!(!filter.is_single_filter());
    }

    // ─────────────────────────────
    // is_event_type Tests
    // ─────────────────────────────

    #[test]
    fn test_is_event_type_true() {
        let filter = FilterGroup::new_filter(
            "event_type".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("order_created".to_string())),
            None,
        );

        assert!(filter.is_event_type());
    }

    #[test]
    fn test_is_event_type_false() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert!(!filter.is_event_type());
    }

    #[test]
    fn test_is_event_type_false_for_group() {
        let filter = FilterGroup::And(vec![]);
        assert!(!filter.is_event_type());
    }

    // ─────────────────────────────
    // is_context_id Tests
    // ─────────────────────────────

    #[test]
    fn test_is_context_id_true() {
        let filter = FilterGroup::new_filter(
            "context_id".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("user-123".to_string())),
            None,
        );

        assert!(filter.is_context_id());
    }

    #[test]
    fn test_is_context_id_false() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert!(!filter.is_context_id());
    }

    #[test]
    fn test_is_context_id_false_for_group() {
        let filter = FilterGroup::Or(vec![]);
        assert!(!filter.is_context_id());
    }

    // ─────────────────────────────
    // column/operation/value Getters Tests
    // ─────────────────────────────

    #[test]
    fn test_column_getter() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert_eq!(filter.column(), Some("status"));
    }

    #[test]
    fn test_column_getter_none_for_group() {
        let filter = FilterGroup::And(vec![]);
        assert_eq!(filter.column(), None);
    }

    #[test]
    fn test_operation_getter() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Gt),
            Some(ScalarValue::Int64(100)),
            None,
        );

        assert_eq!(filter.operation(), Some(&CompareOp::Gt));
    }

    #[test]
    fn test_operation_getter_none() {
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            None,
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        assert_eq!(filter.operation(), None);
    }

    #[test]
    fn test_operation_getter_none_for_group() {
        let filter = FilterGroup::Or(vec![]);
        assert_eq!(filter.operation(), None);
    }

    #[test]
    fn test_value_getter() {
        let value = ScalarValue::Utf8("active".to_string());
        let filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(value.clone()),
            None,
        );

        assert_eq!(filter.value(), Some(&value));
    }

    #[test]
    fn test_value_getter_none() {
        let filter = FilterGroup::new_filter("status".to_string(), Some(CompareOp::Eq), None, None);

        assert_eq!(filter.value(), None);
    }

    #[test]
    fn test_value_getter_none_for_group() {
        let filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        )));
        assert_eq!(filter.value(), None);
    }

    // ─────────────────────────────
    // index_strategy_mut Tests
    // ─────────────────────────────

    #[test]
    fn test_index_strategy_mut_some() {
        let mut filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let strategy_ref = filter.index_strategy_mut();
        assert!(strategy_ref.is_some());
        assert!(strategy_ref.unwrap().is_none());
    }

    #[test]
    fn test_index_strategy_mut_none_for_group() {
        let mut filter = FilterGroup::And(vec![]);
        assert!(filter.index_strategy_mut().is_none());
    }

    #[test]
    fn test_index_strategy_mut_set_value() {
        let mut filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let strategy = IndexStrategy::ZoneSuRF {
            field: "status".to_string(),
        };
        *filter.index_strategy_mut().unwrap() = Some(strategy.clone());

        match filter {
            FilterGroup::Filter { index_strategy, .. } => {
                assert!(index_strategy.is_some());
                match index_strategy {
                    Some(IndexStrategy::ZoneSuRF { field }) => {
                        assert_eq!(field, "status");
                    }
                    _ => panic!("Expected ZoneSuRF strategy"),
                }
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    // ─────────────────────────────
    // sync_index_strategies_from Tests
    // ─────────────────────────────

    #[test]
    fn test_sync_index_strategies_from_matches_single_filter() {
        let mut filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let source_filter = FilterGroup::Filter {
            column: "status".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(ScalarValue::Utf8("active".to_string())),
            priority: 2,
            uid: None,
            index_strategy: Some(IndexStrategy::ZoneSuRF {
                field: "status".to_string(),
            }),
        };

        filter.sync_index_strategies_from(&[source_filter]);

        match filter {
            FilterGroup::Filter { index_strategy, .. } => {
                assert!(index_strategy.is_some());
                assert!(matches!(
                    index_strategy.unwrap(),
                    IndexStrategy::ZoneSuRF { .. }
                ));
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_no_match() {
        let mut filter = FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        );

        let source_filter = FilterGroup::new_filter(
            "amount".to_string(),
            Some(CompareOp::Gt),
            Some(ScalarValue::Int64(100)),
            None,
        );

        filter.sync_index_strategies_from(&[source_filter]);

        match filter {
            FilterGroup::Filter { index_strategy, .. } => {
                assert!(
                    index_strategy.is_none(),
                    "Index strategy should remain None when no match"
                );
            }
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_preserves_existing_strategy() {
        let mut filter = FilterGroup::Filter {
            column: "status".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(ScalarValue::Utf8("active".to_string())),
            priority: 2,
            uid: None,
            index_strategy: Some(IndexStrategy::FullScan),
        };

        let source_filter = FilterGroup::Filter {
            column: "status".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(ScalarValue::Utf8("active".to_string())),
            priority: 2,
            uid: None,
            index_strategy: Some(IndexStrategy::ZoneSuRF {
                field: "status".to_string(),
            }),
        };

        filter.sync_index_strategies_from(&[source_filter]);

        match filter {
            FilterGroup::Filter { index_strategy, .. } => match index_strategy {
                Some(IndexStrategy::FullScan) => {}
                _ => panic!("Expected FullScan strategy to be preserved"),
            },
            _ => panic!("Expected Filter variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_and_group() {
        let mut filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "a".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(1)),
                None,
            ),
            FilterGroup::new_filter(
                "b".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(2)),
                None,
            ),
        ]);

        let source_filters = vec![
            FilterGroup::Filter {
                column: "a".to_string(),
                operation: Some(CompareOp::Eq),
                value: Some(ScalarValue::Int64(1)),
                priority: 2,
                uid: None,
                index_strategy: Some(IndexStrategy::ZoneSuRF {
                    field: "a".to_string(),
                }),
            },
            FilterGroup::Filter {
                column: "b".to_string(),
                operation: Some(CompareOp::Eq),
                value: Some(ScalarValue::Int64(2)),
                priority: 2,
                uid: None,
                index_strategy: Some(IndexStrategy::EnumBitmap {
                    field: "b".to_string(),
                }),
            },
        ];

        filter.sync_index_strategies_from(&source_filters);

        match filter {
            FilterGroup::And(children) => {
                match &children[0] {
                    FilterGroup::Filter { index_strategy, .. } => {
                        assert!(matches!(
                            index_strategy.as_ref().unwrap(),
                            IndexStrategy::ZoneSuRF { .. }
                        ));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[1] {
                    FilterGroup::Filter { index_strategy, .. } => {
                        assert!(matches!(
                            index_strategy.as_ref().unwrap(),
                            IndexStrategy::EnumBitmap { .. }
                        ));
                    }
                    _ => panic!("Expected Filter variant"),
                }
            }
            _ => panic!("Expected And variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_or_group() {
        let mut filter = FilterGroup::Or(vec![FilterGroup::new_filter(
            "x".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Int64(1)),
            None,
        )]);

        let source_filters = vec![FilterGroup::Filter {
            column: "x".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(ScalarValue::Int64(1)),
            priority: 2,
            uid: None,
            index_strategy: Some(IndexStrategy::FullScan),
        }];

        filter.sync_index_strategies_from(&source_filters);

        match filter {
            FilterGroup::Or(children) => match &children[0] {
                FilterGroup::Filter { index_strategy, .. } => match index_strategy {
                    Some(IndexStrategy::FullScan) => {}
                    _ => panic!("Expected FullScan strategy"),
                },
                _ => panic!("Expected Filter variant"),
            },
            _ => panic!("Expected Or variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_not_group() {
        let mut filter = FilterGroup::Not(Box::new(FilterGroup::new_filter(
            "status".to_string(),
            Some(CompareOp::Eq),
            Some(ScalarValue::Utf8("active".to_string())),
            None,
        )));

        let source_filters = vec![FilterGroup::Filter {
            column: "status".to_string(),
            operation: Some(CompareOp::Eq),
            value: Some(ScalarValue::Utf8("active".to_string())),
            priority: 2,
            uid: None,
            index_strategy: Some(IndexStrategy::ZoneSuRF {
                field: "status".to_string(),
            }),
        }];

        filter.sync_index_strategies_from(&source_filters);

        match filter {
            FilterGroup::Not(child) => match child.as_ref() {
                FilterGroup::Filter { index_strategy, .. } => {
                    assert!(matches!(
                        index_strategy.as_ref().unwrap(),
                        IndexStrategy::ZoneSuRF { .. }
                    ));
                }
                _ => panic!("Expected Filter variant"),
            },
            _ => panic!("Expected Not variant"),
        }
    }

    #[test]
    fn test_sync_index_strategies_from_nested() {
        let mut filter = FilterGroup::And(vec![
            FilterGroup::new_filter(
                "a".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(1)),
                None,
            ),
            FilterGroup::Or(vec![FilterGroup::new_filter(
                "b".to_string(),
                Some(CompareOp::Eq),
                Some(ScalarValue::Int64(2)),
                None,
            )]),
        ]);

        let source_filters = vec![
            FilterGroup::Filter {
                column: "a".to_string(),
                operation: Some(CompareOp::Eq),
                value: Some(ScalarValue::Int64(1)),
                priority: 2,
                uid: None,
                index_strategy: Some(IndexStrategy::ZoneSuRF {
                    field: "a".to_string(),
                }),
            },
            FilterGroup::Filter {
                column: "b".to_string(),
                operation: Some(CompareOp::Eq),
                value: Some(ScalarValue::Int64(2)),
                priority: 2,
                uid: None,
                index_strategy: Some(IndexStrategy::EnumBitmap {
                    field: "b".to_string(),
                }),
            },
        ];

        filter.sync_index_strategies_from(&source_filters);

        match filter {
            FilterGroup::And(children) => {
                match &children[0] {
                    FilterGroup::Filter { index_strategy, .. } => {
                        assert!(matches!(
                            index_strategy.as_ref().unwrap(),
                            IndexStrategy::ZoneSuRF { .. }
                        ));
                    }
                    _ => panic!("Expected Filter variant"),
                }
                match &children[1] {
                    FilterGroup::Or(or_children) => match &or_children[0] {
                        FilterGroup::Filter { index_strategy, .. } => {
                            assert!(matches!(
                                index_strategy.as_ref().unwrap(),
                                IndexStrategy::EnumBitmap { .. }
                            ));
                        }
                        _ => panic!("Expected Filter variant"),
                    },
                    _ => panic!("Expected Or variant"),
                }
            }
            _ => panic!("Expected And variant"),
        }
    }
}
