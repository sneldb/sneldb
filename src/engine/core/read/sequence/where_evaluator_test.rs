
use crate::command::types::{CompareOp, Expr};
use crate::engine::core::read::sequence::group::RowIndex;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use serde_json::json;
use std::collections::HashMap;

/// Helper to create a test zone with payload fields for WHERE clause testing
fn create_zone_with_payload(
    zone_id: u32,
    segment_id: &str,
    context_ids: &[&str],
    user_ids: &[&str],
    timestamps: &[i64],
    payload_fields: &HashMap<String, Vec<String>>,
) -> crate::engine::core::CandidateZone {
    use crate::engine::core::column::column_values::ColumnValues;
    use crate::engine::core::read::cache::DecompressedBlock;
    use std::sync::Arc;

    let mut values_map: HashMap<String, ColumnValues> = HashMap::new();

    let create_column = |strings: &[&str]| -> ColumnValues {
        let mut bytes: Vec<u8> = Vec::new();
        let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(strings.len());
        for s in strings {
            let start = bytes.len();
            bytes.extend_from_slice(s.as_bytes());
            let len = s.as_bytes().len();
            ranges.push((start, len));
        }
        let block = Arc::new(DecompressedBlock::from_bytes(bytes));
        ColumnValues::new(block, ranges)
    };

    values_map.insert("context_id".to_string(), create_column(context_ids));
    values_map.insert("user_id".to_string(), create_column(user_ids));

    let timestamp_strings: Vec<String> = timestamps.iter().map(|t| t.to_string()).collect();
    let timestamp_strs: Vec<&str> = timestamp_strings.iter().map(|s| s.as_str()).collect();
    values_map.insert("timestamp".to_string(), create_column(&timestamp_strs));

    // Add payload fields
    for (field_name, values) in payload_fields {
        let value_strs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        values_map.insert(field_name.clone(), create_column(&value_strs));
    }

    let mut zone = crate::engine::core::CandidateZone::new(zone_id, segment_id.to_string());
    zone.set_values(values_map);
    zone
}

#[test]
fn test_extract_event_specific_condition() {
    // Test: page_view.page = "/checkout"
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let _evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Should have evaluator for page_view but not order_created
    // (since order_created has no conditions, evaluate_row should return true for it)
    // The actual evaluation happens in matcher.rs with real zones
}

#[test]
fn test_evaluate_row_with_event_specific_field() {
    // Test: page_view.page = "/checkout"
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create zone with page field
    let mut payload_fields = HashMap::new();
    payload_fields.insert("page".to_string(), vec!["/checkout".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: page = "/checkout"
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when page matches");

    // Test with non-matching value
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("page".to_string(), vec!["/home".to_string()]);

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when page doesn't match");
}

#[test]
fn test_evaluate_row_with_no_evaluator() {
    // Test: No WHERE clause, or event type not in WHERE clause
    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let evaluator = SequenceWhereEvaluator::new(None, &event_types);

    let mut payload_fields = HashMap::new();
    payload_fields.insert("page".to_string(), vec!["/checkout".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: no evaluator means row passes
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when no evaluator exists");
}

#[test]
fn test_evaluate_row_with_event_not_in_where_clause() {
    // Test: WHERE clause only has page_view.page, but we evaluate order_created
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    let mut payload_fields = HashMap::new();
    payload_fields.insert("status".to_string(), vec!["done".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: order_created has no evaluator, so it passes
    let result = evaluator.evaluate_row("order_created", &zone, &row_index);
    assert!(result, "Should pass when event type has no evaluator");
}

#[test]
fn test_extract_and_condition_with_event_specific_fields() {
    // Test: page_view.page = "/checkout" AND order_created.status = "done"
    let where_clause = Expr::And(
        Box::new(Expr::Compare {
            field: "page_view.page".to_string(),
            op: CompareOp::Eq,
            value: json!("/checkout"),
        }),
        Box::new(Expr::Compare {
            field: "order_created.status".to_string(),
            op: CompareOp::Eq,
            value: json!("done"),
        }),
    );

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create zones for both event types
    let mut page_payload = HashMap::new();
    page_payload.insert("page".to_string(), vec!["/checkout".to_string()]);

    let page_zone =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &page_payload);

    let mut status_payload = HashMap::new();
    status_payload.insert("status".to_string(), vec!["done".to_string()]);

    let order_zone =
        create_zone_with_payload(0, "seg1", &["ctx2"], &["user1"], &[1500], &status_payload);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Both should pass
    let page_result = evaluator.evaluate_row("page_view", &page_zone, &row_index);
    let order_result = evaluator.evaluate_row("order_created", &order_zone, &row_index);

    assert!(page_result, "page_view should pass");
    assert!(order_result, "order_created should pass");
}

#[test]
fn test_extract_common_field_condition() {
    // Test: timestamp > 1000 (common field)
    let where_clause = Expr::Compare {
        field: "timestamp".to_string(),
        op: CompareOp::Gt,
        value: json!(1000),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create zone with timestamp
    let zone = create_zone_with_payload(
        0,
        "seg1",
        &["ctx1"],
        &["user1"],
        &[1500], // > 1000
        &HashMap::new(),
    );

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: timestamp > 1000
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when timestamp > 1000");

    // Test with timestamp <= 1000
    let zone2 = create_zone_with_payload(
        0,
        "seg1",
        &["ctx1"],
        &["user1"],
        &[500], // <= 1000
        &HashMap::new(),
    );

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when timestamp <= 1000");
}

#[test]
fn test_parse_event_field() {
    // Test the parse_event_field logic indirectly through evaluation
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create zone with page field (not page_view.page)
    let mut payload_fields = HashMap::new();
    payload_fields.insert("page".to_string(), vec!["/checkout".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: page_view.page was parsed to just "page"
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass after parsing page_view.page to page");
}

#[test]
fn test_multiple_conditions_for_same_event() {
    // Test: page_view.page = "/checkout" AND page_view.user_id = "u1"
    // This tests that multiple conditions for the same event are combined with AND
    let where_clause = Expr::And(
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

    let event_types = vec!["page_view".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create zone with both fields matching
    let mut payload_fields = HashMap::new();
    payload_fields.insert("page".to_string(), vec!["/checkout".to_string()]);
    payload_fields.insert("user_id".to_string(), vec!["u1".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: both conditions match
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when all conditions match");

    // Test with one condition failing
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("page".to_string(), vec!["/checkout".to_string()]);
    payload_fields2.insert("user_id".to_string(), vec!["u2".to_string()]); // Different user

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when one condition doesn't match");
}

#[test]
fn test_sequence_where_clause_scenario() {
    // Test the actual scenario from integration test:
    // page_view.page = "/checkout" for page_view FOLLOWED BY order_created
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types);

    // Create page_view zone with /checkout (should pass)
    let mut page_payload = HashMap::new();
    page_payload.insert("page".to_string(), vec!["/checkout".to_string()]);

    let page_zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["u1"], &[1000], &page_payload);

    // Create page_view zone with /home (should fail)
    let mut page_payload2 = HashMap::new();
    page_payload2.insert("page".to_string(), vec!["/home".to_string()]);

    let page_zone2 =
        create_zone_with_payload(0, "seg1", &["ctx2"], &["u2"], &[1000], &page_payload2);

    // Create order_created zone (should pass - no evaluator for order_created)
    let order_zone =
        create_zone_with_payload(0, "seg1", &["ctx3"], &["u1"], &[1500], &HashMap::new());

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Test page_view with /checkout
    let page_result1 = evaluator.evaluate_row("page_view", &page_zone, &row_index);
    assert!(page_result1, "page_view with /checkout should pass");

    // Test page_view with /home
    let page_result2 = evaluator.evaluate_row("page_view", &page_zone2, &row_index);
    assert!(!page_result2, "page_view with /home should fail");

    // Test order_created (no evaluator, should pass)
    let order_result = evaluator.evaluate_row("order_created", &order_zone, &row_index);
    assert!(order_result, "order_created should pass (no evaluator)");
}
