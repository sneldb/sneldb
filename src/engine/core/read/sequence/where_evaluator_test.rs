use crate::command::types::{CompareOp, Expr};
use crate::engine::core::read::sequence::group::RowIndex;
use crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator;
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use crate::engine::schema::types::FieldType;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Helper to create a test registry with schemas for event types
async fn create_test_registry(
    event_types_and_fields: &[(&str, &[&str])],
) -> Arc<RwLock<SchemaRegistry>> {
    let tempdir = tempdir().expect("Failed to create temp dir");
    let path = tempdir.path().join("schemas.bin");
    let mut registry = SchemaRegistry::new_with_path(path).expect("Failed to create registry");

    for (event_type, fields) in event_types_and_fields {
        let mut schema_fields = HashMap::new();
        for field in *fields {
            schema_fields.insert(field.to_string(), FieldType::String);
        }
        let schema = MiniSchema {
            fields: schema_fields,
        };
        registry
            .define(event_type, schema)
            .expect("Failed to define schema");
    }

    Arc::new(RwLock::new(registry))
}

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

#[tokio::test]
async fn test_extract_event_specific_condition() {
    // Test: page_view.page = "/checkout"
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let _evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Should have evaluator for page_view but not order_created
    // (since order_created has no conditions, evaluate_row should return true for it)
    // The actual evaluation happens in matcher.rs with real zones
}

#[tokio::test]
async fn test_evaluate_row_with_event_specific_field() {
    // Test: page_view.page = "/checkout"
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["page"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_evaluate_row_with_no_evaluator() {
    // Test: No WHERE clause, or event type not in WHERE clause
    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(None, &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_evaluate_row_with_event_not_in_where_clause() {
    // Test: WHERE clause only has page_view.page, but we evaluate order_created
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_extract_and_condition_with_event_specific_fields() {
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
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_extract_common_field_condition() {
    // Test: timestamp > 1000 (common field - core field, should not cause ambiguity)
    let where_clause = Expr::Compare {
        field: "timestamp".to_string(),
        op: CompareOp::Gt,
        value: json!(1000),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_parse_event_field() {
    // Test the parse_event_field logic indirectly through evaluation
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["page"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_multiple_conditions_for_same_event() {
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
    let registry = create_test_registry(&[("page_view", &["page", "user_id"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_sequence_where_clause_scenario() {
    // Test the actual scenario from integration test:
    // page_view.page = "/checkout" for page_view FOLLOWED BY order_created
    let where_clause = Expr::Compare {
        field: "page_view.page".to_string(),
        op: CompareOp::Eq,
        value: json!("/checkout"),
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["page"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

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

#[tokio::test]
async fn test_ambiguous_field_error() {
    // Test that ambiguous fields (without event prefix) that exist in multiple event types
    // cause an error
    let where_clause = Expr::Compare {
        field: "status".to_string(), // Common field without prefix
        op: CompareOp::Eq,
        value: json!("done"),
    };

    let event_types = vec!["order_created".to_string(), "payment_failed".to_string()];
    let registry = create_test_registry(&[
        ("order_created", &["status"]),
        ("payment_failed", &["status"]), // Both have "status" field
    ])
    .await;

    let result = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry).await;

    assert!(result.is_err(), "Should return error for ambiguous field");
    if let Err(error_msg) = result {
        assert!(
            error_msg.contains("Ambiguous field 'status'"),
            "Error message should mention ambiguous field"
        );
        assert!(
            error_msg.contains("order_created") && error_msg.contains("payment_failed"),
            "Error message should mention both event types"
        );
    }
}

#[tokio::test]
async fn test_non_ambiguous_field_passes() {
    // Test that a common field that exists in only one event type doesn't cause an error
    let where_clause = Expr::Compare {
        field: "status".to_string(),
        op: CompareOp::Eq,
        value: json!("done"),
    };

    let event_types = vec!["order_created".to_string(), "payment_failed".to_string()];
    let registry = create_test_registry(&[
        ("order_created", &["status"]),
        ("payment_failed", &["amount"]), // payment_failed has amount but not status
    ])
    .await;

    let result = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry).await;

    assert!(
        result.is_ok(),
        "Should not return error when field is not ambiguous"
    );
}

// =============================================================================
// IN OPERATOR TESTS
// =============================================================================

#[tokio::test]
async fn test_in_clause_with_event_specific_field() {
    // Test: page_view.id IN (1, 2, 3)
    let where_clause = Expr::In {
        field: "page_view.id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["id"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with id field matching IN list
    let mut payload_fields = HashMap::new();
    payload_fields.insert("id".to_string(), vec!["2".to_string()]); // id=2 is in [1,2,3]

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: id=2 is in [1,2,3]
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when id is in the IN list");

    // Test with non-matching value
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("id".to_string(), vec!["5".to_string()]); // id=5 is not in [1,2,3]

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when id is not in the IN list");
}

#[tokio::test]
async fn test_in_clause_with_common_field() {
    // Test: id IN (1, 2, 3) - common field, but only exists in one event type (not ambiguous)
    let where_clause = Expr::In {
        field: "id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    };

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    // Only page_view has "id", order_created has "status" - not ambiguous
    let registry =
        create_test_registry(&[("page_view", &["id"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone for page_view with matching id
    let mut payload_fields = HashMap::new();
    payload_fields.insert("id".to_string(), vec!["2".to_string()]);

    let page_zone =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // page_view should pass: id=2 is in [1,2,3]
    let page_result = evaluator.evaluate_row("page_view", &page_zone, &row_index);
    assert!(page_result, "page_view should pass when id is in IN list");

    // order_created gets an evaluator for "id" (common field applied to all event types)
    // but since order_created zones don't have "id" field, evaluation fails
    let mut order_payload = HashMap::new();
    order_payload.insert("status".to_string(), vec!["done".to_string()]);
    let order_zone =
        create_zone_with_payload(0, "seg1", &["ctx2"], &["user1"], &[1500], &order_payload);
    let order_result = evaluator.evaluate_row("order_created", &order_zone, &row_index);
    // Common fields are applied to all event types, so order_created gets evaluator for "id"
    // but since the zone doesn't have "id", the condition fails
    assert!(
        !order_result,
        "order_created should fail (has evaluator for id but field missing)"
    );
}

#[tokio::test]
async fn test_in_clause_with_string_values() {
    // Test: page_view.status IN ("active", "completed")
    let where_clause = Expr::In {
        field: "page_view.status".to_string(),
        values: vec![json!("active"), json!("completed")],
    };

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with status="active" (in list)
    let mut payload_fields = HashMap::new();
    payload_fields.insert("status".to_string(), vec!["active".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: status="active" is in ["active", "completed"]
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(result, "Should pass when status is in the IN list");

    // Test with non-matching value
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("status".to_string(), vec!["pending".to_string()]);

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when status is not in the IN list");
}

#[tokio::test]
async fn test_in_clause_combined_with_and() {
    // Test: page_view.id IN (1, 2, 3) AND page_view.status = "active"
    let where_clause = Expr::And(
        Box::new(Expr::In {
            field: "page_view.id".to_string(),
            values: vec![json!(1), json!(2), json!(3)],
        }),
        Box::new(Expr::Compare {
            field: "page_view.status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
    );

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["id", "status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with both conditions matching
    let mut payload_fields = HashMap::new();
    payload_fields.insert("id".to_string(), vec!["2".to_string()]);
    payload_fields.insert("status".to_string(), vec!["active".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: both conditions match
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(
        result,
        "Should pass when both IN and Compare conditions match"
    );

    // Test with IN condition failing
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("id".to_string(), vec!["5".to_string()]); // Not in [1,2,3]
    payload_fields2.insert("status".to_string(), vec!["active".to_string()]);

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(!result2, "Should fail when IN condition doesn't match");

    // Test with Compare condition failing
    let mut payload_fields3 = HashMap::new();
    payload_fields3.insert("id".to_string(), vec!["2".to_string()]);
    payload_fields3.insert("status".to_string(), vec!["pending".to_string()]); // Not "active"

    let zone3 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields3);

    let result3 = evaluator.evaluate_row("page_view", &zone3, &row_index);
    assert!(!result3, "Should fail when Compare condition doesn't match");
}

#[tokio::test]
async fn test_in_clause_combined_with_or() {
    // Test: page_view.id IN (1, 2) OR page_view.status = "active"
    // OR logic should now be preserved correctly
    let where_clause = Expr::Or(
        Box::new(Expr::In {
            field: "page_view.id".to_string(),
            values: vec![json!(1), json!(2)],
        }),
        Box::new(Expr::Compare {
            field: "page_view.status".to_string(),
            op: CompareOp::Eq,
            value: json!("active"),
        }),
    );

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["id", "status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with both conditions matching (should pass)
    let mut payload_fields1 = HashMap::new();
    payload_fields1.insert("id".to_string(), vec!["2".to_string()]);
    payload_fields1.insert("status".to_string(), vec!["active".to_string()]);

    let zone1 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields1);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: both conditions match
    let result1 = evaluator.evaluate_row("page_view", &zone1, &row_index);
    assert!(result1, "Should pass when both conditions match");

    // Create zone with only IN condition matching (should pass with OR logic)
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("id".to_string(), vec!["2".to_string()]);
    payload_fields2.insert("status".to_string(), vec!["pending".to_string()]); // Not "active"

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    // Should pass: one condition matches (OR logic)
    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(result2, "Should pass when one condition matches (OR logic)");

    // Create zone with only status matching (should pass with OR logic)
    let mut payload_fields3 = HashMap::new();
    payload_fields3.insert("id".to_string(), vec!["5".to_string()]); // Not in [1,2]
    payload_fields3.insert("status".to_string(), vec!["active".to_string()]);

    let zone3 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields3);

    // Should pass: one condition matches (OR logic)
    let result3 = evaluator.evaluate_row("page_view", &zone3, &row_index);
    assert!(result3, "Should pass when status matches (OR logic)");

    // Create zone with neither condition matching (should fail)
    let mut payload_fields4 = HashMap::new();
    payload_fields4.insert("id".to_string(), vec!["5".to_string()]); // Not in [1,2]
    payload_fields4.insert("status".to_string(), vec!["pending".to_string()]); // Not "active"

    let zone4 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields4);

    // Should fail: neither condition matches
    let result4 = evaluator.evaluate_row("page_view", &zone4, &row_index);
    assert!(!result4, "Should fail when neither condition matches");
}

#[tokio::test]
async fn test_in_clause_combined_with_not() {
    // Test: NOT (page_view.id IN (1, 2, 3))
    // NOT logic should now be preserved correctly
    let where_clause = Expr::Not(Box::new(Expr::In {
        field: "page_view.id".to_string(),
        values: vec![json!(1), json!(2), json!(3)],
    }));

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["id"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with id in list (should fail with NOT logic)
    let mut payload_fields = HashMap::new();
    payload_fields.insert("id".to_string(), vec!["2".to_string()]); // In [1,2,3]

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should fail: id=2 is in [1,2,3], so NOT(id IN [1,2,3]) is false
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(!result, "Should fail when id is in IN list (NOT logic)");

    // Test with id not in list (should pass with NOT logic)
    let mut payload_fields2 = HashMap::new();
    payload_fields2.insert("id".to_string(), vec!["5".to_string()]); // Not in [1,2,3]

    let zone2 =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields2);

    // Should pass: id=5 is not in [1,2,3], so NOT(id IN [1,2,3]) is true
    let result2 = evaluator.evaluate_row("page_view", &zone2, &row_index);
    assert!(result2, "Should pass when id is not in IN list (NOT logic)");
}

#[tokio::test]
async fn test_in_clause_ambiguous_field_error() {
    // Test that ambiguous IN fields (without event prefix) that exist in multiple event types
    // cause an error
    let where_clause = Expr::In {
        field: "status".to_string(), // Common field without prefix
        values: vec![json!("active"), json!("completed")],
    };

    let event_types = vec!["order_created".to_string(), "payment_failed".to_string()];
    let registry = create_test_registry(&[
        ("order_created", &["status"]),
        ("payment_failed", &["status"]), // Both have "status" field
    ])
    .await;

    let result = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry).await;

    assert!(
        result.is_err(),
        "Should return error for ambiguous IN field"
    );
    if let Err(error_msg) = result {
        assert!(
            error_msg.contains("Ambiguous field 'status'"),
            "Error message should mention ambiguous field"
        );
        assert!(
            error_msg.contains("order_created") && error_msg.contains("payment_failed"),
            "Error message should mention both event types"
        );
    }
}

#[tokio::test]
async fn test_in_clause_non_ambiguous_field_passes() {
    // Test that a common IN field that exists in only one event type doesn't cause an error
    let where_clause = Expr::In {
        field: "status".to_string(),
        values: vec![json!("active"), json!("completed")],
    };

    let event_types = vec!["order_created".to_string(), "payment_failed".to_string()];
    let registry = create_test_registry(&[
        ("order_created", &["status"]),
        ("payment_failed", &["amount"]), // payment_failed has amount but not status
    ])
    .await;

    let result = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry).await;

    assert!(
        result.is_ok(),
        "Should not return error when IN field is not ambiguous"
    );
}

#[tokio::test]
async fn test_in_clause_with_multiple_event_types() {
    // Test: page_view.id IN (1, 2) AND order_created.status IN ("done", "pending")
    let where_clause = Expr::And(
        Box::new(Expr::In {
            field: "page_view.id".to_string(),
            values: vec![json!(1), json!(2)],
        }),
        Box::new(Expr::In {
            field: "order_created.status".to_string(),
            values: vec![json!("done"), json!("pending")],
        }),
    );

    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry =
        create_test_registry(&[("page_view", &["id"]), ("order_created", &["status"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zones for both event types
    let mut page_payload = HashMap::new();
    page_payload.insert("id".to_string(), vec!["2".to_string()]);

    let page_zone =
        create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &page_payload);

    let mut order_payload = HashMap::new();
    order_payload.insert("status".to_string(), vec!["done".to_string()]);

    let order_zone =
        create_zone_with_payload(0, "seg1", &["ctx2"], &["user1"], &[1500], &order_payload);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Both should pass
    let page_result = evaluator.evaluate_row("page_view", &page_zone, &row_index);
    let order_result = evaluator.evaluate_row("order_created", &order_zone, &row_index);

    assert!(page_result, "page_view should pass when id is in IN list");
    assert!(
        order_result,
        "order_created should pass when status is in IN list"
    );
}

#[tokio::test]
async fn test_in_clause_with_single_value() {
    // Test: page_view.id IN (42)
    let where_clause = Expr::In {
        field: "page_view.id".to_string(),
        values: vec![json!(42)],
    };

    let event_types = vec!["page_view".to_string()];
    let registry = create_test_registry(&[("page_view", &["id"])]).await;
    let evaluator = SequenceWhereEvaluator::new(Some(&where_clause), &event_types, &registry)
        .await
        .expect("Should create evaluator");

    // Create zone with matching id
    let mut payload_fields = HashMap::new();
    payload_fields.insert("id".to_string(), vec!["42".to_string()]);

    let zone = create_zone_with_payload(0, "seg1", &["ctx1"], &["user1"], &[1000], &payload_fields);

    let row_index = RowIndex {
        zone_idx: 0,
        row_idx: 0,
    };

    // Should pass: id=42 is in [42]
    let result = evaluator.evaluate_row("page_view", &zone, &row_index);
    assert!(
        result,
        "Should pass when id matches single value in IN list"
    );
}
