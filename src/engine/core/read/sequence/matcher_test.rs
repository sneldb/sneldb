
use crate::command::types::{EventSequence, EventTarget, SequenceLink};
use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::ColumnarGrouper;
use crate::engine::core::read::sequence::matcher::SequenceMatcher;
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use crate::engine::schema::types::FieldType;
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
        registry.define(event_type, schema).expect("Failed to define schema");
    }

    Arc::new(RwLock::new(registry))
}

/// Helper to create test zones (reuse from group_test)
fn create_test_zone(
    zone_id: u32,
    segment_id: &str,
    context_ids: &[&str],
    user_ids: &[&str],
    timestamps: &[i64],
) -> CandidateZone {
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

    let mut zone = CandidateZone::new(zone_id, segment_id.to_string());
    zone.set_values(values_map);
    zone
}

#[test]
fn test_match_followed_by_basic() {
    // Setup: user1 has page_view(1000) -> order_created(1500)
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1000])];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    let order_zones = vec![create_test_zone(0, "seg1", &["ctx2"], &["user1"], &[1500])];
    zones_by_type.insert("order_created".to_string(), order_zones);

    // Group by user_id
    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    // Match sequences
    let sequence = EventSequence {
        head: EventTarget {
            event: "page_view".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::FollowedBy,
            EventTarget {
                event: "order_created".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 1 match
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].matched_rows.len(), 2);
}

#[test]
fn test_match_followed_by_no_match() {
    // Setup: user1 has order_created(1000) -> page_view(1500) (wrong order)
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1500])];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    let order_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx2"],
        &["user1"],
        &[1000], // Before page_view
    )];
    zones_by_type.insert("order_created".to_string(), order_zones);

    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    let sequence = EventSequence {
        head: EventTarget {
            event: "page_view".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::FollowedBy,
            EventTarget {
                event: "order_created".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 0 matches (order_created is before page_view)
    assert_eq!(matches.len(), 0);
}

#[test]
fn test_match_preceded_by_basic() {
    // Setup: user1 has payment_failed(1000) -> order_created(1500)
    let mut zones_by_type = HashMap::new();

    let payment_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1000])];
    zones_by_type.insert("payment_failed".to_string(), payment_zones);

    let order_zones = vec![create_test_zone(0, "seg1", &["ctx2"], &["user1"], &[1500])];
    zones_by_type.insert("order_created".to_string(), order_zones);

    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    let sequence = EventSequence {
        head: EventTarget {
            event: "order_created".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::PrecededBy,
            EventTarget {
                event: "payment_failed".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 1 match
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].matched_rows.len(), 2);
}

#[test]
fn test_match_with_limit() {
    // Setup: 3 users, all with matches
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx1", "ctx2", "ctx3"],
        &["user1", "user2", "user3"],
        &[1000, 2000, 3000],
    )];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    let order_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx4", "ctx5", "ctx6"],
        &["user1", "user2", "user3"],
        &[1500, 2500, 3500],
    )];
    zones_by_type.insert("order_created".to_string(), order_zones);

    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    let sequence = EventSequence {
        head: EventTarget {
            event: "page_view".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::FollowedBy,
            EventTarget {
                event: "order_created".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, Some(2));

    // Should find only 2 matches due to limit
    assert_eq!(matches.len(), 2);
}

#[test]
fn test_match_preceded_by_no_match_when_no_preceding_event() {
    // Setup: user1 has order_created(1500) but NO payment_failed
    // user2 has payment_failed(1000) -> order_created(1500)
    let mut zones_by_type = HashMap::new();

    let payment_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx1"],
        &["user2"], // Only user2 has payment_failed
        &[1000],
    )];
    zones_by_type.insert("payment_failed".to_string(), payment_zones);

    let order_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx2", "ctx3"],
        &["user1", "user2"], // Both users have order_created
        &[1500, 1500],
    )];
    zones_by_type.insert("order_created".to_string(), order_zones);

    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    let sequence = EventSequence {
        head: EventTarget {
            event: "order_created".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::PrecededBy,
            EventTarget {
                event: "payment_failed".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 1 match (only user2, not user1)
    assert_eq!(matches.len(), 1);
    // Verify it's user2's match
    let matched_user = &matches[0].link_value;
    assert!(format!("{:?}", matched_user).contains("user2"));
}

#[test]
fn test_match_preceded_by_same_timestamp_no_match() {
    // Setup: payment_failed and order_created have same timestamp
    // PRECEDED BY should require strict <, so same timestamp should not match
    let mut zones_by_type = HashMap::new();

    let payment_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1000])];
    zones_by_type.insert("payment_failed".to_string(), payment_zones);

    let order_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx2"],
        &["user1"],
        &[1000], // Same timestamp as payment_failed
    )];
    zones_by_type.insert("order_created".to_string(), order_zones);

    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    let sequence = EventSequence {
        head: EventTarget {
            event: "order_created".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::PrecededBy,
            EventTarget {
                event: "payment_failed".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string());
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 0 matches (same timestamp, not strictly preceded)
    assert_eq!(matches.len(), 0);
}

#[tokio::test]
async fn test_match_followed_by_with_where_clause() {
    // Test: page_view FOLLOWED BY order_created WHERE page_view.page = "/checkout"
    // This is the scenario from the failing integration test
    let mut zones_by_type = HashMap::new();

    // Create page_view zones: u1 has /checkout, u2 has /home
    let mut page_payload1 = HashMap::new();
    page_payload1.insert("page".to_string(), vec!["/checkout".to_string()]);
    let page_zone1 =
        create_test_zone_with_payload(0, "seg1", &["ctx1"], &["u1"], &[1000], &page_payload1);

    let mut page_payload2 = HashMap::new();
    page_payload2.insert("page".to_string(), vec!["/home".to_string()]);
    let page_zone2 =
        create_test_zone_with_payload(0, "seg1", &["ctx2"], &["u2"], &[1000], &page_payload2);

    let page_view_zones = vec![page_zone1, page_zone2];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    // Create order_created zones for both users
    let order_zones = vec![
        create_test_zone(0, "seg1", &["ctx3"], &["u1"], &[1500]),
        create_test_zone(0, "seg1", &["ctx4"], &["u2"], &[1500]),
    ];
    zones_by_type.insert("order_created".to_string(), order_zones);

    // Group by user_id
    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    // Create WHERE clause: page_view.page = "/checkout"
    let where_clause = crate::command::types::Expr::Compare {
        field: "page_view.page".to_string(),
        op: crate::command::types::CompareOp::Eq,
        value: serde_json::json!("/checkout"),
    };

    // Create WHERE evaluator
    let event_types = vec!["page_view".to_string(), "order_created".to_string()];
    let registry = create_test_registry(&[
        ("page_view", &["page"]),
        ("order_created", &["status"]),
    ]).await;
    let where_evaluator =
        crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator::new(
            Some(&where_clause),
            &event_types,
            &registry,
        )
        .await
        .expect("Should create evaluator");

    // Match sequences with WHERE clause
    let sequence = EventSequence {
        head: EventTarget {
            event: "page_view".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::FollowedBy,
            EventTarget {
                event: "order_created".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string()).with_where_evaluator(where_evaluator);
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 1 match (only u1, not u2)
    assert_eq!(matches.len(), 1, "Should find exactly 1 match for u1");

    // Verify it's u1's match
    let matched_user = &matches[0].link_value;
    assert!(
        format!("{:?}", matched_user).contains("u1"),
        "Should match u1, not u2"
    );
}

#[tokio::test]
async fn test_match_preceded_by_with_where_clause() {
    // Test: order_created PRECEDED BY payment_failed WHERE order_created.status = "done"
    let mut zones_by_type = HashMap::new();

    // Create payment_failed zones for both users
    let payment_zones = vec![
        create_test_zone(0, "seg1", &["ctx1"], &["u1"], &[1000]),
        create_test_zone(0, "seg1", &["ctx2"], &["u2"], &[1000]),
    ];
    zones_by_type.insert("payment_failed".to_string(), payment_zones);

    // Create order_created zones: u1 has status="done", u2 has status="pending"
    let mut order_payload1 = HashMap::new();
    order_payload1.insert("status".to_string(), vec!["done".to_string()]);
    let order_zone1 =
        create_test_zone_with_payload(0, "seg1", &["ctx3"], &["u1"], &[1500], &order_payload1);

    let mut order_payload2 = HashMap::new();
    order_payload2.insert("status".to_string(), vec!["pending".to_string()]);
    let order_zone2 =
        create_test_zone_with_payload(0, "seg1", &["ctx4"], &["u2"], &[1500], &order_payload2);

    let order_zones = vec![order_zone1, order_zone2];
    zones_by_type.insert("order_created".to_string(), order_zones);

    // Group by user_id
    let grouper = ColumnarGrouper::new("user_id".to_string(), "timestamp".to_string());
    let groups = grouper.group_zones_by_link_field(&zones_by_type);

    // Create WHERE clause: order_created.status = "done"
    let where_clause = crate::command::types::Expr::Compare {
        field: "order_created.status".to_string(),
        op: crate::command::types::CompareOp::Eq,
        value: serde_json::json!("done"),
    };

    // Create WHERE evaluator
    let event_types = vec!["payment_failed".to_string(), "order_created".to_string()];
    let registry = create_test_registry(&[
        ("payment_failed", &["amount"]), // payment_failed has amount but not status
        ("order_created", &["status"]),
    ]).await;
    let where_evaluator =
        crate::engine::core::read::sequence::where_evaluator::SequenceWhereEvaluator::new(
            Some(&where_clause),
            &event_types,
            &registry,
        )
        .await
        .expect("Should create evaluator");

    // Match sequences with WHERE clause
    let sequence = EventSequence {
        head: EventTarget {
            event: "order_created".to_string(),
            field: None,
        },
        links: vec![(
            SequenceLink::PrecededBy,
            EventTarget {
                event: "payment_failed".to_string(),
                field: None,
            },
        )],
    };

    let matcher = SequenceMatcher::new(sequence, "timestamp".to_string()).with_where_evaluator(where_evaluator);
    let matches = matcher.match_sequences(groups, &zones_by_type, None);

    // Should find 1 match (only u1, not u2)
    assert_eq!(matches.len(), 1, "Should find exactly 1 match for u1");

    // Verify it's u1's match
    let matched_user = &matches[0].link_value;
    assert!(
        format!("{:?}", matched_user).contains("u1"),
        "Should match u1, not u2"
    );
}

/// Helper to create test zone with payload fields
fn create_test_zone_with_payload(
    zone_id: u32,
    segment_id: &str,
    context_ids: &[&str],
    user_ids: &[&str],
    timestamps: &[i64],
    payload_fields: &std::collections::HashMap<String, Vec<String>>,
) -> CandidateZone {
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

    let mut zone = CandidateZone::new(zone_id, segment_id.to_string());
    zone.set_values(values_map);
    zone
}
