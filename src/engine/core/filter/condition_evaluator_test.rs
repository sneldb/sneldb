use crate::engine::core::column::column_values::ColumnValues;
use crate::engine::core::filter::condition::CompareOp;
use crate::engine::core::filter::condition_evaluator::ConditionEvaluator;
use crate::engine::core::read::cache::DecompressedBlock;
use crate::engine::core::zone::candidate_zone::CandidateZone;
use crate::test_helpers::factories::CandidateZoneFactory;
use std::collections::HashMap;

fn make_u64_column(values: &[u64]) -> ColumnValues {
    // Build a fake typed-u64 ColumnValues backed by a single block
    let mut buf: Vec<u8> = Vec::with_capacity(values.len() * 8);
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    let block = DecompressedBlock::from_bytes(buf);
    ColumnValues::new_typed_u64(std::sync::Arc::new(block), 0, values.len(), None)
}

#[test]
fn numeric_condition_prefers_u64_and_matches() {
    // id column contains 1..=20 step 1
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3, 9, 10, 11, 12]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_numeric_condition("id".into(), CompareOp::Eq, 10);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 1, "should match exactly one row with id=10");
}

#[test]
fn numeric_condition_handles_u64_lt() {
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3, 4, 5]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_numeric_condition("id".into(), CompareOp::Lt, 3);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 2, "ids 1 and 2 are < 3");
}

#[test]
fn evaluates_zone_with_mixed_conditions() {
    // Zone with 3 events
    let mut values = HashMap::new();
    values.insert(
        "timestamp".to_string(),
        vec!["100".into(), "200".into(), "300".into()],
    );
    values.insert(
        "context_id".to_string(),
        vec!["ctx1".into(), "ctx2".into(), "ctx3".into()],
    );
    values.insert(
        "event_type".to_string(),
        vec!["login".into(), "login".into(), "logout".into()],
    );
    values.insert(
        "device".to_string(),
        vec!["ios".into(), "android".into(), "ios".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 42)
        .with("segment_id", "1")
        .with_values(values)
        .create();

    // Conditions: timestamp >= 200 AND device == ios
    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_numeric_condition("timestamp".into(), CompareOp::Gte, 200);
    evaluator.add_string_condition("device".into(), CompareOp::Eq, "ios".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    // Should match only the 3rd event (timestamp: 300, device: ios)
    assert_eq!(results.len(), 1);

    let event = &results[0];
    assert_eq!(event.get_field_value("timestamp"), "300");
    assert_eq!(event.get_field_value("device"), "ios");
    assert_eq!(event.get_field_value("context_id"), "ctx3");
}

#[test]
fn evaluate_row_at_works_with_accessor() {
    use crate::engine::core::filter::condition::PreparedAccessor;
    // Build a small zone-like columns map with two rows
    let mut values = HashMap::new();
    values.insert("timestamp".to_string(), vec!["100".into(), "200".into()]);
    values.insert("kind".to_string(), vec!["a".into(), "b".into()]);

    // Convert to ColumnValues via CandidateZoneFactory so PreparedAccessor can index
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 7)
        .with("segment_id", "seg")
        .with_values(values)
        .create();

    let accessor = PreparedAccessor::new(&zone.values);
    let mut evaluator = ConditionEvaluator::new();
    // timestamp >= 150 AND kind == b
    evaluator.add_numeric_condition("timestamp".into(), CompareOp::Gte, 150);
    evaluator.add_string_condition("kind".into(), CompareOp::Eq, "b".into());

    // Row 0 should fail (100, a)
    assert!(!evaluator.evaluate_row_at(&accessor, 0));
    // Row 1 should pass (200, b)
    assert!(evaluator.evaluate_row_at(&accessor, 1));
}

#[test]
fn evaluates_zones_with_limit_zero_returns_empty() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["ok".into(), "ok".into(), "ok".into()],
    );
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-a")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone], Some(0));
    assert_eq!(results.len(), 0);
}

#[test]
fn evaluates_zones_with_limit_one_across_multiple_zones() {
    // Zone A: 2 matches
    let mut a_vals = HashMap::new();
    a_vals.insert("kind".to_string(), vec!["x".into(), "x".into()]);
    let zone_a = CandidateZoneFactory::new()
        .with("zone_id", 10)
        .with("segment_id", "seg-a")
        .with_values(a_vals)
        .create();

    // Zone B: 3 matches
    let mut b_vals = HashMap::new();
    b_vals.insert("kind".to_string(), vec!["x".into(), "x".into(), "x".into()]);
    let zone_b = CandidateZoneFactory::new()
        .with("zone_id", 11)
        .with("segment_id", "seg-b")
        .with_values(b_vals)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("kind".into(), CompareOp::Eq, "x".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone_a, zone_b], Some(1));
    assert_eq!(results.len(), 1);
}

#[test]
fn evaluates_zones_with_limit_exact_boundary_stops_mid_zone() {
    // 5 rows, all matching; set limit to 3
    let mut values = HashMap::new();
    values.insert(
        "flag".to_string(),
        vec!["t".into(), "t".into(), "t".into(), "t".into(), "t".into()],
    );
    values.insert(
        "seq".to_string(),
        vec!["1".into(), "2".into(), "3".into(), "4".into(), "5".into()],
    );
    let zone = CandidateZoneFactory::new()
        .with("zone_id", 99)
        .with("segment_id", "seg-x")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("flag".into(), CompareOp::Eq, "t".into());

    let results = evaluator.evaluate_zones_with_limit(vec![zone], Some(3));
    assert_eq!(results.len(), 3);
    // Ensure first three rows in order were captured
    assert_eq!(results[0].get_field_value("seq"), "1");
    assert_eq!(results[1].get_field_value("seq"), "2");
    assert_eq!(results[2].get_field_value("seq"), "3");
}

#[test]
fn generates_synthetic_event_id_when_missing() {
    // Zone without event_id column
    let mut values = HashMap::new();
    values.insert("status".to_string(), vec!["ok".into(), "fail".into()]);
    values.insert("context_id".to_string(), vec!["ctx1".into(), "ctx2".into()]);
    values.insert("event_type".to_string(), vec!["test".into(), "test".into()]);

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 42)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    assert_eq!(results.len(), 1);
    let event = &results[0];

    // Should have synthetic ID: (zone_id << 32) | row_index
    let expected_id = (42u64 << 32) | 0u64;
    assert_eq!(event.event_id().raw(), expected_id);
    assert!(!event.event_id().is_zero());
}

#[test]
fn generates_synthetic_event_id_when_zero() {
    // Zone with event_id column containing zeros
    let mut values = HashMap::new();
    values.insert("status".to_string(), vec!["ok".into()]);
    values.insert("context_id".to_string(), vec!["ctx1".into()]);
    values.insert("event_type".to_string(), vec!["test".into()]);
    values.insert("event_id".to_string(), vec!["0".into()]); // Zero event_id

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 10)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    assert_eq!(results.len(), 1);
    let event = &results[0];

    // Should have synthetic ID instead of zero
    let expected_id = (10u64 << 32) | 0u64;
    assert_eq!(event.event_id().raw(), expected_id);
    assert!(!event.event_id().is_zero());
}

#[test]
fn preserves_valid_event_id_when_present() {
    // Zone with valid event_id column
    let mut values = HashMap::new();
    values.insert("status".to_string(), vec!["ok".into()]);
    values.insert("context_id".to_string(), vec!["ctx1".into()]);
    values.insert("event_type".to_string(), vec!["test".into()]);
    values.insert("event_id".to_string(), vec!["12345".into()]); // Valid event_id

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 10)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    assert_eq!(results.len(), 1);
    let event = &results[0];

    // Should preserve the original event_id
    assert_eq!(event.event_id().raw(), 12345);
}

#[test]
fn generates_unique_synthetic_ids_per_row() {
    // Zone with multiple rows, missing event_id
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["ok".into(), "ok".into(), "ok".into()],
    );
    values.insert(
        "context_id".to_string(),
        vec!["ctx1".into(), "ctx2".into(), "ctx3".into()],
    );
    values.insert(
        "event_type".to_string(),
        vec!["test".into(), "test".into(), "test".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 5)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones(vec![zone]);

    assert_eq!(results.len(), 3);

    // Each row should have a unique synthetic ID
    let ids: Vec<u64> = results.iter().map(|e| e.event_id().raw()).collect();
    assert_eq!(ids[0], (5u64 << 32) | 0u64);
    assert_eq!(ids[1], (5u64 << 32) | 1u64);
    assert_eq!(ids[2], (5u64 << 32) | 2u64);

    // All IDs should be unique
    let unique_ids: std::collections::HashSet<u64> = ids.iter().cloned().collect();
    assert_eq!(unique_ids.len(), 3);
}

#[test]
fn generates_synthetic_ids_across_multiple_zones() {
    // Two zones, both missing event_id
    let mut zone1_values = HashMap::new();
    zone1_values.insert("status".to_string(), vec!["ok".into()]);
    zone1_values.insert("context_id".to_string(), vec!["ctx1".into()]);
    zone1_values.insert("event_type".to_string(), vec!["test".into()]);

    let zone1 = CandidateZoneFactory::new()
        .with("zone_id", 10)
        .with("segment_id", "seg-1")
        .with_values(zone1_values)
        .create();

    let mut zone2_values = HashMap::new();
    zone2_values.insert("status".to_string(), vec!["ok".into()]);
    zone2_values.insert("context_id".to_string(), vec!["ctx2".into()]);
    zone2_values.insert("event_type".to_string(), vec!["test".into()]);

    let zone2 = CandidateZoneFactory::new()
        .with("zone_id", 20)
        .with("segment_id", "seg-2")
        .with_values(zone2_values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "ok".into());

    let results = evaluator.evaluate_zones(vec![zone1, zone2]);

    assert_eq!(results.len(), 2);

    // Each zone should generate unique IDs
    assert_eq!(results[0].event_id().raw(), (10u64 << 32) | 0u64);
    assert_eq!(results[1].event_id().raw(), (20u64 << 32) | 0u64);
}

// =============================================================================
// IN OPERATOR TESTS
// =============================================================================

fn make_i64_column(values: &[i64]) -> ColumnValues {
    // Build a fake typed-i64 ColumnValues backed by a single block
    let mut buf: Vec<u8> = Vec::with_capacity(values.len() * 8);
    for v in values {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    let block = DecompressedBlock::from_bytes(buf);
    ColumnValues::new_typed_i64(std::sync::Arc::new(block), 0, values.len(), None)
}

#[test]
fn in_numeric_condition_filters_u64_column() {
    // id column contains values: 1, 2, 3, 5, 7, 9, 10
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3, 5, 7, 9, 10]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![2, 5, 8, 10]);
    let out = ev.evaluate_zones(vec![zone]);
    // Should match: 2, 5, 10 (3 rows)
    assert_eq!(out.len(), 3, "should match rows with id in [2, 5, 8, 10]");
}

#[test]
fn in_numeric_condition_filters_i64_column() {
    // id column contains values: -5, -2, 0, 2, 5, 8
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_i64_column(&[-5, -2, 0, 2, 5, 8]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![-2, 2, 5, 9]);
    let out = ev.evaluate_zones(vec![zone]);
    // Should match: -2, 2, 5 (3 rows)
    assert_eq!(out.len(), 3, "should match rows with id in [-2, 2, 5, 9]");
}

#[test]
fn in_numeric_condition_no_matches() {
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![10, 20, 30]);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 0, "should match no rows when values not in list");
}

#[test]
fn in_numeric_condition_all_matches() {
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![1, 2, 3, 4, 5]);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(
        out.len(),
        3,
        "should match all rows when all values in list"
    );
}

#[test]
fn in_string_condition_filters_correctly() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec![
            "active".into(),
            "pending".into(),
            "completed".into(),
            "cancelled".into(),
        ],
    );
    values.insert(
        "context_id".to_string(),
        vec!["ctx1".into(), "ctx2".into(), "ctx3".into(), "ctx4".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut ev = ConditionEvaluator::new();
    ev.add_in_string_condition("status".into(), vec!["active".into(), "completed".into()]);
    let out = ev.evaluate_zones(vec![zone]);
    // Should match: active, completed (2 rows)
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get_field_value("status"), "active");
    assert_eq!(out[1].get_field_value("status"), "completed");
}

#[test]
fn in_string_condition_no_matches() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["pending".into(), "cancelled".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut ev = ConditionEvaluator::new();
    ev.add_in_string_condition("status".into(), vec!["active".into(), "completed".into()]);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 0);
}

#[test]
fn in_numeric_condition_with_limit() {
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3, 5, 7, 9, 10]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![2, 5, 8, 10]);
    let out = ev.evaluate_zones_with_limit(vec![zone], Some(2));
    // Should match only first 2: 2, 5
    assert_eq!(out.len(), 2);
}

#[test]
fn in_numeric_condition_with_evaluate_row_at() {
    use crate::engine::core::filter::condition::PreparedAccessor;
    let mut values = HashMap::new();
    values.insert(
        "id".to_string(),
        vec!["1".into(), "2".into(), "3".into(), "5".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let accessor = PreparedAccessor::new(&zone.values);
    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_in_numeric_condition("id".into(), vec![2, 5, 8]);

    // Row 0: id=1 -> should fail
    assert!(!evaluator.evaluate_row_at(&accessor, 0));
    // Row 1: id=2 -> should pass
    assert!(evaluator.evaluate_row_at(&accessor, 1));
    // Row 2: id=3 -> should fail
    assert!(!evaluator.evaluate_row_at(&accessor, 2));
    // Row 3: id=5 -> should pass
    assert!(evaluator.evaluate_row_at(&accessor, 3));
}

#[test]
fn in_string_condition_with_evaluate_row_at() {
    use crate::engine::core::filter::condition::PreparedAccessor;
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["active".into(), "pending".into(), "completed".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let accessor = PreparedAccessor::new(&zone.values);
    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_in_string_condition("status".into(), vec!["active".into(), "completed".into()]);

    // Row 0: status="active" -> should pass
    assert!(evaluator.evaluate_row_at(&accessor, 0));
    // Row 1: status="pending" -> should fail
    assert!(!evaluator.evaluate_row_at(&accessor, 1));
    // Row 2: status="completed" -> should pass
    assert!(evaluator.evaluate_row_at(&accessor, 2));
}

#[test]
fn in_numeric_condition_with_evaluate_event() {
    use crate::test_helpers::factories::EventFactory;
    use serde_json::json;

    let event1 = EventFactory::new()
        .with("payload", json!({ "id": 5 }))
        .create();

    let event2 = EventFactory::new()
        .with("payload", json!({ "id": 10 }))
        .create();

    let event3 = EventFactory::new()
        .with("payload", json!({ "id": 15 }))
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_in_numeric_condition("id".into(), vec![5, 10, 12]);

    assert!(evaluator.evaluate_event(&event1)); // id=5 in list
    assert!(evaluator.evaluate_event(&event2)); // id=10 in list
    assert!(!evaluator.evaluate_event(&event3)); // id=15 not in list
}

#[test]
fn in_string_condition_with_evaluate_event() {
    use crate::test_helpers::factories::EventFactory;
    use serde_json::json;

    let event1 = EventFactory::new()
        .with("payload", json!({ "status": "active" }))
        .create();

    let event2 = EventFactory::new()
        .with("payload", json!({ "status": "pending" }))
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_in_string_condition("status".into(), vec!["active".into(), "completed".into()]);

    assert!(evaluator.evaluate_event(&event1)); // status="active" in list
    assert!(!evaluator.evaluate_event(&event2)); // status="pending" not in list
}

#[test]
fn in_condition_combined_with_other_conditions() {
    let mut values = HashMap::new();
    values.insert(
        "id".to_string(),
        vec!["1".into(), "2".into(), "3".into(), "5".into(), "7".into()],
    );
    values.insert(
        "status".to_string(),
        vec![
            "active".into(),
            "active".into(),
            "pending".into(),
            "active".into(),
            "completed".into(),
        ],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    // id IN (2, 5, 8) AND status = "active"
    evaluator.add_in_numeric_condition("id".into(), vec![2, 5, 8]);
    evaluator.add_string_condition("status".into(), CompareOp::Eq, "active".into());

    let results = evaluator.evaluate_zones(vec![zone]);
    // Should match: id=2 with status="active", id=5 with status="active" (2 rows)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_field_value("id"), "2");
    assert_eq!(results[0].get_field_value("status"), "active");
    assert_eq!(results[1].get_field_value("id"), "5");
    assert_eq!(results[1].get_field_value("status"), "active");
}

#[test]
fn in_condition_across_multiple_zones() {
    // Zone 1: ids 1, 2, 3
    let mut zone1_values = HashMap::new();
    zone1_values.insert("id".to_string(), vec!["1".into(), "2".into(), "3".into()]);
    let zone1 = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(zone1_values)
        .create();

    // Zone 2: ids 5, 7, 9
    let mut zone2_values = HashMap::new();
    zone2_values.insert("id".to_string(), vec!["5".into(), "7".into(), "9".into()]);
    let zone2 = CandidateZoneFactory::new()
        .with("zone_id", 2)
        .with("segment_id", "seg-2")
        .with_values(zone2_values)
        .create();

    let mut evaluator = ConditionEvaluator::new();
    evaluator.add_in_numeric_condition("id".into(), vec![2, 5, 8, 10]);

    let results = evaluator.evaluate_zones(vec![zone1, zone2]);
    // Should match: id=2 from zone1, id=5 from zone2 (2 rows)
    assert_eq!(results.len(), 2);
}

#[test]
fn in_numeric_condition_single_value() {
    let mut zone = CandidateZone::new(0, "00000".into());
    let mut cols: HashMap<String, ColumnValues> = HashMap::new();
    cols.insert("id".into(), make_u64_column(&[1, 2, 3, 5]));
    zone.set_values(cols);

    let mut ev = ConditionEvaluator::new();
    ev.add_in_numeric_condition("id".into(), vec![2]);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 1, "should match exactly one row with id=2");
}

#[test]
fn in_string_condition_single_value() {
    let mut values = HashMap::new();
    values.insert(
        "status".to_string(),
        vec!["active".into(), "pending".into(), "active".into()],
    );

    let zone = CandidateZoneFactory::new()
        .with("zone_id", 1)
        .with("segment_id", "seg-1")
        .with_values(values)
        .create();

    let mut ev = ConditionEvaluator::new();
    ev.add_in_string_condition("status".into(), vec!["active".into()]);
    let out = ev.evaluate_zones(vec![zone]);
    assert_eq!(out.len(), 2, "should match 2 rows with status='active'");
}
