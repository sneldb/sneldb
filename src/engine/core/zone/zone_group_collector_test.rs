use crate::command::types::{Command, CompareOp};
use crate::engine::core::filter::filter_group::{filter_key, FilterGroup};
use crate::engine::core::read::query_plan::QueryPlan;
use crate::engine::core::zone::zone_group_collector::ZoneGroupCollector;
use crate::engine::core::{CandidateZone, QueryCaches};
use crate::engine::schema::registry::SchemaRegistry;
use crate::engine::types::ScalarValue;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock as StdRwLock};
use tempfile::TempDir;
use tokio::sync::RwLock;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Helper to create a filter for testing
fn create_test_filter(column: &str, op: CompareOp, value: ScalarValue) -> FilterGroup {
    FilterGroup::Filter {
        column: column.to_string(),
        operation: Some(op),
        value: Some(value),
        priority: 2,
        uid: None,
        index_strategy: None,
    }
}

/// Helper to create zones for testing
fn create_zones(zone_ids: &[u32], segment_id: &str) -> Vec<CandidateZone> {
    zone_ids.iter().map(|&id| CandidateZone::new(id, segment_id.to_string())).collect()
}

/// Helper to create a minimal QueryPlan for testing
/// Uses a static temp directory that persists for the test lifetime
fn create_test_plan() -> QueryPlan {
    create_test_plan_with_segments(vec!["seg1".to_string()])
}

/// Helper to create a QueryPlan with specific segments
fn create_test_plan_with_segments(segment_ids: Vec<String>) -> QueryPlan {
    // Create a temp directory that will be cleaned up when the test ends
    // We use a static to keep it alive, but in practice each test gets its own
    thread_local! {
        static TEMP_DIR: TempDir = tempfile::tempdir().expect("Failed to create temp dir");
    }

    let command = Command::Query {
        event_type: "test_event".to_string(),
        context_id: None,
        since: None,
        time_field: None,
        sequence_time_field: None,
        where_clause: None,
        limit: None,
        offset: None,
        order_by: None,
        picked_zones: None,
        return_fields: None,
        link_field: None,
        aggs: None,
        time_bucket: None,
        group_by: None,
        event_sequence: None,
    };

    TEMP_DIR.with(|tempdir| {
        let path = tempdir.path().join("schemas.bin");
        let registry = Arc::new(RwLock::new(
            SchemaRegistry::new_with_path(path).expect("Failed to initialize SchemaRegistry")
        ));
        let segment_base_dir = PathBuf::from("/tmp/test");
        let segment_ids_arc = Arc::new(StdRwLock::new(segment_ids));

        // Use tokio runtime to create plan synchronously
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(QueryPlan::new(command, &registry, &segment_base_dir, &segment_ids_arc))
            .unwrap()
    })
}

// ============================================================================
// EXTRACT UNIQUE FILTERS TESTS
// ============================================================================

#[test]
fn extract_unique_filters_deduplicates_same_filter() {
    // Create filter A
    let filter_a = create_test_filter(
        "status",
        CompareOp::Eq,
        ScalarValue::from(json!("active")),
    );

    // Create tree: (A AND A) OR A
    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![filter_a.clone(), filter_a.clone()]),
        filter_a.clone(),
    ]);

    // Extract unique filters - should only return one instance of A
    let unique = tree.extract_unique_filters();
    assert_eq!(unique.len(), 1, "Should deduplicate same filter");
    assert_eq!(unique[0].column(), Some("status"));
}

#[test]
fn extract_unique_filters_preserves_different_filters() {
    // Create different filters
    let filter_a = create_test_filter(
        "status",
        CompareOp::Eq,
        ScalarValue::from(json!("active")),
    );

    let filter_b = create_test_filter(
        "priority",
        CompareOp::Gt,
        ScalarValue::from(json!(5)),
    );

    let filter_c = create_test_filter(
        "status",
        CompareOp::Eq,
        ScalarValue::from(json!("pending")), // Different value
    );

    // Create tree: (A AND B) OR C
    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![filter_a.clone(), filter_b.clone()]),
        filter_c.clone(),
    ]);

    // Extract unique filters - should return all three
    let unique = tree.extract_unique_filters();
    assert_eq!(unique.len(), 3, "Should preserve different filters");

    let columns: Vec<Option<&str>> = unique.iter().map(|f| f.column()).collect();
    assert!(columns.contains(&Some("status")));
    assert!(columns.contains(&Some("priority")));
}

#[test]
fn extract_unique_filters_handles_in_expansion() {
    // Create tree with IN expansion: id IN(1,2,3)
    let filter_1 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_2 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(2)));
    let filter_3 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(3)));

    let tree = FilterGroup::Or(vec![filter_1, filter_2, filter_3]);

    let unique = tree.extract_unique_filters();
    assert_eq!(unique.len(), 3, "Should preserve all unique values from IN expansion");
}

#[test]
fn extract_unique_filters_handles_nested_duplicates() {
    // (A AND B) OR (A AND C) - A appears twice
    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("amount", CompareOp::Gte, ScalarValue::from(json!(100)));
    let filter_c = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(42)));

    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![filter_a.clone(), filter_b.clone()]),
        FilterGroup::And(vec![filter_a.clone(), filter_c.clone()]),
    ]);

    let unique = tree.extract_unique_filters();
    assert_eq!(unique.len(), 3, "Should deduplicate filter A");
}

// ============================================================================
// FILTER KEY TESTS
// ============================================================================

#[test]
fn filter_key_creates_unique_keys() {
    let key1 = filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active"))));
    let key2 = filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active"))));
    let key3 = filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("pending"))));
    let key4 = filter_key("priority", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active"))));

    assert_eq!(key1, key2, "Same filter should have same key");
    assert_ne!(key1, key3, "Different values should have different keys");
    assert_ne!(key1, key4, "Different columns should have different keys");
}

#[test]
fn filter_key_handles_none_operation() {
    let key1 = filter_key("field", &None, &Some(ScalarValue::from(json!("value"))));
    let key2 = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("value"))));

    assert_ne!(key1, key2, "None operation should differ from Eq operation");
}

#[test]
fn filter_key_handles_none_value() {
    let key1 = filter_key("field", &Some(CompareOp::Eq), &None);
    let key2 = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("value"))));

    assert_ne!(key1, key2, "None value should differ from Some value");
}

#[test]
fn filter_key_handles_all_operations() {
    let ops = vec![
        CompareOp::Eq,
        CompareOp::Neq,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::Lt,
        CompareOp::Lte,
    ];

    let mut keys = Vec::new();
    for op in ops {
        let key = filter_key("field", &Some(op), &Some(ScalarValue::from(json!(100))));
        keys.push(key);
    }

    // All keys should be unique
    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            assert_ne!(keys[i], keys[j], "Different operations should have different keys");
        }
    }
}

#[test]
fn filter_key_handles_special_scalar_values() {
    let key_null = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Null));
    let key_bool_true = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Boolean(true)));
    let key_bool_false = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Boolean(false)));
    let key_int = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Int64(42)));
    let key_float = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Float64(3.14)));

    // All should be different
    assert_ne!(key_null, key_bool_true);
    assert_ne!(key_bool_true, key_bool_false);
    assert_ne!(key_int, key_float);
}

#[test]
fn filter_key_handles_nan_float() {
    let key_nan = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Float64(f64::NAN)));
    let key_normal = filter_key("field", &Some(CompareOp::Eq), &Some(ScalarValue::Float64(3.14)));

    assert_ne!(key_nan, key_normal, "NaN should be handled specially");
    assert!(key_nan.contains("NaN"), "NaN key should contain 'NaN'");
}

// ============================================================================
// ZONE GROUP COLLECTOR TESTS - BASIC OPERATIONS
// ============================================================================

#[test]
fn zone_group_collector_combines_and_then_or() {
    // Create zones for filters A, B, C
    let zones_a = create_zones(&[1, 2, 3], "seg1");
    let zones_b = create_zones(&[2, 3, 4], "seg1");
    let zones_c = create_zones(&[3, 5], "seg1");

    // Build cache
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones_a,
    );
    cache.insert(
        filter_key("priority", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(5)))),
        zones_b,
    );
    cache.insert(
        filter_key("type", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("test")))),
        zones_c,
    );

    // Create tree: (A AND B) OR C
    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("priority", CompareOp::Gt, ScalarValue::from(json!(5)));
    let filter_c = create_test_filter("type", CompareOp::Eq, ScalarValue::from(json!("test")));

    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![filter_a, filter_b]),
        filter_c,
    ]);

    // Collect zones
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Expected: (A AND B) = [zone_2, zone_3], then OR C = [zone_2, zone_3, zone_5]
    let zone_ids: Vec<u32> = result.iter().map(|z| z.zone_id).collect();
    assert_eq!(zone_ids.len(), 3, "Should have 3 zones after deduplication");
    assert!(zone_ids.contains(&2), "Should include zone 2 from A AND B");
    assert!(zone_ids.contains(&3), "Should include zone 3 from A AND B and C");
    assert!(zone_ids.contains(&5), "Should include zone 5 from C");
}

#[test]
fn zone_group_collector_handles_single_filter() {
    let zones = create_zones(&[1, 2, 3], "seg1");

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones.clone(),
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&filter);

    assert_eq!(result.len(), zones.len());
    assert_eq!(result[0].zone_id, zones[0].zone_id);
}

#[test]
fn zone_group_collector_handles_missing_filter_in_cache() {
    let mut cache = HashMap::new();
    // Don't add filter to cache

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&filter);

    assert_eq!(result.len(), 0, "Missing filter should return empty zones");
}

#[test]
fn zone_group_collector_handles_duplicate_filters_in_tree() {
    // Create zones for filter A
    let zones_a = create_zones(&[1, 2], "seg1");

    // Build cache with single entry for A
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones_a.clone(),
    );

    // Create tree: (A AND A) OR A - filter A appears 3 times
    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));

    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![filter_a.clone(), filter_a.clone()]),
        filter_a.clone(),
    ]);

    // Collect zones - should use cache lookup (O(1)) for all occurrences
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Expected: (A AND A) = zones_a (intersection of same set), then OR A = zones_a
    assert_eq!(result.len(), zones_a.len(), "Should return zones from A");
}

#[test]
fn zone_group_collector_handles_empty_zones() {
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        vec![], // Empty zones
    );

    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("priority", CompareOp::Gt, ScalarValue::from(json!(5)));

    cache.insert(
        filter_key("priority", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(5)))),
        create_zones(&[1], "seg1"),
    );

    // (A AND B) where A has empty zones should return empty
    let tree = FilterGroup::And(vec![filter_a, filter_b]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "AND with empty zones should return empty");
}

#[test]
fn zone_group_collector_handles_all_empty_zones_in_or() {
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        vec![],
    );
    cache.insert(
        filter_key("priority", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(5)))),
        vec![],
    );

    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("priority", CompareOp::Gt, ScalarValue::from(json!(5)));

    let tree = FilterGroup::Or(vec![filter_a, filter_b]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "OR with all empty zones should return empty");
}

#[test]
fn zone_group_collector_deduplicates_after_operations() {
    // Create zones with duplicates
    let zones_a = vec![
        CandidateZone::new(1, "seg1".to_string()),
        CandidateZone::new(2, "seg1".to_string()),
        CandidateZone::new(2, "seg1".to_string()), // Duplicate
    ];

    let zones_b = vec![
        CandidateZone::new(2, "seg1".to_string()),
        CandidateZone::new(3, "seg1".to_string()),
        CandidateZone::new(2, "seg1".to_string()), // Duplicate
    ];

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones_a,
    );
    cache.insert(
        filter_key("priority", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(5)))),
        zones_b,
    );

    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("priority", CompareOp::Gt, ScalarValue::from(json!(5)));

    // A OR B should deduplicate zone_2
    let tree = FilterGroup::Or(vec![filter_a, filter_b]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Should have zone_1, zone_2, zone_3 (deduplicated)
    let zone_ids: Vec<u32> = result.iter().map(|z| z.zone_id).collect();
    assert_eq!(zone_ids.len(), 3, "Should deduplicate zones");
    assert!(zone_ids.contains(&1));
    assert!(zone_ids.contains(&2));
    assert!(zone_ids.contains(&3));
}

#[test]
fn zone_group_collector_handles_nested_and_or() {
    // Create zones
    let zones_a = create_zones(&[1], "seg1");
    let zones_b = create_zones(&[2], "seg1");
    let zones_c = create_zones(&[3], "seg1");
    let zones_d = create_zones(&[4], "seg1");

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("a", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        zones_a,
    );
    cache.insert(
        filter_key("b", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        zones_b,
    );
    cache.insert(
        filter_key("c", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(3)))),
        zones_c,
    );
    cache.insert(
        filter_key("d", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(4)))),
        zones_d,
    );

    // Create tree: ((A AND B) OR C) AND D
    let filter_a = create_test_filter("a", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_b = create_test_filter("b", CompareOp::Eq, ScalarValue::from(json!(2)));
    let filter_c = create_test_filter("c", CompareOp::Eq, ScalarValue::from(json!(3)));
    let filter_d = create_test_filter("d", CompareOp::Eq, ScalarValue::from(json!(4)));

    let tree = FilterGroup::And(vec![
        FilterGroup::Or(vec![
            FilterGroup::And(vec![filter_a, filter_b]),
            filter_c,
        ]),
        filter_d,
    ]);

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Expected: (A AND B) = empty (no intersection), OR C = [zone_3], AND D = empty (no intersection)
    assert_eq!(result.len(), 0, "Complex nested query should handle correctly");
}

#[test]
fn zone_group_collector_handles_cross_segment_zones() {
    // Zones from different segments - AND should intersect by both zone_id AND segment_id
    let zones_a = vec![
        CandidateZone::new(1, "seg1".to_string()),
        CandidateZone::new(2, "seg1".to_string()), // zone 2 in seg1
        CandidateZone::new(2, "seg2".to_string()),
    ];

    let zones_b = vec![
        CandidateZone::new(2, "seg1".to_string()), // zone 2 in seg1 - matches!
        CandidateZone::new(3, "seg2".to_string()),
    ];

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones_a,
    );
    cache.insert(
        filter_key("priority", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(5)))),
        zones_b,
    );

    let filter_a = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_b = create_test_filter("priority", CompareOp::Gt, ScalarValue::from(json!(5)));

    // A AND B - should intersect by both zone_id AND segment_id
    // zones_a: [(1, seg1), (2, seg1), (2, seg2)]
    // zones_b: [(2, seg1), (3, seg2)]
    // Intersection: (2, seg1) - zone 2 in seg1 exists in both
    let tree = FilterGroup::And(vec![filter_a, filter_b]);
    let plan = create_test_plan_with_segments(vec!["seg1".to_string(), "seg2".to_string()]);
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Should have zone_2 from seg1 (only intersection)
    assert_eq!(result.len(), 1, "AND should intersect zones by both zone_id and segment_id");
    assert_eq!(result[0].zone_id, 2);
    assert_eq!(result[0].segment_id, "seg1");
}

#[test]
fn zone_group_collector_handles_not_operation() {
    // Smart NOT: computes complement (all zones minus matching zones)
    // fill_factor = 3, so all zones for seg1 are [0, 1, 2]
    // Matching zones are [1, 2], so NOT should return [0]
    let matching_zones = create_zones(&[1, 2], "seg1");

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        matching_zones.clone(),
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let tree = FilterGroup::Not(Box::new(filter));

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // NOT should return complement: all zones [0, 1, 2] - matching [1, 2] = [0]
    assert_eq!(result.len(), 1, "NOT should return complement zones");
    assert_eq!(result[0].zone_id, 0, "Should return zone 0 (not in matching zones)");
    assert_eq!(result[0].segment_id, "seg1");
}

#[test]
fn zone_group_collector_handles_nested_not() {
    // NOT (NOT A) -> double negation should return A
    // fill_factor = 3, so all zones for seg1 are [0, 1, 2]
    // Matching zones are [1, 2], so NOT A = [0], then NOT (NOT A) = NOT [0] = [1, 2]
    let matching_zones = create_zones(&[1, 2], "seg1");

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        matching_zones.clone(),
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let tree = FilterGroup::Not(Box::new(FilterGroup::Not(Box::new(filter))));

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Double negation should return original matching zones
    assert_eq!(result.len(), matching_zones.len());
    assert_eq!(result[0].zone_id, 1);
    assert_eq!(result[1].zone_id, 2);
}

#[test]
fn zone_group_collector_handles_large_or_with_many_filters() {
    // A OR B OR C OR D OR E
    let mut cache = HashMap::new();
    let filters = vec!["a", "b", "c", "d", "e"];

    for (i, field) in filters.iter().enumerate() {
        cache.insert(
            filter_key(field, &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(i)))),
            create_zones(&[i as u32, (i + 10) as u32], "seg1"),
        );
    }

    let filter_group = FilterGroup::Or(
        filters.iter().enumerate().map(|(i, field)| {
            create_test_filter(field, CompareOp::Eq, ScalarValue::from(json!(i)))
        }).collect()
    );

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&filter_group);

    // Should union all zones: [0,10,1,11,2,12,3,13,4,14] = 10 zones
    assert_eq!(result.len(), 10);
}

#[test]
fn zone_group_collector_handles_large_and_with_many_filters() {
    // A AND B AND C AND D AND E - all intersect at zone 10
    let mut cache = HashMap::new();
    let filters = vec!["a", "b", "c", "d", "e"];

    for field in &filters {
        cache.insert(
            filter_key(field, &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
            create_zones(&[10], "seg1"), // All have only zone 10 for intersection
        );
    }

    let filter_group = FilterGroup::And(
        filters.iter().map(|field| {
            create_test_filter(field, CompareOp::Eq, ScalarValue::from(json!(1)))
        }).collect()
    );

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&filter_group);

    // Should intersect: only zone 10
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].zone_id, 10);
}

#[test]
fn zone_group_collector_handles_and_with_no_intersection() {
    // A AND B where they don't intersect
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("a", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        create_zones(&[1, 2], "seg1"),
    );
    cache.insert(
        filter_key("b", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        create_zones(&[3, 4], "seg1"), // No intersection
    );

    let filter_a = create_test_filter("a", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_b = create_test_filter("b", CompareOp::Eq, ScalarValue::from(json!(2)));

    let tree = FilterGroup::And(vec![filter_a, filter_b]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "AND with no intersection should return empty");
}

#[test]
fn zone_group_collector_handles_or_with_in_expansion() {
    // id IN(1,2,3) OR status = "active"
    // This simulates what happens after IN expansion
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        create_zones(&[1], "seg1"),
    );
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        create_zones(&[2], "seg1"),
    );
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(3)))),
        create_zones(&[3], "seg1"),
    );
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        create_zones(&[4], "seg1"),
    );

    // Simulate expanded IN: id=1 OR id=2 OR id=3 OR status="active"
    let filter_1 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_2 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(2)));
    let filter_3 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(3)));
    let filter_status = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));

    let tree = FilterGroup::Or(vec![filter_1, filter_2, filter_3, filter_status]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Should union all: [1, 2, 3, 4]
    assert_eq!(result.len(), 4);
    let zone_ids: Vec<u32> = result.iter().map(|z| z.zone_id).collect();
    assert!(zone_ids.contains(&1));
    assert!(zone_ids.contains(&2));
    assert!(zone_ids.contains(&3));
    assert!(zone_ids.contains(&4));
}

#[test]
fn zone_group_collector_handles_complex_nested_with_in_expansion() {
    // (id IN(1,2) AND status="active") OR id IN(3,4)
    // After expansion: ((id=1 OR id=2) AND status="active") OR (id=3 OR id=4)
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        create_zones(&[1, 10], "seg1"),
    );
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        create_zones(&[2, 10], "seg1"), // Intersects with id=1 at zone 10
    );
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        create_zones(&[10, 20], "seg1"), // Intersects with id=1 and id=2 at zone 10
    );
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(3)))),
        create_zones(&[3], "seg1"),
    );
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(4)))),
        create_zones(&[4], "seg1"),
    );

    // Build tree: ((id=1 OR id=2) AND status="active") OR (id=3 OR id=4)
    let filter_id1 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_id2 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(2)));
    let filter_status = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_id3 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(3)));
    let filter_id4 = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(4)));

    let tree = FilterGroup::Or(vec![
        FilterGroup::And(vec![
            FilterGroup::Or(vec![filter_id1, filter_id2]),
            filter_status,
        ]),
        FilterGroup::Or(vec![filter_id3, filter_id4]),
    ]);

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Expected: ((id=1 OR id=2) AND status) = [zone_10], OR (id=3 OR id=4) = [zone_3, zone_4]
    // Final: [zone_10, zone_3, zone_4]
    assert_eq!(result.len(), 3);
    let zone_ids: Vec<u32> = result.iter().map(|z| z.zone_id).collect();
    assert!(zone_ids.contains(&10));
    assert!(zone_ids.contains(&3));
    assert!(zone_ids.contains(&4));
}

#[test]
fn zone_group_collector_handles_empty_filter_group() {
    // Empty AND group
    let tree = FilterGroup::And(vec![]);
    let mut cache = HashMap::new();
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "Empty AND group should return empty");
}

#[test]
fn zone_group_collector_handles_empty_or_group() {
    // Empty OR group
    let tree = FilterGroup::Or(vec![]);
    let mut cache = HashMap::new();
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "Empty OR group should return empty");
}

#[test]
fn zone_group_collector_handles_single_child_and() {
    // AND with single child
    let zones = create_zones(&[1, 2], "seg1");
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones.clone(),
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let tree = FilterGroup::And(vec![filter]);

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), zones.len());
}

#[test]
fn zone_group_collector_handles_single_child_or() {
    // OR with single child
    let zones = create_zones(&[1, 2], "seg1");
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones.clone(),
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let tree = FilterGroup::Or(vec![filter]);

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), zones.len());
}

#[test]
fn zone_group_collector_handles_very_large_zone_sets() {
    // Test with many zones to ensure performance
    let zones: Vec<CandidateZone> = (0..1000)
        .map(|i| CandidateZone::new(i, "seg1".to_string()))
        .collect();

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        zones,
    );

    let filter = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&filter);

    assert_eq!(result.len(), 1000);
}

#[test]
fn zone_group_collector_handles_multiple_segments() {
    // Zones from multiple segments
    let zones_a = vec![
        CandidateZone::new(1, "seg1".to_string()),
        CandidateZone::new(2, "seg2".to_string()),
    ];
    let zones_b = vec![
        CandidateZone::new(2, "seg1".to_string()),
        CandidateZone::new(3, "seg3".to_string()),
    ];

    let mut cache = HashMap::new();
    cache.insert(
        filter_key("a", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        zones_a,
    );
    cache.insert(
        filter_key("b", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        zones_b,
    );

    let filter_a = create_test_filter("a", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_b = create_test_filter("b", CompareOp::Eq, ScalarValue::from(json!(2)));

    // A OR B should union across segments
    let tree = FilterGroup::Or(vec![filter_a, filter_b]);
    let plan = create_test_plan_with_segments(vec!["seg1".to_string(), "seg2".to_string(), "seg3".to_string()]);
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 4); // All zones from both filters
    let segments: Vec<&str> = result.iter().map(|z| z.segment_id.as_str()).collect();
    assert!(segments.contains(&"seg1"));
    assert!(segments.contains(&"seg2"));
    assert!(segments.contains(&"seg3"));
}

#[test]
fn zone_group_collector_handles_and_early_exit_optimization() {
    // A AND B where A has empty zones - should early exit
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("a", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        vec![], // Empty
    );
    cache.insert(
        filter_key("b", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        create_zones(&[1, 2, 3], "seg1"),
    );

    let filter_a = create_test_filter("a", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_b = create_test_filter("b", CompareOp::Eq, ScalarValue::from(json!(2)));

    let tree = FilterGroup::And(vec![filter_a, filter_b]);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "Should early exit on empty zones in AND");
}

#[test]
fn zone_group_collector_handles_and_with_many_children_early_exit() {
    // A AND B AND C AND D where B has empty zones
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("a", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        create_zones(&[1, 2], "seg1"),
    );
    cache.insert(
        filter_key("b", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(2)))),
        vec![], // Empty - should trigger early exit
    );
    cache.insert(
        filter_key("c", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(3)))),
        create_zones(&[1, 2], "seg1"),
    );
    cache.insert(
        filter_key("d", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(4)))),
        create_zones(&[1, 2], "seg1"),
    );

    let filters = vec![
        create_test_filter("a", CompareOp::Eq, ScalarValue::from(json!(1))),
        create_test_filter("b", CompareOp::Eq, ScalarValue::from(json!(2))),
        create_test_filter("c", CompareOp::Eq, ScalarValue::from(json!(3))),
        create_test_filter("d", CompareOp::Eq, ScalarValue::from(json!(4))),
    ];

    let tree = FilterGroup::And(filters);
    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    assert_eq!(result.len(), 0, "Should early exit and not process remaining filters");
}

#[test]
fn zone_group_collector_handles_nested_or_with_different_fields() {
    // Test the case: (id = 1 OR status = "active") OR amount > 100
    // This verifies that preserved nested OR structures (with different fields) are handled correctly
    let mut cache = HashMap::new();
    cache.insert(
        filter_key("id", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!(1)))),
        create_zones(&[1, 5], "seg1"),
    );
    cache.insert(
        filter_key("status", &Some(CompareOp::Eq), &Some(ScalarValue::from(json!("active")))),
        create_zones(&[2, 5], "seg1"), // Intersects with id=1 at zone 5
    );
    cache.insert(
        filter_key("amount", &Some(CompareOp::Gt), &Some(ScalarValue::from(json!(100)))),
        create_zones(&[3, 6], "seg1"),
    );

    // Build tree: ((id=1 OR status="active") OR amount>100)
    // The nested OR should be preserved (not flattened) because fields differ
    let filter_id = create_test_filter("id", CompareOp::Eq, ScalarValue::from(json!(1)));
    let filter_status = create_test_filter("status", CompareOp::Eq, ScalarValue::from(json!("active")));
    let filter_amount = create_test_filter("amount", CompareOp::Gt, ScalarValue::from(json!(100)));

    let tree = FilterGroup::Or(vec![
        FilterGroup::Or(vec![filter_id, filter_status]), // Nested OR with different fields
        filter_amount,
    ]);

    let plan = create_test_plan();
    let collector = ZoneGroupCollector::new(cache, &plan, None);
    let result = collector.collect_zones_from_group(&tree);

    // Expected: (id=1 OR status="active") = [zone_1, zone_2, zone_5] (union)
    // Then OR amount>100 = [zone_1, zone_2, zone_3, zone_5, zone_6] (union)
    assert_eq!(result.len(), 5, "Should correctly process preserved nested OR structure");
    let zone_ids: Vec<u32> = result.iter().map(|z| z.zone_id).collect();
    assert!(zone_ids.contains(&1), "Should include zone from id=1");
    assert!(zone_ids.contains(&2), "Should include zone from status='active'");
    assert!(zone_ids.contains(&3), "Should include zone from amount>100");
    assert!(zone_ids.contains(&5), "Should include intersection zone");
    assert!(zone_ids.contains(&6), "Should include zone from amount>100");
}

