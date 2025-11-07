
use crate::engine::core::CandidateZone;
use crate::engine::core::read::sequence::group::RowIndex;
use crate::engine::core::read::sequence::matcher::MatchedSequenceIndices;
use crate::engine::core::read::sequence::materializer::SequenceMaterializer;
use crate::engine::types::ScalarValue;
use std::collections::HashMap;

/// Helper to create test zones (reuse pattern from group_test)
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
fn test_materialize_single_match() {
    // Create zones
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1000])];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    let order_zones = vec![create_test_zone(0, "seg1", &["ctx2"], &["user1"], &[1500])];
    zones_by_type.insert("order_created".to_string(), order_zones);

    // Create matched sequence indices
    let match_indices = MatchedSequenceIndices {
        link_value: ScalarValue::Utf8("user1".to_string()),
        matched_rows: vec![
            (
                "page_view".to_string(),
                RowIndex {
                    zone_idx: 0,
                    row_idx: 0,
                },
            ),
            (
                "order_created".to_string(),
                RowIndex {
                    zone_idx: 0,
                    row_idx: 0,
                },
            ),
        ],
    };

    // Materialize
    let materializer = SequenceMaterializer::new();
    let results = materializer.materialize_matches(vec![match_indices], &zones_by_type);

    // Should have 1 matched sequence with 2 events
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].events.len(), 2);
    assert_eq!(results[0].events[0].event_type, "page_view");
    assert_eq!(results[0].events[1].event_type, "order_created");
}

#[test]
fn test_materialize_multiple_matches() {
    // Create zones with multiple events
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx1", "ctx2"],
        &["user1", "user2"],
        &[1000, 2000],
    )];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    let order_zones = vec![create_test_zone(
        0,
        "seg1",
        &["ctx3", "ctx4"],
        &["user1", "user2"],
        &[1500, 2500],
    )];
    zones_by_type.insert("order_created".to_string(), order_zones);

    // Create two matched sequence indices
    let matches = vec![
        MatchedSequenceIndices {
            link_value: ScalarValue::Utf8("user1".to_string()),
            matched_rows: vec![
                (
                    "page_view".to_string(),
                    RowIndex {
                        zone_idx: 0,
                        row_idx: 0,
                    },
                ),
                (
                    "order_created".to_string(),
                    RowIndex {
                        zone_idx: 0,
                        row_idx: 0,
                    },
                ),
            ],
        },
        MatchedSequenceIndices {
            link_value: ScalarValue::Utf8("user2".to_string()),
            matched_rows: vec![
                (
                    "page_view".to_string(),
                    RowIndex {
                        zone_idx: 0,
                        row_idx: 1,
                    },
                ),
                (
                    "order_created".to_string(),
                    RowIndex {
                        zone_idx: 0,
                        row_idx: 1,
                    },
                ),
            ],
        },
    ];

    // Materialize
    let materializer = SequenceMaterializer::new();
    let results = materializer.materialize_matches(matches, &zones_by_type);

    // Should have 2 matched sequences
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].events.len(), 2);
    assert_eq!(results[1].events.len(), 2);
}

#[test]
fn test_materialize_handles_invalid_row_index() {
    // Create zones
    let mut zones_by_type = HashMap::new();

    let page_view_zones = vec![create_test_zone(0, "seg1", &["ctx1"], &["user1"], &[1000])];
    zones_by_type.insert("page_view".to_string(), page_view_zones);

    // Create match with invalid row index
    let match_indices = MatchedSequenceIndices {
        link_value: ScalarValue::Utf8("user1".to_string()),
        matched_rows: vec![
            (
                "page_view".to_string(),
                RowIndex {
                    zone_idx: 0,
                    row_idx: 999,
                },
            ), // Invalid
        ],
    };

    // Materialize
    let materializer = SequenceMaterializer::new();
    let results = materializer.materialize_matches(vec![match_indices], &zones_by_type);

    // Should skip invalid row and return empty sequence (filtered out)
    assert_eq!(results.len(), 0);
}
