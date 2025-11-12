use std::sync::Arc;

use super::watermark::WatermarkDeduplicator;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::HighWaterMark;
use crate::engine::types::ScalarValue;
use serde_json::json;

fn build_batch(timestamps: &[u64], event_ids: &[u64]) -> Arc<ColumnBatch> {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
        ])
        .expect("schema"),
    );

    let timestamp_column: Vec<ScalarValue> = timestamps
        .iter()
        .map(|ts| ScalarValue::from(json!(ts)))
        .collect();
    let event_column: Vec<ScalarValue> = event_ids
        .iter()
        .map(|id| ScalarValue::from(json!(id)))
        .collect();

    Arc::new(
        ColumnBatch::new(
            schema,
            vec![timestamp_column, event_column],
            timestamps.len(),
            None,
        )
        .expect("batch"),
    )
}

fn build_batch_with_context_id(
    timestamps: &[u64],
    event_ids: &[u64],
    context_ids: &[&str],
) -> Arc<ColumnBatch> {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "context_id".to_string(),
                logical_type: "String".to_string(),
            },
        ])
        .expect("schema"),
    );

    let timestamp_column: Vec<ScalarValue> = timestamps
        .iter()
        .map(|ts| ScalarValue::from(json!(ts)))
        .collect();
    let event_column: Vec<ScalarValue> = event_ids
        .iter()
        .map(|id| ScalarValue::from(json!(id)))
        .collect();
    let context_column: Vec<ScalarValue> = context_ids
        .iter()
        .map(|cid| ScalarValue::from(json!(cid)))
        .collect();

    Arc::new(
        ColumnBatch::new(
            schema,
            vec![timestamp_column, event_column, context_column],
            timestamps.len(),
            None,
        )
        .expect("batch"),
    )
}

fn build_batch_with_missing_values(
    timestamps: &[Option<u64>],
    event_ids: &[Option<u64>],
) -> Arc<ColumnBatch> {
    let schema = Arc::new(
        BatchSchema::new(vec![
            ColumnSpec {
                name: "timestamp".to_string(),
                logical_type: "Number".to_string(),
            },
            ColumnSpec {
                name: "event_id".to_string(),
                logical_type: "Number".to_string(),
            },
        ])
        .expect("schema"),
    );

    let timestamp_column: Vec<ScalarValue> = timestamps
        .iter()
        .map(|ts| {
            ts.map(|v| ScalarValue::from(json!(v)))
                .unwrap_or_else(|| ScalarValue::Null)
        })
        .collect();
    let event_column: Vec<ScalarValue> = event_ids
        .iter()
        .map(|id| {
            id.map(|v| ScalarValue::from(json!(v)))
                .unwrap_or_else(|| ScalarValue::Null)
        })
        .collect();

    Arc::new(
        ColumnBatch::new(
            schema,
            vec![timestamp_column, event_column],
            timestamps.len(),
            None,
        )
        .expect("batch"),
    )
}

// ==================== Basic Functionality Tests ====================

#[test]
fn filters_rows_beyond_watermark() {
    let batch = build_batch(&[100, 150, 90], &[10, 20, 30]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    assert_eq!(filtered.len(), 1);

    // Verify only row with (150, 20) is kept (greater than watermark (100, 10))
    let filtered_ts = filtered.column(0).expect("timestamp column");
    let filtered_events = filtered.column(1).expect("event_id column");
    assert_eq!(filtered_ts[0].as_u64(), Some(150));
    assert_eq!(filtered_events[0].as_u64(), Some(20));
}

#[test]
fn filters_rows_using_tuple_comparison() {
    // Test that (timestamp, event_id) tuple comparison works correctly
    // Row (100, 20) should pass (same timestamp but higher event_id)
    // Row (100, 5) should fail (same timestamp but lower event_id)
    let batch = build_batch(&[100, 100, 100], &[5, 20, 10]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    assert_eq!(filtered.len(), 1); // Only (100, 20) should pass

    let filtered_events = filtered.column(1).expect("event_id column");
    assert_eq!(filtered_events[0].as_u64(), Some(20));
}

#[test]
fn returns_none_when_all_rows_filtered() {
    let batch = build_batch(&[50, 75], &[1, 2]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    assert!(dedup.filter(batch).is_none());
}

#[test]
fn returns_all_rows_when_all_pass() {
    let batch = build_batch(&[150, 200, 300], &[20, 30, 40]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch.clone());
    assert!(filtered.is_some());
    let filtered = filtered.unwrap();

    // When all rows pass, should return original batch (no filtering needed)
    assert_eq!(filtered.len(), 3);
}

#[test]
fn returns_none_for_empty_batch() {
    let batch = build_batch(&[], &[]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    assert!(dedup.filter(batch).is_none());
}

// ==================== Edge Cases: Missing Indices ====================

#[test]
fn no_filter_when_timestamp_index_missing() {
    let batch = build_batch(&[10], &[1]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(0, 0), None, Some(1));

    let original = Arc::clone(&batch);
    let result = dedup.filter(batch).expect("batch");
    assert!(Arc::ptr_eq(&result, &original));
}

#[test]
fn no_filter_when_event_id_index_missing() {
    let batch = build_batch(&[10], &[1]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(0, 0), Some(0), None);

    let original = Arc::clone(&batch);
    let result = dedup.filter(batch).expect("batch");
    assert!(Arc::ptr_eq(&result, &original));
}

#[test]
fn no_filter_when_both_indices_missing() {
    let batch = build_batch(&[10], &[1]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(0, 0), None, None);

    let original = Arc::clone(&batch);
    let result = dedup.filter(batch).expect("batch");
    assert!(Arc::ptr_eq(&result, &original));
}

#[test]
fn enabled_returns_false_when_indices_missing() {
    let dedup1 = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), None, Some(1));
    assert!(!dedup1.enabled());

    let dedup2 = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), None);
    assert!(!dedup2.enabled());

    let dedup3 = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), None, None);
    assert!(!dedup3.enabled());
}

#[test]
fn enabled_returns_true_when_both_indices_present() {
    let dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));
    assert!(dedup.enabled());
}

// ==================== Edge Cases: Missing Values ====================

#[test]
fn keeps_rows_with_missing_timestamp() {
    let batch = build_batch_with_missing_values(&[None, Some(150)], &[Some(10), Some(20)]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Both rows should be kept (missing timestamp means we can't filter)
    assert_eq!(filtered.len(), 2);
}

#[test]
fn keeps_rows_with_missing_event_id() {
    let batch = build_batch_with_missing_values(&[Some(150), Some(200)], &[None, Some(20)]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Both rows should be kept (missing event_id means we can't filter)
    assert_eq!(filtered.len(), 2);
}

#[test]
fn keeps_rows_with_both_missing() {
    let batch = build_batch_with_missing_values(&[None, Some(150)], &[Some(10), None]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Both rows should be kept (missing values mean we can't filter)
    assert_eq!(filtered.len(), 2);
}

// ==================== Watermark Advancement Tests ====================

#[test]
fn advances_watermark_to_max_in_batch() {
    let batch1 = build_batch(&[100, 150, 120], &[10, 20, 15]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(90, 5), Some(0), Some(1));

    let _filtered1 = dedup.filter(batch1);

    // Watermark should advance to max in batch: (150, 20)
    assert_eq!(dedup.watermark().timestamp, 150);
    assert_eq!(dedup.watermark().event_id, 20);
}

#[test]
fn advances_watermark_including_filtered_rows() {
    // Even if rows are filtered out, watermark should advance to max in ALL rows
    let batch = build_batch(&[50, 150, 75], &[1, 20, 5]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch);
    assert!(filtered.is_some());

    // Watermark should advance to (150, 20) even though (50, 1) and (75, 5) were filtered
    assert_eq!(dedup.watermark().timestamp, 150);
    assert_eq!(dedup.watermark().event_id, 20);
}

#[test]
fn uses_initial_watermark_for_filtering_not_advancing() {
    // Test that filtering uses initial watermark, not the advancing one
    let batch1 = build_batch(&[100, 150], &[10, 20]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(90, 5), Some(0), Some(1));

    let filtered1 = dedup.filter(batch1);
    assert!(filtered1.is_some());
    assert_eq!(filtered1.unwrap().len(), 2); // Both pass initial watermark (90, 5)

    // Watermark advances to (150, 20)
    assert_eq!(dedup.watermark().timestamp, 150);
    assert_eq!(dedup.watermark().event_id, 20);

    // Second batch: rows between initial and advanced watermark
    // Should still use initial watermark (90, 5) for filtering
    let batch2 = build_batch(&[95, 140], &[8, 15]);
    let filtered2 = dedup.filter(batch2);
    assert!(filtered2.is_some());
    // Both rows (95, 8) and (140, 15) are > initial watermark (90, 5)
    assert_eq!(filtered2.unwrap().len(), 2);
}

#[test]
fn watermark_advances_with_missing_values() {
    // Batch with some missing values - only rows with both timestamp and event_id are considered
    // Row 0: (100, 10) - both valid
    // Row 1: (None, 20) - timestamp missing, not considered for advancement
    // Row 2: (150, None) - event_id missing, not considered for advancement
    let batch = build_batch_with_missing_values(&[Some(100), None, Some(150)], &[Some(10), Some(20), None]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(90, 5), Some(0), Some(1));

    let _filtered = dedup.filter(batch);

    // Watermark should advance to max valid pair: (100, 10) from row 0
    // Rows with missing values are not considered for watermark advancement
    assert_eq!(dedup.watermark().timestamp, 100);
    assert_eq!(dedup.watermark().event_id, 10);
}

#[test]
fn watermark_advances_with_partial_missing_values() {
    // Test that watermark advances correctly when some rows have both values
    let batch = build_batch_with_missing_values(&[Some(100), Some(150), None], &[Some(10), Some(20), Some(30)]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(90, 5), Some(0), Some(1));

    let _filtered = dedup.filter(batch);

    // Watermark should advance to max valid pair: (150, 20) from row 1
    // Row 2 has missing timestamp, so not considered
    assert_eq!(dedup.watermark().timestamp, 150);
    assert_eq!(dedup.watermark().event_id, 20);
}

#[test]
fn watermark_does_not_advance_below_initial() {
    let batch = build_batch(&[50, 75], &[1, 2]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let _filtered = dedup.filter(batch);

    // Watermark should not go below initial (100, 10)
    // Since all rows are filtered, watermark stays at initial
    assert_eq!(dedup.watermark().timestamp, 100);
    assert_eq!(dedup.watermark().event_id, 10);
}

// ==================== Multiple Batch Tests ====================

#[test]
fn handles_multiple_batches_sequentially() {
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    // First batch: some pass, some fail
    let batch1 = build_batch(&[90, 150, 120], &[5, 20, 15]);
    let filtered1 = dedup.filter(batch1).expect("filtered batch");
    assert_eq!(filtered1.len(), 2); // (150, 20) and (120, 15) pass
    assert_eq!(dedup.watermark().timestamp, 150);
    assert_eq!(dedup.watermark().event_id, 20);

    // Second batch: uses initial watermark (100, 10) for filtering
    let batch2 = build_batch(&[110, 95, 200], &[12, 8, 25]);
    let filtered2 = dedup.filter(batch2).expect("filtered batch");
    // (110, 12) and (200, 25) pass, (95, 8) fails
    assert_eq!(filtered2.len(), 2);

    // Watermark advances to max: (200, 25)
    assert_eq!(dedup.watermark().timestamp, 200);
    assert_eq!(dedup.watermark().event_id, 25);
}

#[test]
fn handles_out_of_order_batches() {
    // Test that out-of-order batches don't incorrectly filter rows
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    // First batch: high values
    let batch1 = build_batch(&[200, 250], &[30, 40]);
    let _filtered1 = dedup.filter(batch1);
    assert_eq!(dedup.watermark().timestamp, 250);
    assert_eq!(dedup.watermark().event_id, 40);

    // Second batch: lower values (out of order)
    // Should still use initial watermark (100, 10) for filtering
    let batch2 = build_batch(&[150, 180], &[15, 20]);
    let filtered2 = dedup.filter(batch2).expect("filtered batch");
    // Both rows pass initial watermark (100, 10)
    assert_eq!(filtered2.len(), 2);
}

// ==================== Error Handling Tests ====================

#[test]
fn handles_missing_timestamp_column_gracefully() {
    // Create batch without timestamp column
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "event_id".to_string(),
            logical_type: "Number".to_string(),
        }])
        .expect("schema"),
    );

    let batch = Arc::new(
        ColumnBatch::new(
            schema,
            vec![vec![ScalarValue::from(json!(10))]],
            1,
            None,
        )
        .expect("batch"),
    );

    // Use None for timestamp_idx since column doesn't exist
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), None, Some(0));

    // Should return batch unchanged (can't filter without timestamp)
    let result = dedup.filter(batch.clone());
    assert!(result.is_some());

    // Verify it's the same batch (no filtering occurred)
    let original = Arc::clone(&batch);
    assert!(Arc::ptr_eq(&result.unwrap(), &original));
}

#[test]
fn handles_missing_event_id_column_gracefully() {
    // Create batch without event_id column
    let schema = Arc::new(
        BatchSchema::new(vec![ColumnSpec {
            name: "timestamp".to_string(),
            logical_type: "Number".to_string(),
        }])
        .expect("schema"),
    );

    let batch = Arc::new(
        ColumnBatch::new(
            schema,
            vec![vec![ScalarValue::from(json!(150))]],
            1,
            None,
        )
        .expect("batch"),
    );

    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(0));

    // Should return batch unchanged (can't filter without event_id)
    let result = dedup.filter(batch.clone());
    assert!(result.is_some());
}

// ==================== Boundary Condition Tests ====================

#[test]
fn filters_rows_equal_to_watermark() {
    // Rows equal to watermark should be filtered (we want strictly greater)
    let batch = build_batch(&[100, 100, 100], &[10, 10, 10]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch);
    assert!(filtered.is_none()); // All rows equal to watermark, all filtered
}

#[test]
fn filters_rows_with_same_timestamp_but_lower_event_id() {
    let batch = build_batch(&[100, 100], &[5, 15]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    assert_eq!(filtered.len(), 1); // Only (100, 15) passes
    assert_eq!(filtered.column(1).unwrap()[0].as_u64(), Some(15));
}

#[test]
fn filters_rows_with_lower_timestamp_but_higher_event_id() {
    let batch = build_batch(&[90, 110], &[20, 5]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    assert_eq!(filtered.len(), 1); // Only (110, 5) passes (tuple comparison)
    assert_eq!(filtered.column(0).unwrap()[0].as_u64(), Some(110));
}

#[test]
fn handles_zero_watermark() {
    let batch = build_batch(&[0, 1, 0], &[0, 1, 0]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(0, 0), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Only (1, 1) should pass (strictly greater than (0, 0))
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered.column(0).unwrap()[0].as_u64(), Some(1));
    assert_eq!(filtered.column(1).unwrap()[0].as_u64(), Some(1));
}

#[test]
fn handles_very_large_watermark() {
    let batch = build_batch(&[u64::MAX - 1, u64::MAX], &[u64::MAX - 1, u64::MAX]);
    let mut dedup = WatermarkDeduplicator::new(
        HighWaterMark::new(u64::MAX - 2, u64::MAX - 2),
        Some(0),
        Some(1),
    );

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Both rows should pass
    assert_eq!(filtered.len(), 2);
}

// ==================== Context ID Tests ====================

#[test]
fn handles_context_id_column_when_present() {
    let batch = build_batch_with_context_id(
        &[100, 150],
        &[10, 20],
        &["ctx1", "ctx2"],
    );
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(90, 5), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    // Both rows pass watermark
    assert_eq!(filtered.len(), 2);

    // Verify context_id column is preserved
    let context_col = filtered.column(2).expect("context_id column");
    assert_eq!(context_col[0].as_str(), Some("ctx1"));
    assert_eq!(context_col[1].as_str(), Some("ctx2"));
}

// ==================== Constructor Tests ====================

#[test]
fn new_stores_initial_watermark() {
    let hwm = HighWaterMark::new(100, 10);
    let dedup = WatermarkDeduplicator::new(hwm, Some(0), Some(1));

    assert_eq!(dedup.initial_watermark().timestamp, 100);
    assert_eq!(dedup.initial_watermark().event_id, 10);
    assert_eq!(dedup.watermark().timestamp, 100);
    assert_eq!(dedup.watermark().event_id, 10);
}

#[test]
fn new_with_zero_watermark() {
    let hwm = HighWaterMark::new(0, 0);
    let dedup = WatermarkDeduplicator::new(hwm, Some(0), Some(1));

    assert_eq!(dedup.initial_watermark().timestamp, 0);
    assert_eq!(dedup.initial_watermark().event_id, 0);
    assert!(dedup.enabled());
}
