use std::sync::Arc;

use super::watermark::WatermarkDeduplicator;
use crate::engine::core::read::flow::{BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::HighWaterMark;
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

    let timestamp_column = timestamps.iter().map(|ts| json!(ts)).collect();
    let event_column = event_ids.iter().map(|id| json!(id)).collect();

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

#[test]
fn filters_rows_beyond_watermark() {
    let batch = build_batch(&[100, 150, 90], &[10, 20, 30]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    let filtered = dedup.filter(batch).expect("filtered batch");
    assert_eq!(filtered.len(), 1);
}

#[test]
fn returns_none_when_all_rows_filtered() {
    let batch = build_batch(&[50, 75], &[1, 2]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(100, 10), Some(0), Some(1));

    assert!(dedup.filter(batch).is_none());
}

#[test]
fn no_filter_when_indices_missing() {
    let batch = build_batch(&[10], &[1]);
    let mut dedup = WatermarkDeduplicator::new(HighWaterMark::new(0, 0), None, Some(1));

    let original = Arc::clone(&batch);
    let result = dedup.filter(batch).expect("batch");
    assert!(Arc::ptr_eq(&result, &original));
}
