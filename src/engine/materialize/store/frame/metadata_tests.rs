use super::metadata::StoredFrameMeta;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;

fn sample_schema() -> Vec<SchemaSnapshot> {
    vec![SchemaSnapshot::new("timestamp", "Timestamp")]
}

#[test]
fn serde_defaults_high_water_mark() {
    let json = serde_json::json!({
        "file_name": "000123.mat",
        "schema": [{"name": "ts", "logical_type": "Timestamp"}],
        "schema_hash": 42,
        "row_count": 10,
        "min_timestamp": 100,
        "max_timestamp": 200,
        "max_event_id": 5,
        "compressed_len": 256,
        "uncompressed_len": 512,
        "null_bitmap_len": 8
    });

    let meta: StoredFrameMeta = serde_json::from_value(json).expect("deserialize meta");
    assert_eq!(meta.file_name, "000123.mat");
    assert_eq!(meta.high_water_mark, HighWaterMark::default());
}

#[test]
fn stored_frame_meta_construction_preserves_values() {
    let meta = StoredFrameMeta {
        file_name: "000001.mat".into(),
        schema: sample_schema(),
        schema_hash: 123,
        row_count: 2,
        min_timestamp: 10,
        max_timestamp: 20,
        max_event_id: 30,
        compressed_len: 64,
        uncompressed_len: 128,
        null_bitmap_len: 4,
        high_water_mark: HighWaterMark::new(20, 30),
    };

    assert_eq!(meta.schema_hash, 123);
    assert_eq!(meta.high_water_mark.timestamp, 20);
    assert_eq!(meta.schema.len(), 1);
}
