use super::decoder::Decoder;
use super::encoder::Encoder;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::catalog::SchemaSnapshot;
use crate::engine::materialize::high_water::HighWaterMark;
use crate::engine::materialize::store::frame::header::FrameHeader;
use crate::engine::materialize::store::frame::metadata::StoredFrameMeta;
use crate::engine::types::ScalarValue;
use serde_json::{Value, json};
use std::sync::Arc;

fn build_schema_with_types(types: Vec<(&str, &str)>) -> (Arc<BatchSchema>, Vec<SchemaSnapshot>) {
    let columns: Vec<ColumnSpec> = types
        .iter()
        .map(|(name, logical_type)| ColumnSpec {
            name: name.to_string(),
            logical_type: logical_type.to_string(),
        })
        .collect();

    let batch_schema = Arc::new(BatchSchema::new(columns).unwrap());
    let snapshots: Vec<SchemaSnapshot> = types
        .iter()
        .map(|(name, logical_type)| SchemaSnapshot::new(name.to_string(), logical_type.to_string()))
        .collect();

    (batch_schema, snapshots)
}

fn build_batch(schema: &Arc<BatchSchema>, rows: Vec<Vec<Value>>) -> ColumnBatch {
    let pool = BatchPool::new(100).unwrap();
    let mut builder = pool.acquire(Arc::clone(schema));

    for row in rows {
        let scalar_row: Vec<ScalarValue> = row.into_iter().map(ScalarValue::from).collect();
        builder.push_row(&scalar_row).unwrap();
    }

    builder.finish().unwrap()
}

fn create_payload_from_batch(
    schema: &Arc<BatchSchema>,
    snapshots: &[SchemaSnapshot],
    batch: &ColumnBatch,
) -> (Vec<u8>, FrameHeader, StoredFrameMeta) {
    let encoded = Encoder::encode(snapshots, batch).unwrap();

    // Serialize format: [null_bitmap] [num_var_cols:u32] [length_tables] [column_data]
    let mut payload = Vec::new();
    payload.extend_from_slice(&encoded.null_bitmap);
    payload.extend_from_slice(&(encoded.length_tables.len() as u32).to_le_bytes());

    for length_table in &encoded.length_tables {
        for &len in length_table {
            payload.extend_from_slice(&len.to_le_bytes());
        }
    }

    for col_data in &encoded.column_data {
        payload.extend_from_slice(col_data);
    }

    let header = FrameHeader {
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count as u32,
        column_count: snapshots.len() as u32,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        uncompressed_len: payload.len() as u32,
        compressed_len: 0,
        null_bitmap_len: encoded.null_bitmap.len() as u32,
        checksum: 0,
    };

    let meta = StoredFrameMeta {
        file_name: "test.mat".to_string(),
        schema: snapshots.to_vec(),
        schema_hash: encoded.schema_hash,
        row_count: encoded.row_count as u32,
        min_timestamp: encoded.min_timestamp,
        max_timestamp: encoded.max_timestamp,
        max_event_id: encoded.max_event_id,
        compressed_len: 0,
        uncompressed_len: payload.len() as u32,
        null_bitmap_len: encoded.null_bitmap.len() as u32,
        high_water_mark: HighWaterMark::new(encoded.max_timestamp, encoded.max_event_id),
    };

    (payload, header, meta)
}

#[test]
fn decode_basic_batch() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("timestamp", "Timestamp"), ("event_id", "Integer")]);

    let original = build_batch(
        &schema,
        vec![
            vec![json!(1700000000_u64), json!(1_u64)],
            vec![json!(1700000001_u64), json!(2_u64)],
        ],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1700000000_u64))
    );
    assert_eq!(
        decoded.column(0).unwrap()[1],
        ScalarValue::from(json!(1700000001_u64))
    );
    assert_eq!(
        decoded.column(1).unwrap()[0],
        ScalarValue::from(json!(1_u64))
    );
    assert_eq!(
        decoded.column(1).unwrap()[1],
        ScalarValue::from(json!(2_u64))
    );
}

#[test]
fn decode_handles_null_values() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("id", "Integer"), ("optional", "String")]);

    let original = build_batch(
        &schema,
        vec![
            vec![json!(1), json!("present")],
            vec![json!(2), Value::Null],
            vec![Value::Null, json!("also")],
        ],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded.column(0).unwrap()[0], ScalarValue::from(json!(1)));
    assert_eq!(decoded.column(0).unwrap()[2], ScalarValue::Null);
    assert_eq!(decoded.column(1).unwrap()[1], ScalarValue::Null);
}

#[test]
fn decode_handles_all_column_types() {
    let (schema, snapshots) = build_schema_with_types(vec![
        ("timestamp", "Timestamp"),
        ("id", "Integer"),
        ("price", "Float"),
        ("active", "Boolean"),
        ("name", "String"),
        ("metadata", "JSON"),
    ]);

    let original = build_batch(
        &schema,
        vec![vec![
            json!(1000_u64),
            json!(42_i64),
            json!(19.99),
            json!(true),
            json!("product"),
            json!({"category": "electronics"}),
        ]],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 1);
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1000_u64))
    );
    assert_eq!(
        decoded.column(1).unwrap()[0],
        ScalarValue::from(json!(42_i64))
    );
    assert_eq!(
        decoded.column(3).unwrap()[0],
        ScalarValue::from(json!(true))
    );
    assert_eq!(
        decoded.column(4).unwrap()[0],
        ScalarValue::from(json!("product"))
    );
}

#[test]
fn decode_handles_string_columns() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer"), ("name", "String")]);

    let original = build_batch(
        &schema,
        vec![
            vec![json!(1), json!("short")],
            vec![json!(2), json!("much longer string here")],
        ],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded.column(1).unwrap()[0],
        ScalarValue::from(json!("short"))
    );
    assert_eq!(
        decoded.column(1).unwrap()[1],
        ScalarValue::from(json!("much longer string here"))
    );
}

#[test]
fn decode_handles_json_columns() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer"), ("data", "JSON")]);

    let original = build_batch(
        &schema,
        vec![
            vec![json!(1), json!({"key": "value"})],
            vec![json!(2), json!([1, 2, 3])],
        ],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 2);
    assert_eq!(
        decoded.column(1).unwrap()[0],
        ScalarValue::from(json!({"key": "value"}))
    );
    assert_eq!(
        decoded.column(1).unwrap()[1],
        ScalarValue::from(json!([1, 2, 3]))
    );
}

#[test]
fn decode_fails_on_insufficient_payload_size() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer")]);
    let original = build_batch(&schema, vec![vec![json!(1)]]);
    let (mut payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);

    // Truncate payload
    payload.truncate(payload.len() - 10);

    let err = Decoder::decode(&meta, payload, &header).unwrap_err();
    assert!(err.to_string().contains("too small"));
}

#[test]
fn decode_fails_on_mismatched_variable_column_count() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer"), ("name", "String")]);
    let original = build_batch(&schema, vec![vec![json!(1), json!("test")]]);
    let (mut payload, header, mut meta) = create_payload_from_batch(&schema, &snapshots, &original);

    // Corrupt the variable column count
    payload[meta.null_bitmap_len as usize] = 99; // Wrong count

    let err = Decoder::decode(&meta, payload, &header).unwrap_err();
    assert!(err.to_string().contains("variable column count mismatch"));
}

#[test]
fn decode_handles_empty_batch() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer")]);
    let original = build_batch(&schema, vec![]);
    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);

    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 0);
}

#[test]
fn decode_fails_on_trailing_bytes() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer")]);
    let original = build_batch(&schema, vec![vec![json!(1)]]);
    let (mut payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);

    // Add extra bytes
    payload.extend_from_slice(&[1, 2, 3, 4]);

    let err = Decoder::decode(&meta, payload, &header).unwrap_err();
    assert!(err.to_string().contains("trailing bytes"));
}

#[test]
fn encode_decode_roundtrip_complex() {
    let (schema, snapshots) = build_schema_with_types(vec![
        ("timestamp", "Timestamp"),
        ("id", "Integer"),
        ("price", "Float"),
        ("active", "Boolean"),
        ("name", "String"),
        ("tags", "JSON"),
    ]);

    let original = build_batch(
        &schema,
        vec![
            vec![
                json!(1000_u64),
                json!(1),
                json!(10.5),
                json!(true),
                json!("item1"),
                json!(["tag1", "tag2"]),
            ],
            vec![
                json!(2000_u64),
                json!(2),
                json!(20.75),
                json!(false),
                json!("item2"),
                json!({"meta": "data"}),
            ],
            vec![
                json!(3000_u64),
                Value::Null,
                json!(30.0),
                Value::Null,
                json!("item3"),
                Value::Null,
            ],
        ],
    );

    let (payload, header, meta) = create_payload_from_batch(&schema, &snapshots, &original);
    let decoded = Decoder::decode(&meta, payload, &header).unwrap();

    assert_eq!(decoded.len(), 3);
    // Verify first row
    assert_eq!(
        decoded.column(0).unwrap()[0],
        ScalarValue::from(json!(1000_u64))
    );
    assert_eq!(decoded.column(1).unwrap()[0], ScalarValue::from(json!(1)));
    assert_eq!(
        decoded.column(2).unwrap()[0],
        ScalarValue::from(json!(10.5))
    );
    assert_eq!(
        decoded.column(3).unwrap()[0],
        ScalarValue::from(json!(true))
    );
    assert_eq!(
        decoded.column(4).unwrap()[0],
        ScalarValue::from(json!("item1"))
    );
    assert_eq!(
        decoded.column(5).unwrap()[0],
        ScalarValue::from(json!(["tag1", "tag2"]))
    );

    // Verify nulls in third row
    assert_eq!(decoded.column(1).unwrap()[2], ScalarValue::Null);
    assert_eq!(decoded.column(3).unwrap()[2], ScalarValue::Null);
    assert_eq!(decoded.column(5).unwrap()[2], ScalarValue::Null);
}
