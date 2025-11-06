use super::encoder::Encoder;
use crate::engine::core::read::flow::{BatchPool, BatchSchema, ColumnBatch};
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::catalog::SchemaSnapshot;
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
    use crate::engine::types::ScalarValue;
    // Use capacity larger than needed for large batch tests
    let capacity = rows.len().max(1000);
    let pool = BatchPool::new(capacity).unwrap();
    let mut builder = pool.acquire(Arc::clone(schema));

    for row in rows {
        let scalar_row: Vec<ScalarValue> = row.into_iter().map(ScalarValue::from).collect();
        builder.push_row(&scalar_row).unwrap();
    }

    builder.finish().unwrap()
}

#[test]
fn encode_basic_batch() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("timestamp", "Timestamp"), ("event_id", "Integer")]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!(1700000000_u64), json!(1_u64)],
            vec![json!(1700000001_u64), json!(2_u64)],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 2);
    assert_eq!(
        encoded.schema_hash,
        super::schema::SchemaConverter::hash(&snapshots)
    );
    assert_eq!(encoded.min_timestamp, 1700000000);
    assert_eq!(encoded.max_timestamp, 1700000001);
    assert_eq!(encoded.max_event_id, 2);
}

#[test]
fn encode_tracks_timestamp_range() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("timestamp", "Timestamp"), ("value", "String")]);

    let timestamps = vec![
        json!(1000_u64),
        json!(500_u64),
        json!(2000_u64),
        json!(1500_u64),
    ];

    let batch = build_batch(
        &schema,
        timestamps
            .into_iter()
            .map(|ts| vec![ts, json!("test")])
            .collect(),
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.min_timestamp, 500);
    assert_eq!(encoded.max_timestamp, 2000);
}

#[test]
fn encode_tracks_max_event_id() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("timestamp", "Timestamp"), ("event_id", "Integer")]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!(1000_u64), json!(5_u64)],
            vec![json!(2000_u64), json!(10_u64)],
            vec![json!(3000_u64), json!(3_u64)],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.max_event_id, 10);
}

#[test]
fn encode_handles_null_values() {
    let (schema, snapshots) = build_schema_with_types(vec![
        ("timestamp", "Timestamp"),
        ("event_id", "Integer"),
        ("optional", "String"),
    ]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!(1000_u64), json!(1_u64), json!("present")],
            vec![json!(2000_u64), Value::Null, json!("also")],
            vec![json!(3000_u64), json!(2_u64), Value::Null],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 3);
    // Null bitmap should have bits set for null values
    assert!(encoded.null_bitmap.len() > 0);
}

#[test]
fn encode_handles_string_columns() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("name", "String"), ("description", "String")]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!("alice"), json!("short")],
            vec![json!("bob"), json!("much longer description here")],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 2);
    assert_eq!(encoded.length_tables.len(), 2); // Two variable-size columns
    assert_eq!(encoded.length_tables[0].len(), 2);
    assert_eq!(encoded.length_tables[1].len(), 2);
}

#[test]
fn encode_handles_json_columns() {
    let (schema, snapshots) =
        build_schema_with_types(vec![("id", "Integer"), ("metadata", "JSON")]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!(1), json!({"key": "value"})],
            vec![json!(2), json!([1, 2, 3])],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 2);
    assert_eq!(encoded.length_tables.len(), 1); // One JSON column
}

#[test]
fn encode_handles_mixed_column_types() {
    let (schema, snapshots) = build_schema_with_types(vec![
        ("timestamp", "Timestamp"),
        ("id", "Integer"),
        ("price", "Float"),
        ("active", "Boolean"),
        ("name", "String"),
        ("metadata", "JSON"),
    ]);

    let batch = build_batch(
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

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 1);
    assert_eq!(encoded.length_tables.len(), 2); // String and JSON are variable-size
}

#[test]
fn encode_handles_empty_batch() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer")]);
    let batch = build_batch(&schema, vec![]);

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 0);
    assert_eq!(encoded.min_timestamp, 0);
    assert_eq!(encoded.max_timestamp, 0);
}

#[test]
fn encode_handles_large_batch() {
    let (schema, snapshots) = build_schema_with_types(vec![("value", "Integer")]);
    let rows: Vec<Vec<Value>> = (0..1000).map(|i| vec![json!(i)]).collect();
    let batch = build_batch(&schema, rows);

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.row_count, 1000);
}

#[test]
fn encode_sets_default_timestamp_when_none() {
    let (schema, snapshots) = build_schema_with_types(vec![("id", "Integer")]);
    let batch = build_batch(&schema, vec![vec![json!(1)]]);

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    assert_eq!(encoded.min_timestamp, 0);
    assert_eq!(encoded.max_timestamp, 0);
}

#[test]
fn encode_creates_length_tables_for_variable_columns() {
    let (schema, snapshots) = build_schema_with_types(vec![
        ("id", "Integer"),
        ("text", "String"),
        ("data", "JSON"),
        ("value", "Integer"),
    ]);

    let batch = build_batch(
        &schema,
        vec![
            vec![json!(1), json!("short"), json!({"a": 1}), json!(10)],
            vec![
                json!(2),
                json!("much longer text"),
                json!({"b": 2}),
                json!(20),
            ],
        ],
    );

    let encoded = Encoder::encode(&snapshots, &batch).unwrap();

    // Only String and JSON should have length tables
    assert_eq!(encoded.length_tables.len(), 2);
    // Each length table should have 2 entries (one per row)
    assert_eq!(encoded.length_tables[0].len(), 2);
    assert_eq!(encoded.length_tables[1].len(), 2);
}
