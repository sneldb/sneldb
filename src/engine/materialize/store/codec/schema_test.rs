use super::schema::{
    SchemaConverter, batch_schema_to_snapshots, schema_hash, schema_to_batch_schema,
};
use crate::engine::core::read::flow::BatchSchema;
use crate::engine::core::read::result::ColumnSpec;
use crate::engine::materialize::catalog::SchemaSnapshot;

fn create_test_snapshots() -> Vec<SchemaSnapshot> {
    vec![
        SchemaSnapshot::new("timestamp", "Timestamp"),
        SchemaSnapshot::new("event_id", "Integer"),
        SchemaSnapshot::new("context_id", "String"),
    ]
}

#[test]
fn to_batch_schema_converts_snapshots() {
    let snapshots = create_test_snapshots();
    let batch_schema = SchemaConverter::to_batch_schema(&snapshots).unwrap();

    assert_eq!(batch_schema.column_count(), 3);
    assert_eq!(batch_schema.columns()[0].name, "timestamp");
    assert_eq!(batch_schema.columns()[0].logical_type, "Timestamp");
    assert_eq!(batch_schema.columns()[1].name, "event_id");
    assert_eq!(batch_schema.columns()[1].logical_type, "Integer");
    assert_eq!(batch_schema.columns()[2].name, "context_id");
    assert_eq!(batch_schema.columns()[2].logical_type, "String");
}

#[test]
fn to_batch_schema_handles_empty_schema() {
    let snapshots = vec![];
    let err = SchemaConverter::to_batch_schema(&snapshots).unwrap_err();
    assert!(
        err.to_string()
            .contains("schema must contain at least one column")
    );
}

#[test]
fn from_batch_schema_converts_to_snapshots() {
    let batch_schema = BatchSchema::new(vec![
        ColumnSpec {
            name: "timestamp".into(),
            logical_type: "Timestamp".into(),
        },
        ColumnSpec {
            name: "event_id".into(),
            logical_type: "Integer".into(),
        },
    ])
    .unwrap();

    let snapshots = SchemaConverter::from_batch_schema(&batch_schema);

    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].name, "timestamp");
    assert_eq!(snapshots[0].logical_type, "Timestamp");
    assert_eq!(snapshots[1].name, "event_id");
    assert_eq!(snapshots[1].logical_type, "Integer");
}

#[test]
fn from_batch_schema_handles_all_types() {
    let batch_schema = BatchSchema::new(vec![
        ColumnSpec {
            name: "int_col".into(),
            logical_type: "Integer".into(),
        },
        ColumnSpec {
            name: "float_col".into(),
            logical_type: "Float".into(),
        },
        ColumnSpec {
            name: "bool_col".into(),
            logical_type: "Boolean".into(),
        },
        ColumnSpec {
            name: "str_col".into(),
            logical_type: "String".into(),
        },
        ColumnSpec {
            name: "json_col".into(),
            logical_type: "JSON".into(),
        },
    ])
    .unwrap();

    let snapshots = SchemaConverter::from_batch_schema(&batch_schema);

    assert_eq!(snapshots.len(), 5);
    assert_eq!(snapshots[0].logical_type, "Integer");
    assert_eq!(snapshots[1].logical_type, "Float");
    assert_eq!(snapshots[2].logical_type, "Boolean");
    assert_eq!(snapshots[3].logical_type, "String");
    assert_eq!(snapshots[4].logical_type, "JSON");
}

#[test]
fn to_from_batch_schema_roundtrip() {
    let original = create_test_snapshots();
    let batch_schema = SchemaConverter::to_batch_schema(&original).unwrap();
    let converted = SchemaConverter::from_batch_schema(&batch_schema);

    assert_eq!(converted.len(), original.len());
    for (orig, conv) in original.iter().zip(converted.iter()) {
        assert_eq!(orig.name, conv.name);
        assert_eq!(orig.logical_type, conv.logical_type);
    }
}

#[test]
fn hash_is_deterministic() {
    let snapshots = create_test_snapshots();
    let hash1 = SchemaConverter::hash(&snapshots);
    let hash2 = SchemaConverter::hash(&snapshots);

    assert_eq!(hash1, hash2);
}

#[test]
fn hash_differs_for_different_schemas() {
    let snapshots1 = vec![
        SchemaSnapshot::new("timestamp", "Timestamp"),
        SchemaSnapshot::new("event_id", "Integer"),
    ];
    let snapshots2 = vec![
        SchemaSnapshot::new("timestamp", "Timestamp"),
        SchemaSnapshot::new("event_id", "String"), // Different type
    ];
    let snapshots3 = vec![
        SchemaSnapshot::new("timestamp", "Timestamp"),
        SchemaSnapshot::new("other_id", "Integer"), // Different name
    ];

    let hash1 = SchemaConverter::hash(&snapshots1);
    let hash2 = SchemaConverter::hash(&snapshots2);
    let hash3 = SchemaConverter::hash(&snapshots3);

    assert_ne!(hash1, hash2);
    assert_ne!(hash1, hash3);
    assert_ne!(hash2, hash3);
}

#[test]
fn hash_is_order_dependent() {
    let snapshots1 = vec![
        SchemaSnapshot::new("timestamp", "Timestamp"),
        SchemaSnapshot::new("event_id", "Integer"),
    ];
    let snapshots2 = vec![
        SchemaSnapshot::new("event_id", "Integer"),
        SchemaSnapshot::new("timestamp", "Timestamp"),
    ];

    let hash1 = SchemaConverter::hash(&snapshots1);
    let hash2 = SchemaConverter::hash(&snapshots2);

    assert_ne!(hash1, hash2);
}

#[test]
fn convenience_functions_work() {
    let snapshots = create_test_snapshots();
    let batch_schema = schema_to_batch_schema(&snapshots).unwrap();
    let converted = batch_schema_to_snapshots(&batch_schema);
    let hash = schema_hash(&snapshots);

    assert_eq!(converted.len(), snapshots.len());
    assert_ne!(hash, 0);
}
