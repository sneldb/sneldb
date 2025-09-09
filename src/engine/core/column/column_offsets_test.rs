use crate::engine::core::{ColumnKey, ColumnOffsets};
use crate::test_helpers::factories::SchemaRegistryFactory;
use tempfile::tempdir;

#[tokio::test]
async fn test_column_offsets_write_and_load_roundtrip() {
    // --- Setup ---
    let tmp = tempdir().unwrap();
    let segment_dir = tmp.path();
    let registry_factory = SchemaRegistryFactory::new();
    let registry = registry_factory.registry();
    let event_type = "user_event";

    registry_factory
        .define_with_fields(event_type, &[("email", "string")])
        .await
        .expect("Schema definition failed");

    let uid = registry
        .read()
        .await
        .get_uid(event_type)
        .expect("UID not found for 'user_event'");

    let field = "email".to_string();
    let key: ColumnKey = (event_type.to_string(), field.clone());

    // --- Insert mock data ---
    let mut offsets = ColumnOffsets::new();
    offsets.insert_offset(key.clone(), 0, 100, "foo@example.com".to_string());
    offsets.insert_offset(key.clone(), 0, 200, "bar@example.com".to_string());
    offsets.insert_offset(key.clone(), 1, 300, "baz@example.com".to_string());

    // --- Write to .zf ---
    offsets.write(segment_dir, &registry).await.unwrap();

    // --- Load from .zf ---
    let loaded = ColumnOffsets::load(segment_dir, "00000", &uid, &field).unwrap();

    // --- Verify ---
    assert_eq!(loaded.len(), 2); // 2 zones: 0 and 1
    assert_eq!(loaded[&0], vec![100, 200]);
    assert_eq!(loaded[&1], vec![300]);
}
