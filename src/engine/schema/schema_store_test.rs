use crate::engine::schema::schema_store::SchemaStore;
use crate::test_helpers::factories::SchemaRecordFactory;
use tempfile::tempdir;

#[test]
fn test_schema_store_append_and_load() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let file_path = temp_dir.path().join("schemas_test.bin");

    let store = SchemaStore::new(file_path.clone()).expect("Failed to create SchemaStore");

    let record = SchemaRecordFactory::new("test_event")
        .with_uid("uid-test")
        .with_field("fieldA", "string")
        .without_field("field1")
        .create();

    store
        .append(&record)
        .expect("Failed to append schema record");

    let loaded = store.load().expect("Failed to load schema records");

    assert_eq!(loaded.len(), 1);
    let loaded_record = &loaded[0];
    assert_eq!(loaded_record.event_type, "test_event");
    assert_eq!(loaded_record.uid, "uid-test");
    assert!(loaded_record.schema.fields.contains_key("fieldA"));
    assert!(!loaded_record.schema.fields.contains_key("field1"));
}
