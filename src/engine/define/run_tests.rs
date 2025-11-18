use super::run::define_schema;
use crate::engine::schema::FieldType;
use crate::engine::schema::registry::{MiniSchema, SchemaRegistry};
use std::collections::HashMap;
use tempfile;

#[tokio::test]
async fn test_define_schema_success() {
    crate::logging::init_for_tests();
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut registry = SchemaRegistry::new_with_path(tmpfile.path().to_path_buf()).unwrap();
    let mut fields = HashMap::new();
    fields.insert("field1".to_string(), FieldType::String);
    let schema = MiniSchema {
        fields: fields.clone(),
    };
    let result = define_schema(&mut registry, "test_event", 1, schema.clone()).await;
    assert!(result.is_ok(), "define_schema failed: {:?}", result);
    assert_eq!(registry.get("test_event").unwrap(), &schema);
}

#[tokio::test]
async fn test_define_schema_duplicate() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut registry = SchemaRegistry::new_with_path(tmpfile.path().to_path_buf()).unwrap();
    let mut fields = HashMap::new();
    fields.insert("field1".to_string(), FieldType::String);
    let schema = MiniSchema {
        fields: fields.clone(),
    };
    let _ = define_schema(&mut registry, "test_event", 1, schema.clone()).await;
    let result = define_schema(&mut registry, "test_event", 1, schema).await;
    assert!(result.is_err());
}
