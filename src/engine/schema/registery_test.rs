use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::registry::SchemaRegistry;
use crate::test_helpers::factories::MiniSchemaFactory;
use std::fs::{self, File};
use std::io::Write;
use tempfile::tempdir;

#[test]
fn define_and_lookup_schema_registry() {
    let dir = tempdir().expect("temp dir failed");
    let path = dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
    let schema = MiniSchemaFactory::new().create();
    registry.define("user_signed_up", schema.clone()).unwrap();

    // Verify getters
    assert_eq!(registry.get("user_signed_up"), Some(&schema));
    let uid = registry.get_uid("user_signed_up").unwrap();
    assert_eq!(
        registry.get_event_type_by_uid(&uid),
        Some("user_signed_up".to_string())
    );
    assert_eq!(registry.get_schema_by_uid(&uid), Some(&schema));

    // Reconstruct registry from file and check persistence
    let registry2 = SchemaRegistry::new_with_path(path).unwrap();
    assert_eq!(registry2.get("user_signed_up"), Some(&schema));
    let uid2 = registry2.get_uid("user_signed_up").unwrap();
    assert_eq!(
        registry2.get_event_type_by_uid(&uid2),
        Some("user_signed_up".to_string())
    );
}

#[test]
fn cannot_define_same_event_twice() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(path).unwrap();
    let schema = MiniSchemaFactory::new().create();
    registry.define("user_signed_up", schema.clone()).unwrap();

    let result = registry.define("user_signed_up", schema);
    assert!(matches!(result, Err(SchemaError::AlreadyDefined(_))));
}

#[test]
fn cannot_define_empty_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(path).unwrap();
    let schema = MiniSchemaFactory::empty().create();

    let result = registry.define("user_signed_up", schema);
    assert!(matches!(result, Err(SchemaError::EmptySchema)));
}

#[test]
fn test_define_and_get_schema() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(file_path.clone()).unwrap();

    let schema = MiniSchemaFactory::new()
        .with("id", "int | null")
        .with("status", "string")
        .create();
    registry
        .define("order_created", schema.clone())
        .expect("Failed to define schema");

    // Check retrieval
    let loaded = registry.get("order_created").expect("Schema not found");
    assert_eq!(loaded, &schema);

    // Ensure file is created
    assert!(file_path.exists());
}

#[test]
fn test_redefine_should_fail() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(file_path).unwrap();

    let schema1 = MiniSchemaFactory::new().with("id", "int").create();
    let schema2 = MiniSchemaFactory::new().with("id", "string").create();

    registry
        .define("user_registered", schema1)
        .expect("Initial define should succeed");

    let result = registry.define("user_registered", schema2);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        SchemaError::AlreadyDefined(_)
    ));
}

#[test]
fn test_persist_and_reload_schema() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("schemas.bin");

    {
        let mut registry = SchemaRegistry::new_with_path(file_path.clone()).unwrap();

        let schema = MiniSchemaFactory::new()
            .with("amount", "float")
            .with("currency", "string")
            .create();
        registry
            .define("payment_received", schema.clone())
            .expect("Failed to define schema");
    }

    {
        let registry = SchemaRegistry::new_with_path(file_path.clone()).unwrap();

        let loaded = registry
            .get("payment_received")
            .expect("Schema not loaded after restart");

        assert_eq!(
            loaded,
            &MiniSchemaFactory::new()
                .with("amount", "float")
                .with("currency", "string")
                .create()
        );
    }
}

#[test]
fn test_empty_file_does_not_crash() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    // Create an empty file
    File::create(&path).unwrap();

    // Should not panic or fail
    let registry = SchemaRegistry::new_with_path(path).unwrap();
    assert!(registry.get("anything").is_none());
}

#[test]
fn test_corrupted_schema_file_is_partially_ignored() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    {
        let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
        registry
            .define("good", MiniSchemaFactory::new().with("a", "int").create())
            .expect("Failed to define good schema");
        registry
            .define(
                "corrupt_me",
                MiniSchemaFactory::new().with("b", "string").create(),
            )
            .expect("Failed to define corruptable schema");
    }

    // Manually truncate the last schema (simulate corruption)
    let metadata = fs::metadata(&path).unwrap();
    let len = metadata.len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 10) // truncate partway into the second schema
        .unwrap();

    // Now reload
    let registry = SchemaRegistry::new_with_path(path).unwrap();
    // "good" should still be there
    assert!(
        registry.get("good").is_some(),
        "Expected good schema to survive"
    );
    // "corrupt_me" likely isn't
    assert!(
        registry.get("corrupt_me").is_none(),
        "Corrupted schema should not be present"
    );
}
#[test]
fn test_multiple_schemas_persist_and_reload() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    {
        let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
        registry
            .define(
                "event_1",
                MiniSchemaFactory::new().with("a", "int").create(),
            )
            .unwrap();
        registry
            .define(
                "event_2",
                MiniSchemaFactory::new().with("b", "string").create(),
            )
            .unwrap();
    }

    let registry = SchemaRegistry::new_with_path(path).unwrap();
    assert!(registry.get("event_1").is_some());
    assert!(registry.get("event_2").is_some());
}

#[test]
fn test_reload_does_not_mutate_existing_schemas() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    let schema = MiniSchemaFactory::new().with("x", "int").create();

    {
        let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
        registry.define("reloaded_event", schema.clone()).unwrap();
    }

    let registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
    let loaded = registry.get("reloaded_event").unwrap();
    assert_eq!(loaded, &schema);
}

#[test]
fn test_duplicate_define_even_if_identical() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    let schema = MiniSchemaFactory::new().with("a", "int").create();

    let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();
    registry
        .define("dup_event", schema.clone())
        .expect("Initial define should succeed");

    let result = registry.define("dup_event", schema.clone());
    assert!(
        result.is_err(),
        "Redefinition should fail even if identical"
    );
}

#[test]
fn test_oversized_schema_record_skipped_silently() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().join("schemas.bin");

    // Write a corrupted schema with a very large size prefix
    let mut file = File::create(&path).unwrap();
    let large_len: u32 = 999_999;
    file.write_all(&large_len.to_le_bytes()).unwrap();
    file.write_all(&vec![0u8; 16]).unwrap(); // dummy payload

    // Load the registry — should succeed but yield no records
    let registry = SchemaRegistry::new_with_path(path).expect("Registry load should succeed");
    assert_eq!(
        registry.get_all().len(),
        0,
        "No schemas should be loaded from corrupted file"
    );
}

#[test]
fn define_with_enum_field_persists_and_reloads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();

    let schema = crate::test_helpers::factories::MiniSchemaFactory::empty()
        .with_enum("plan", &["pro", "basic"])
        .create();

    registry
        .define("subscription", schema.clone())
        .expect("define schema should succeed");

    // Verify present
    let loaded = registry.get("subscription").unwrap();
    assert_eq!(loaded, &schema);

    // Reload registry and verify
    let registry2 = SchemaRegistry::new_with_path(path).unwrap();
    let loaded2 = registry2.get("subscription").unwrap();
    assert_eq!(loaded2, &schema);
}

#[test]
fn define_with_optional_field_persists_and_reloads() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    let mut registry = SchemaRegistry::new_with_path(path.clone()).unwrap();

    let schema = crate::test_helpers::factories::MiniSchemaFactory::empty()
        .with_optional("nickname", "string")
        .create();

    registry
        .define("user_profile", schema.clone())
        .expect("define schema should succeed");

    let loaded = registry.get("user_profile").unwrap();
    assert_eq!(loaded, &schema);

    let registry2 = SchemaRegistry::new_with_path(path).unwrap();
    let loaded2 = registry2.get("user_profile").unwrap();
    assert_eq!(loaded2, &schema);
}
