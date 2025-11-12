use std::path::Path;

use super::catalog::{CatalogGateway, CatalogHandle, FileCatalogGateway};
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationEntry;
use crate::engine::materialize::catalog::{MaterializationCatalog, SchemaSnapshot};
use crate::test_helpers::factories::command_factory::CommandFactory;

fn make_entry(root: &Path, alias: &str) -> MaterializationEntry {
    let command = CommandFactory::query().with_event_type("orders").create();

    let spec = MaterializedQuerySpec {
        name: alias.to_string(),
        query: Box::new(command),
    };

    let mut entry = MaterializationEntry::new(spec, root).expect("entry");
    entry.schema = vec![
        SchemaSnapshot::new("timestamp", "Number"),
        SchemaSnapshot::new("event_id", "Number"),
    ];
    entry
}

#[test]
fn fetch_returns_cloned_entry() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let catalog = MaterializationCatalog::load(temp_dir.path()).expect("catalog");
    let mut handle = CatalogHandle::new(catalog);

    let entry = make_entry(temp_dir.path(), "orders_view");
    handle.upsert(entry.clone()).expect("upsert");

    let fetched = handle.fetch("orders_view").expect("fetch");
    assert_eq!(fetched.name, entry.name);
    assert_eq!(fetched.schema, entry.schema);
}

#[test]
fn fetch_missing_entry_returns_error() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let catalog = MaterializationCatalog::load(temp_dir.path()).expect("catalog");
    let handle = CatalogHandle::new(catalog);

    let err = handle.fetch("missing").expect_err("should fail");
    assert!(err.message().contains("missing"));
}

#[test]
fn upsert_persists_to_catalog_file() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let catalog = MaterializationCatalog::load(temp_dir.path()).expect("catalog");
    let mut handle = CatalogHandle::new(catalog);

    let entry = make_entry(temp_dir.path(), "persisted_view");
    handle.upsert(entry.clone()).expect("upsert");

    // Dropping handle writes to disk; reload catalog to confirm entry exists
    let reloaded = MaterializationCatalog::load(temp_dir.path()).expect("reload");
    assert!(reloaded.get("persisted_view").expect("get").is_some());
}

#[test]
fn file_gateway_uses_provided_directory() {
    // Test that gateway can be constructed with a provided directory
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let gateway = FileCatalogGateway::new(temp_dir.path());

    // Try to load the catalog - should succeed with empty catalog
    let handle = gateway.load().expect("load should succeed");

    // Verify we can query it (should be empty, so fetch should fail)
    assert!(handle.fetch("non-existent").is_err());
}
