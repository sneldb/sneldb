use super::entry::MaterializationEntry;
use super::file::CatalogFile;
use crate::engine::materialize::MaterializationError;
use crate::test_helpers::factories::CommandFactory;
use std::collections::HashMap;
use std::fs;
use tempfile::tempdir;

fn build_entry(root: &std::path::Path, name: &str) -> MaterializationEntry {
    let spec = crate::command::types::MaterializedQuerySpec {
        name: name.into(),
        query: Box::new(CommandFactory::query().with_event_type("orders").create()),
    };
    MaterializationEntry::new(spec, root).expect("entry")
}

#[test]
fn load_missing_file_returns_empty() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let file = CatalogFile::new(dir.path().join("catalog.bin"));
    let entries = file.load_entries()?;
    assert!(entries.is_empty());
    Ok(())
}

#[test]
fn persist_and_reload_roundtrip() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let file = CatalogFile::new(dir.path().join("catalog.bin"));
    let entry = build_entry(dir.path(), "orders_daily");

    let mut map = HashMap::new();
    map.insert(entry.name.clone(), entry);
    file.persist(&map)?;

    let loaded = file.load_entries()?;
    assert!(loaded.contains_key("orders_daily"));
    Ok(())
}

#[test]
fn corrupt_file_is_backed_up_and_returns_empty() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let path = dir.path().join("catalog.bin");
    fs::write(&path, b"not-a-valid-catalog").unwrap();

    let file = CatalogFile::new(path.clone());
    let entries = file.load_entries()?;
    assert!(entries.is_empty());

    let backups: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("catalog.corrupt-") {
                Some(entry.path())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(backups.len(), 1);
    assert!(!path.exists());
    Ok(())
}
