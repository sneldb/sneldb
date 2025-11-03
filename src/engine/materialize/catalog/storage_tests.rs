use super::entry::MaterializationEntry;
use super::storage::MaterializationCatalog;
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationError;
use crate::test_helpers::factories::CommandFactory;
use tempfile::tempdir;

fn new_entry(name: &str, root: &std::path::Path) -> MaterializationEntry {
    let spec = MaterializedQuerySpec {
        name: name.into(),
        query: Box::new(CommandFactory::query().with_event_type("orders").create()),
    };
    MaterializationEntry::new(spec, root).expect("entry")
}

#[test]
fn load_creates_catalog_directory() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    assert!(catalog.root_dir().exists());
    assert_eq!(catalog.entries().len(), 0);
    Ok(())
}

#[test]
fn insert_and_reload_roundtrip() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    {
        let mut catalog = MaterializationCatalog::load(dir.path())?;
        let entry = new_entry("orders_daily", catalog.root_dir());
        catalog.insert(entry)?;
    }

    let catalog = MaterializationCatalog::load(dir.path())?;
    assert!(catalog.get("orders_daily").is_some());
    Ok(())
}

#[test]
fn duplicate_insert_rejected() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());
    catalog.insert(entry.clone())?;

    let err = catalog.insert(entry).unwrap_err();
    matches!(err, MaterializationError::Duplicate(_));
    Ok(())
}

#[test]
fn remove_deletes_entry() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());
    catalog.insert(entry)?;

    let removed = catalog.remove("orders_daily")?;
    assert!(removed.is_some());
    assert!(catalog.get("orders_daily").is_none());
    Ok(())
}
