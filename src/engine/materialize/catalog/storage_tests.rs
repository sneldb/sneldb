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
    assert_eq!(catalog.entries()?.len(), 0);
    Ok(())
}

#[test]
fn load_creates_index_file() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let index_path = catalog.catalog_path();
    assert!(index_path.exists());
    assert!(index_path.ends_with("catalog.mcat"));
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
    let loaded = catalog.get("orders_daily")?;
    assert!(loaded.is_some());
    assert_eq!(loaded.as_ref().unwrap().name, "orders_daily");
    Ok(())
}

#[test]
fn insert_creates_entry_file() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());
    catalog.insert(entry)?;

    let entry_path = catalog.root_dir().join("orders_daily").join("entry.mcatentry");
    assert!(entry_path.exists());
    Ok(())
}

#[test]
fn duplicate_insert_rejected() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());
    catalog.insert(entry.clone())?;

    let err = catalog.insert(entry).unwrap_err();
    assert!(matches!(err, MaterializationError::Duplicate(_)));
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
    assert_eq!(removed.as_ref().unwrap().name, "orders_daily");

    let loaded = catalog.get("orders_daily")?;
    assert!(loaded.is_none());
    Ok(())
}

#[test]
fn remove_deletes_entry_file() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());
    catalog.insert(entry)?;

    let entry_path = catalog.root_dir().join("orders_daily").join("entry.mcatentry");
    assert!(entry_path.exists());

    catalog.remove("orders_daily")?;
    assert!(!entry_path.exists());
    Ok(())
}

#[test]
fn remove_missing_entry_returns_none() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    let removed = catalog.remove("missing")?;
    assert!(removed.is_none());
    Ok(())
}

#[test]
fn upsert_inserts_new_entry() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;
    let entry = new_entry("orders_daily", catalog.root_dir());

    catalog.upsert(entry.clone())?;
    let loaded = catalog.get("orders_daily")?;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().name, "orders_daily");
    Ok(())
}

#[test]
fn upsert_updates_existing_entry() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    let mut entry1 = new_entry("orders_daily", catalog.root_dir());
    entry1.row_count = 10;
    catalog.upsert(entry1)?;

    let mut entry2 = new_entry("orders_daily", catalog.root_dir());
    entry2.row_count = 20;
    catalog.upsert(entry2)?;

    let loaded = catalog.get("orders_daily")?;
    assert_eq!(loaded.unwrap().row_count, 20);
    Ok(())
}

#[test]
fn list_names_returns_all_entry_names() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    catalog.insert(new_entry("entry1", catalog.root_dir()))?;
    catalog.insert(new_entry("entry2", catalog.root_dir()))?;
    catalog.insert(new_entry("entry3", catalog.root_dir()))?;

    let names = catalog.list_names()?;
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"entry1".to_string()));
    assert!(names.contains(&"entry2".to_string()));
    assert!(names.contains(&"entry3".to_string()));
    Ok(())
}

#[test]
fn list_names_empty_catalog_returns_empty_vec() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let names = catalog.list_names()?;
    assert!(names.is_empty());
    Ok(())
}

#[test]
fn entries_loads_all_entries() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    catalog.insert(new_entry("entry1", catalog.root_dir()))?;
    catalog.insert(new_entry("entry2", catalog.root_dir()))?;

    let entries = catalog.entries()?;
    assert_eq!(entries.len(), 2);
    assert!(entries.contains_key("entry1"));
    assert!(entries.contains_key("entry2"));
    Ok(())
}

#[test]
fn entries_empty_catalog_returns_empty_map() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let entries = catalog.entries()?;
    assert!(entries.is_empty());
    Ok(())
}

#[test]
fn get_missing_entry_returns_none() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let loaded = catalog.get("missing")?;
    assert!(loaded.is_none());
    Ok(())
}

#[test]
fn catalog_path_returns_index_path() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let path = catalog.catalog_path();
    assert!(path.ends_with("catalog.mcat"));
    assert!(path.starts_with(catalog.root_dir()));
    Ok(())
}

#[test]
fn root_dir_returns_materializations_directory() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let catalog = MaterializationCatalog::load(dir.path())?;

    let root = catalog.root_dir();
    assert!(root.ends_with("materializations"));
    assert!(root.exists());
    Ok(())
}

#[test]
fn multiple_inserts_persist_independently() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    let mut entry1 = new_entry("entry1", catalog.root_dir());
    entry1.row_count = 100;
    catalog.insert(entry1)?;

    let mut entry2 = new_entry("entry2", catalog.root_dir());
    entry2.row_count = 200;
    catalog.insert(entry2)?;

    let loaded1 = catalog.get("entry1")?;
    let loaded2 = catalog.get("entry2")?;

    assert_eq!(loaded1.unwrap().row_count, 100);
    assert_eq!(loaded2.unwrap().row_count, 200);
    Ok(())
}

#[test]
fn catalog_survives_process_restart() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();

    // First process
    {
        let mut catalog = MaterializationCatalog::load(dir.path())?;
        catalog.insert(new_entry("persistent_entry", catalog.root_dir()))?;
    }

    // Second process (simulated by reloading)
    {
        let catalog = MaterializationCatalog::load(dir.path())?;
        let loaded = catalog.get("persistent_entry")?;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().name, "persistent_entry");
    }
    Ok(())
}

#[test]
fn catalog_handles_large_number_of_entries() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let mut catalog = MaterializationCatalog::load(dir.path())?;

    // Insert 100 entries
    for i in 0..100 {
        let entry = new_entry(&format!("entry_{}", i), catalog.root_dir());
        catalog.insert(entry)?;
    }

    let names = catalog.list_names()?;
    assert_eq!(names.len(), 100);

    // Verify we can still get entries
    let loaded = catalog.get("entry_50")?;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().name, "entry_50");
    Ok(())
}

