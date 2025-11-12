use super::index::{CatalogIndex, IndexEntry, IndexFile};
use crate::engine::materialize::MaterializationError;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::shared::time;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn make_index_entry(name: &str, entry_path: PathBuf) -> IndexEntry {
    IndexEntry {
        name: name.to_string(),
        entry_path,
        created_at: time::now(),
    }
}

#[test]
fn catalog_index_new_creates_empty_index() {
    let index = CatalogIndex::new();
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);
    assert_eq!(index.version, 2);
}

#[test]
fn catalog_index_from_entries_creates_index_with_entries() {
    let entries = vec![
        make_index_entry("entry1", PathBuf::from("path1")),
        make_index_entry("entry2", PathBuf::from("path2")),
    ];
    let index = CatalogIndex::from_entries(entries.clone());
    assert_eq!(index.len(), 2);
    assert_eq!(index.version, 2);
    assert_eq!(index.entries.len(), 2);
}

#[test]
fn catalog_index_to_map_converts_to_hashmap() {
    let entries = vec![
        make_index_entry("entry1", PathBuf::from("path1")),
        make_index_entry("entry2", PathBuf::from("path2")),
    ];
    let index = CatalogIndex::from_entries(entries);
    let map = index.to_map();
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("entry1"), Some(&PathBuf::from("path1")));
    assert_eq!(map.get("entry2"), Some(&PathBuf::from("path2")));
}

#[test]
fn catalog_index_get_path_returns_path_for_existing_entry() {
    let entries = vec![make_index_entry("entry1", PathBuf::from("path1"))];
    let index = CatalogIndex::from_entries(entries);
    assert_eq!(index.get_path("entry1"), Some(&PathBuf::from("path1")));
    assert_eq!(index.get_path("missing"), None);
}

#[test]
fn catalog_index_contains_checks_existence() {
    let entries = vec![make_index_entry("entry1", PathBuf::from("path1"))];
    let index = CatalogIndex::from_entries(entries);
    assert!(index.contains("entry1"));
    assert!(!index.contains("missing"));
}

#[test]
fn catalog_index_add_inserts_new_entry() {
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));
    assert_eq!(index.len(), 1);
    assert!(index.contains("entry1"));
    assert_eq!(index.get_path("entry1"), Some(&PathBuf::from("path1")));
}

#[test]
fn catalog_index_add_updates_existing_entry() {
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));
    index.add("entry1".to_string(), PathBuf::from("path2"));
    assert_eq!(index.len(), 1);
    assert_eq!(index.get_path("entry1"), Some(&PathBuf::from("path2")));
}

#[test]
fn catalog_index_remove_deletes_entry() {
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));
    index.add("entry2".to_string(), PathBuf::from("path2"));
    assert_eq!(index.len(), 2);

    let removed = index.remove("entry1");
    assert!(removed);
    assert_eq!(index.len(), 1);
    assert!(!index.contains("entry1"));
    assert!(index.contains("entry2"));
}

#[test]
fn catalog_index_remove_returns_false_for_missing_entry() {
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));
    let removed = index.remove("missing");
    assert!(!removed);
    assert_eq!(index.len(), 1);
}

#[test]
fn index_file_load_missing_file_returns_empty_index() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");
    let index_file = IndexFile::new(index_path);
    let index = index_file.load()?;
    assert!(index.is_empty());
    Ok(())
}

#[test]
fn index_file_persist_and_load_roundtrip() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");
    let index_file = IndexFile::new(index_path.clone());

    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));
    index.add("entry2".to_string(), PathBuf::from("path2"));

    index_file.persist(&index)?;
    assert!(index_path.exists());

    let loaded = index_file.load()?;
    assert_eq!(loaded.len(), 2);
    assert!(loaded.contains("entry1"));
    assert!(loaded.contains("entry2"));
    assert_eq!(loaded.version, 2);
    Ok(())
}

#[test]
fn index_file_load_corrupt_file_backs_up_and_returns_empty() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");
    fs::write(&index_path, b"not-a-valid-index-file").unwrap();

    let index_file = IndexFile::new(index_path.clone());
    let index = index_file.load()?;
    assert!(index.is_empty());

    // Check that corrupt file was backed up
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
    assert!(!index_path.exists());
    Ok(())
}

#[test]
fn index_file_load_file_too_small_returns_empty() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");
    fs::write(&index_path, b"short").unwrap();

    let index_file = IndexFile::new(index_path);
    let index = index_file.load()?;
    assert!(index.is_empty());
    Ok(())
}

#[test]
fn index_file_persist_atomic_write() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");
    let index_file = IndexFile::new(index_path.clone());

    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), PathBuf::from("path1"));

    index_file.persist(&index)?;

    // Check that tmp file was removed
    let tmp_path = dir.path().join("catalog.tmp");
    assert!(!tmp_path.exists());
    assert!(index_path.exists());
    Ok(())
}

#[test]
fn index_file_load_accepts_version_2() -> Result<(), MaterializationError> {
    use std::fs::File;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");

    {
        let mut file = File::create(&index_path)?;
        let header = BinaryHeader::new(FileKind::MaterializationCatalog.magic(), 2, 0);
        header.write_to(&mut file)?;
        let index = CatalogIndex::new();
        let serialized = bincode::serialize(&index)?;
        file.write_all(&serialized)?;
    }

    let index_file = IndexFile::new(index_path.clone());
    let loaded = index_file.load()?;
    assert_eq!(loaded.version, 2);

    Ok(())
}

#[test]
fn catalog_index_default_equals_new() {
    let index1 = CatalogIndex::default();
    let index2 = CatalogIndex::new();
    assert_eq!(index1.len(), index2.len());
    assert_eq!(index1.version, index2.version);
}
