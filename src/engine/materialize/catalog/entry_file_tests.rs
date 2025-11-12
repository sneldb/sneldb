use super::entry::MaterializationEntry;
use super::entry_file::{entry_file_path, EntryFile};
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationError;
use crate::test_helpers::factories::CommandFactory;
use std::fs;
use tempfile::tempdir;

fn make_entry(root: &std::path::Path, name: &str) -> MaterializationEntry {
    let spec = MaterializedQuerySpec {
        name: name.into(),
        query: Box::new(CommandFactory::query().with_event_type("orders").create()),
    };
    MaterializationEntry::new(spec, root).expect("entry")
}

#[test]
fn entry_file_path_constructs_correct_path() {
    let root = std::path::Path::new("/data/materializations");
    let path = entry_file_path(root, "my_entry");
    assert!(path.ends_with("my_entry/entry.mcatentry"));
    assert!(path.starts_with(root));
}

#[test]
fn entry_file_load_missing_file_returns_error() {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("missing.mcatentry");
    let entry_file = EntryFile::new(entry_path);

    let result = entry_file.load();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MaterializationError::Corrupt(_)));
}

#[test]
fn entry_file_persist_and_load_roundtrip() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");
    let entry_file = EntryFile::new(entry_path.clone());

    let entry = make_entry(dir.path(), "test_entry");
    entry_file.persist(&entry)?;
    assert!(entry_path.exists());

    let loaded = entry_file.load()?;
    assert_eq!(loaded.name, entry.name);
    assert_eq!(loaded.spec_hash, entry.spec_hash);
    assert_eq!(loaded.storage_path, entry.storage_path);
    Ok(())
}

#[test]
fn entry_file_persist_creates_parent_directory() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("subdir").join("entry.mcatentry");
    let entry_file = EntryFile::new(entry_path.clone());

    let entry = make_entry(dir.path(), "test_entry");
    entry_file.persist(&entry)?;

    assert!(entry_path.exists());
    assert!(entry_path.parent().unwrap().exists());
    Ok(())
}

#[test]
fn entry_file_load_corrupt_file_returns_error() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");
    fs::write(&entry_path, b"not-a-valid-entry-file").unwrap();

    let entry_file = EntryFile::new(entry_path);
    let result = entry_file.load();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MaterializationError::Corrupt(_)));
    Ok(())
}

#[test]
fn entry_file_load_file_too_small_returns_error() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");
    fs::write(&entry_path, b"short").unwrap();

    let entry_file = EntryFile::new(entry_path);
    let result = entry_file.load();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MaterializationError::Corrupt(_)));
    Ok(())
}

#[test]
fn entry_file_persist_atomic_write() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");
    let entry_file = EntryFile::new(entry_path.clone());

    let entry = make_entry(dir.path(), "test_entry");
    entry_file.persist(&entry)?;

    // Check that tmp file was removed
    let tmp_path = dir.path().join("entry.tmp");
    assert!(!tmp_path.exists());
    assert!(entry_path.exists());
    Ok(())
}

#[test]
fn entry_file_load_rejects_wrong_magic() -> Result<(), MaterializationError> {
    use crate::shared::storage_header::{BinaryHeader, FileKind};
    use std::fs::File;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");

    // Write file with wrong magic
    let mut file = File::create(&entry_path)?;
    let header = BinaryHeader::new(FileKind::MaterializedFrame.magic(), 1, 0);
    header.write_to(&mut file)?;
    let entry = make_entry(dir.path(), "test");
    let serialized = bincode::serialize(&entry)?;
    file.write_all(&serialized)?;

    let entry_file = EntryFile::new(entry_path);
    let result = entry_file.load();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), MaterializationError::Corrupt(_)));
    Ok(())
}

#[test]
fn entry_file_persist_updates_existing_entry() -> Result<(), MaterializationError> {
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entry.mcatentry");
    let entry_file = EntryFile::new(entry_path.clone());

    let mut entry1 = make_entry(dir.path(), "test_entry");
    entry1.row_count = 10;
    entry_file.persist(&entry1)?;

    let mut entry2 = make_entry(dir.path(), "test_entry");
    entry2.row_count = 20;
    entry_file.persist(&entry2)?;

    let loaded = entry_file.load()?;
    assert_eq!(loaded.row_count, 20);
    Ok(())
}

#[test]
fn entry_file_path_handles_nested_directories() {
    let root = std::path::Path::new("/data/materializations");
    let path = entry_file_path(root, "nested/deep/entry");
    assert!(path.ends_with("nested/deep/entry/entry.mcatentry"));
}

