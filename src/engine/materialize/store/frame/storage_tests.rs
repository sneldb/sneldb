use super::storage::FrameStorage;
use tempfile::tempdir;

#[test]
fn storage_creates_directory_and_provides_reader_writer() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path().join("frames")).unwrap();

    assert!(storage.path().exists());
    let _writer = storage.writer();
    let _reader = storage.reader();
}

#[test]
fn storage_remove_deletes_files() {
    let dir = tempdir().unwrap();
    let storage = FrameStorage::create(dir.path()).unwrap();
    let path = storage.path().join("000001.mat");
    std::fs::write(&path, b"test").unwrap();

    assert!(path.exists());
    storage.remove("000001.mat");
    assert!(!path.exists());
}

