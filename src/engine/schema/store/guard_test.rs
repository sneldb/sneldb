use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::store::guard::FileLockGuard;
use fs2::FileExt;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn shared_lock_acquires_and_releases() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();

    {
        let _guard = FileLockGuard::new_shared(file).unwrap();
        // Guard should hold the lock
        let file2 = File::open(&path).unwrap();
        assert!(file2.try_lock_shared().is_ok()); // Shared locks can coexist
    }
    // Guard dropped, lock should be released
    let file3 = File::open(&path).unwrap();
    assert!(file3.try_lock_exclusive().is_ok());
}

#[test]
fn exclusive_lock_acquires_and_releases() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();

    {
        let _guard = FileLockGuard::new_exclusive(file).unwrap();
        // Guard should hold the lock
        let file2 = File::open(&path).unwrap();
        assert!(file2.try_lock_exclusive().is_err()); // Exclusive lock prevents others
        assert!(file2.try_lock_shared().is_err()); // Exclusive lock prevents shared too
    }
    // Guard dropped, lock should be released
    let file3 = File::open(&path).unwrap();
    assert!(file3.try_lock_exclusive().is_ok());
}

#[test]
fn exclusive_lock_prevents_shared_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();

    let _guard = FileLockGuard::new_exclusive(file).unwrap();
    let file2 = File::open(&path).unwrap();
    let result = FileLockGuard::new_shared(file2);
    assert!(matches!(result, Err(SchemaError::IoReadFailed(_))));
}

#[test]
fn shared_lock_allows_multiple_readers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file1 = File::create(&path).unwrap();

    let _guard1 = FileLockGuard::new_shared(file1).unwrap();
    let file2 = File::open(&path).unwrap();
    let _guard2 = FileLockGuard::new_shared(file2).unwrap();
    // Both guards should hold shared locks successfully
    let file3 = File::open(&path).unwrap();
    assert!(file3.try_lock_shared().is_ok());
}

#[test]
fn get_mut_provides_mutable_access() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();

    let guard = FileLockGuard::new_exclusive(file).unwrap();
    let file_mut = guard.get_mut();
    // Should be able to write
    use std::io::Write;
    file_mut.write_all(b"test").unwrap();
    file_mut.sync_all().unwrap();
}

#[test]
fn lock_fails_when_already_locked_exclusively() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file1 = File::create(&path).unwrap();
    let _file1_guard = file1.try_lock_exclusive().unwrap();

    let file2 = File::open(&path).unwrap();
    let result = FileLockGuard::new_exclusive(file2);
    assert!(matches!(result, Err(SchemaError::IoWriteFailed(_))));
}

#[test]
fn lock_fails_when_already_locked_shared() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file1 = File::create(&path).unwrap();
    let _file1_guard = file1.try_lock_shared().unwrap();

    let file2 = File::open(&path).unwrap();
    // Shared lock should still work
    let _guard2 = FileLockGuard::new_shared(file2).unwrap();
    // But exclusive should fail
    let file3 = File::open(&path).unwrap();
    let result = FileLockGuard::new_exclusive(file3);
    assert!(matches!(result, Err(SchemaError::IoWriteFailed(_))));
}
