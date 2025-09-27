use crate::engine::core::WalCleaner;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;

#[test]
fn test_wal_cleaner_removes_old_logs() {
    // Create a temporary directory for testing
    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
    let wal_dir = temp_dir.path().to_path_buf();
    let shard_id = 7;
    let shard_dir = wal_dir.join(format!("shard-{}", shard_id));

    // Clean up any existing files and create directory
    if shard_dir.exists() {
        fs::remove_dir_all(&shard_dir).unwrap();
    }
    fs::create_dir_all(&shard_dir).unwrap();

    // Create test WAL log files: 0..5
    for i in 0..5 {
        let file_path = shard_dir.join(format!("wal-{:05}.log", i));
        let mut f = File::create(&file_path).unwrap();
        writeln!(f, "{{\"test\": {}}}", i).unwrap();
    }

    // Sanity check: files created
    let mut existing: Vec<_> = fs::read_dir(&shard_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    existing.sort();
    assert_eq!(existing.len(), 5);
    assert_eq!(existing[0], "wal-00000.log");

    // Run cleaner to keep only log ID >= 3
    let cleaner = WalCleaner::with_wal_dir(shard_id, shard_dir.clone());
    cleaner.cleanup_up_to(3);

    // Check remaining files
    let mut remaining: Vec<_> = fs::read_dir(&shard_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    remaining.sort();

    assert_eq!(remaining, vec!["wal-00003.log", "wal-00004.log"]);

    // Temporary directory will be automatically cleaned up when temp_dir goes out of scope
}
