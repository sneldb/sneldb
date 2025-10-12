use crate::engine::core::WalEntry;
use crate::engine::core::wal::wal_archive::WalArchive;
use crate::engine::core::wal::wal_archiver::WalArchiver;
use crate::test_helpers::factories::WalEntryFactory;
use serde_json::json;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Helper function to create a test WAL file with given entries
fn create_test_wal_file(
    dir: &Path,
    log_id: u64,
    entries: Vec<WalEntry>,
) -> std::io::Result<PathBuf> {
    let wal_path = dir.join(format!("wal-{:05}.log", log_id));
    let mut file = File::create(&wal_path)?;

    for entry in entries {
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
    }

    Ok(wal_path)
}

/// Helper function to count WAL log files in a directory
#[allow(dead_code)]
fn count_wal_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| ext == "log")
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

/// Helper function to count archive files in a directory
#[allow(dead_code)]
fn count_archive_files(dir: &Path) -> usize {
    if !dir.exists() {
        return 0;
    }
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| {
                    e.path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| ext == "zst")
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

#[test]
fn test_archive_single_log_with_factory() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create test WAL file using factory
    let entries = WalEntryFactory::new()
        .with("event_type", "user_signup")
        .create_list(10);
    create_test_wal_file(&wal_dir, 5, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive the log
    let result = archiver.archive_log(5);
    assert!(result.is_ok(), "Archive should succeed");

    let archive_path = result.unwrap();
    assert!(archive_path.exists(), "Archive file should exist");
    assert!(
        archive_path.to_string_lossy().contains("wal-00005-"),
        "Archive filename should contain log ID"
    );
    assert!(
        archive_path.to_string_lossy().ends_with(".wal.zst"),
        "Archive should have .wal.zst extension"
    );

    // Verify we can read it back
    let archive = WalArchive::read_from_file(&archive_path).unwrap();
    assert_eq!(archive.header.entry_count, 10, "Should have 10 entries");
    assert_eq!(archive.header.log_id, 5, "Log ID should be 5");
    assert_eq!(
        archive.body.entries.len(),
        10,
        "Body should have 10 entries"
    );
    assert_eq!(
        archive.body.entries[0].event_type, "user_signup",
        "Event type should be preserved"
    );
}

#[test]
fn test_archive_multiple_logs_sequential() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create multiple test WAL files with different event types
    for i in 0..5 {
        let entries = WalEntryFactory::new()
            .with("event_type", format!("event_type_{}", i))
            .create_list(5);
        create_test_wal_file(&wal_dir, i, entries).unwrap();
    }

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive logs 0, 1, 2 (keep from 3)
    let results = archiver.archive_logs_up_to(3);
    assert_eq!(results.len(), 3, "Should archive 3 logs");
    assert!(
        results.iter().all(|r| r.is_ok()),
        "All archives should succeed"
    );

    // Verify archives exist
    let archives = archiver.list_archives().unwrap();
    assert_eq!(archives.len(), 3, "Should have 3 archives");

    // Verify each archive has correct content
    for (i, archive_path) in archives.iter().enumerate() {
        let archive = WalArchive::read_from_file(archive_path).unwrap();
        assert_eq!(
            archive.header.log_id, i as u64,
            "Archive {} should have log_id {}",
            i, i
        );
        assert_eq!(
            archive.body.entries.len(),
            5,
            "Archive {} should have 5 entries",
            i
        );
        assert_eq!(
            archive.body.entries[0].event_type,
            format!("event_type_{}", i),
            "Archive {} should have correct event type",
            i
        );
    }
}

#[test]
fn test_archive_empty_log() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create empty WAL file
    create_test_wal_file(&wal_dir, 0, vec![]).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive should succeed even for empty files
    let result = archiver.archive_log(0);
    assert!(result.is_ok(), "Empty log archive should succeed");

    let archive = WalArchive::read_from_file(&result.unwrap()).unwrap();
    assert_eq!(archive.header.entry_count, 0, "Should have 0 entries");
    assert_eq!(archive.body.entries.len(), 0, "Body should be empty");
}

#[test]
fn test_archive_nonexistent_log() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Try to archive non-existent log
    let result = archiver.archive_log(999);
    assert!(result.is_err(), "Should fail for non-existent log");
}

#[test]
fn test_archive_with_various_compression_levels() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create test WAL file with substantial data
    let entries = WalEntryFactory::new()
        .with("payload", json!({"data": "x".repeat(100)}))
        .create_list(50);
    create_test_wal_file(&wal_dir, 1, entries).unwrap();

    // Test different compression levels
    for level in [1, 3, 9, 19] {
        let archive_dir = temp_dir.path().join(format!("archive_{}", level));
        let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), level);

        let result = archiver.archive_log(1);
        assert!(
            result.is_ok(),
            "Archive should succeed with level {}",
            level
        );

        let archive_path = result.unwrap();
        let file_size = fs::metadata(&archive_path).unwrap().len();
        assert!(
            file_size > 0,
            "Archive should have non-zero size at level {}",
            level
        );

        // Verify archive is readable
        let archive = WalArchive::read_from_file(&archive_path).unwrap();
        assert_eq!(
            archive.header.compression_level, level,
            "Archive should record compression level"
        );
        assert_eq!(
            archive.header.entry_count, 50,
            "Should have 50 entries at level {}",
            level
        );
    }
}

#[test]
fn test_archive_preserves_event_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL entries with specific metadata
    let entries = vec![
        WalEntryFactory::new()
            .with("event_type", "user_signup")
            .with("context_id", "user-123")
            .with("timestamp", 1700000000)
            .with(
                "payload",
                json!({"email": "test@example.com", "plan": "pro"}),
            )
            .create(),
        WalEntryFactory::new()
            .with("event_type", "purchase")
            .with("context_id", "user-456")
            .with("timestamp", 1700003600)
            .with("payload", json!({"amount": 99.99, "currency": "USD"}))
            .create(),
    ];

    create_test_wal_file(&wal_dir, 10, entries.clone()).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(10).unwrap();
    let archive = WalArchive::read_from_file(&archive_path).unwrap();

    // Verify all metadata is preserved
    assert_eq!(archive.body.entries.len(), 2);

    let recovered_entry1 = &archive.body.entries[0];
    assert_eq!(recovered_entry1.event_type, "user_signup");
    assert_eq!(recovered_entry1.context_id, "user-123");
    assert_eq!(recovered_entry1.timestamp, 1700000000);
    assert_eq!(recovered_entry1.payload["email"], json!("test@example.com"));
    assert_eq!(recovered_entry1.payload["plan"], json!("pro"));

    let recovered_entry2 = &archive.body.entries[1];
    assert_eq!(recovered_entry2.event_type, "purchase");
    assert_eq!(recovered_entry2.context_id, "user-456");
    assert_eq!(recovered_entry2.timestamp, 1700003600);
    assert_eq!(recovered_entry2.payload["amount"], json!(99.99));
    assert_eq!(recovered_entry2.payload["currency"], json!("USD"));
}

#[test]
fn test_archive_with_malformed_json_lines() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL file with some malformed lines
    let wal_path = wal_dir.join("wal-00001.log");
    let mut file = File::create(&wal_path).unwrap();

    // Valid entry
    let entry1 = WalEntryFactory::new().create();
    writeln!(file, "{}", serde_json::to_string(&entry1).unwrap()).unwrap();

    // Malformed JSON
    writeln!(file, "{{invalid json").unwrap();

    // Valid entry
    let entry2 = WalEntryFactory::new().with("context_id", "ctx2").create();
    writeln!(file, "{}", serde_json::to_string(&entry2).unwrap()).unwrap();

    // Empty line (should be skipped)
    writeln!(file, "").unwrap();

    drop(file);

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive should succeed and skip malformed lines
    let result = archiver.archive_log(1);
    assert!(
        result.is_ok(),
        "Archive should succeed despite malformed lines"
    );

    let archive = WalArchive::read_from_file(&result.unwrap()).unwrap();
    assert_eq!(archive.header.entry_count, 2, "Should have 2 valid entries");
    assert_eq!(archive.body.entries.len(), 2, "Body should have 2 entries");
}

#[test]
fn test_list_archives_sorted() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL files in non-sequential order
    for log_id in [5, 2, 8, 1, 3] {
        let entries = WalEntryFactory::new().create_list(3);
        create_test_wal_file(&wal_dir, log_id, entries).unwrap();
    }

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive all
    archiver.archive_logs_up_to(10);

    // List should be sorted
    let archives = archiver.list_archives().unwrap();
    assert_eq!(archives.len(), 5, "Should have 5 archives");

    // Extract log IDs from filenames and verify they're sorted
    let log_ids: Vec<u64> = archives
        .iter()
        .filter_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|s| {
                    s.strip_prefix("wal-")
                        .and_then(|s| s.split('-').next())
                        .and_then(|id| id.parse::<u64>().ok())
                })
        })
        .collect();

    assert_eq!(log_ids, vec![1, 2, 3, 5, 8], "Log IDs should be sorted");
}

#[test]
fn test_archive_logs_up_to_boundary() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create logs 0-9
    for i in 0..10 {
        let entries = WalEntryFactory::new().create_list(2);
        create_test_wal_file(&wal_dir, i, entries).unwrap();
    }

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);

    // Archive logs < 5 (should archive 0,1,2,3,4)
    let results = archiver.archive_logs_up_to(5);
    assert_eq!(results.len(), 5, "Should archive 5 logs");
    assert!(results.iter().all(|r| r.is_ok()), "All should succeed");

    let archives = archiver.list_archives().unwrap();
    assert_eq!(archives.len(), 5, "Should have 5 archives");

    // Verify archived logs are 0-4
    let log_ids: Vec<u64> = archives
        .iter()
        .filter_map(|path| {
            let archive = WalArchive::read_from_file(path).ok()?;
            Some(archive.header.log_id)
        })
        .collect();

    assert_eq!(log_ids, vec![0, 1, 2, 3, 4], "Should archive logs 0-4");
}

#[test]
fn test_archive_with_different_event_types() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL with multiple event types
    let entries = vec![
        WalEntryFactory::new()
            .with("event_type", "user_signup")
            .create(),
        WalEntryFactory::new()
            .with("event_type", "user_login")
            .create(),
        WalEntryFactory::new()
            .with("event_type", "purchase")
            .create(),
        WalEntryFactory::new()
            .with("event_type", "user_logout")
            .create(),
    ];

    create_test_wal_file(&wal_dir, 1, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(1).unwrap();
    let archive = WalArchive::read_from_file(&archive_path).unwrap();

    assert_eq!(archive.header.entry_count, 4);

    // Verify all event types are preserved
    let event_types: Vec<&str> = archive
        .body
        .entries
        .iter()
        .map(|e| e.event_type.as_str())
        .collect();

    assert_eq!(
        event_types,
        vec!["user_signup", "user_login", "purchase", "user_logout"]
    );
}

#[test]
fn test_archive_timestamp_range_in_header() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create entries with specific timestamps
    let entries = vec![
        WalEntryFactory::new().with("timestamp", 1000).create(),
        WalEntryFactory::new().with("timestamp", 2000).create(),
        WalEntryFactory::new().with("timestamp", 1500).create(),
        WalEntryFactory::new().with("timestamp", 3000).create(),
    ];

    create_test_wal_file(&wal_dir, 1, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(1).unwrap();
    let archive = WalArchive::read_from_file(&archive_path).unwrap();

    // Verify timestamp range
    assert_eq!(
        archive.header.start_timestamp, 1000,
        "Start timestamp should be minimum"
    );
    assert_eq!(
        archive.header.end_timestamp, 3000,
        "End timestamp should be maximum"
    );
}

#[test]
fn test_archive_filename_contains_timestamp_range() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    let entries = vec![
        WalEntryFactory::new()
            .with("timestamp", 1700000000)
            .create(),
        WalEntryFactory::new()
            .with("timestamp", 1700003600)
            .create(),
    ];

    create_test_wal_file(&wal_dir, 42, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(42).unwrap();

    let filename = archive_path.file_name().unwrap().to_string_lossy();
    assert!(filename.contains("wal-00042-"), "Should contain log ID");
    assert!(
        filename.contains("1700000000"),
        "Should contain start timestamp"
    );
    assert!(
        filename.contains("1700003600"),
        "Should contain end timestamp"
    );
    assert!(
        filename.ends_with(".wal.zst"),
        "Should have .wal.zst extension"
    );
}

#[test]
fn test_archive_large_payload() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create entry with large payload
    let large_data = "x".repeat(10000);
    let entries = vec![
        WalEntryFactory::new()
            .with("payload", json!({"data": large_data.clone()}))
            .create(),
    ];

    create_test_wal_file(&wal_dir, 1, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(1).unwrap();
    let archive = WalArchive::read_from_file(&archive_path).unwrap();

    assert_eq!(archive.header.entry_count, 1);
    assert_eq!(
        archive.body.entries[0].payload["data"],
        json!(large_data),
        "Large payload should be preserved"
    );
}

#[test]
fn test_archive_creates_directory_if_not_exists() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("nested/deep/archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Archive dir doesn't exist yet
    assert!(!archive_dir.exists());

    let entries = WalEntryFactory::new().create_list(5);
    create_test_wal_file(&wal_dir, 1, entries).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let result = archiver.archive_log(1);

    assert!(result.is_ok(), "Should create archive directory");
    assert!(archive_dir.exists(), "Archive directory should exist");
}

#[test]
fn test_archive_compression_ratio() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    // Create WAL with highly compressible data
    let entries = WalEntryFactory::new()
        .with("payload", json!({"data": "a".repeat(1000)}))
        .create_list(100);
    let wal_path = create_test_wal_file(&wal_dir, 1, entries).unwrap();

    let original_size = fs::metadata(&wal_path).unwrap().len();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archive_path = archiver.archive_log(1).unwrap();

    let compressed_size = fs::metadata(&archive_path).unwrap().len();

    // Verify compression occurred (should be much smaller)
    assert!(
        compressed_size < original_size / 5,
        "Compressed size {} should be less than 20% of original {}",
        compressed_size,
        original_size
    );
}

#[test]
fn test_list_archives_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();
    fs::create_dir_all(&archive_dir).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archives = archiver.list_archives().unwrap();

    assert_eq!(archives.len(), 0, "Should return empty list");
}

#[test]
fn test_list_archives_nonexistent_directory() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("nonexistent");
    fs::create_dir_all(&wal_dir).unwrap();

    let archiver = WalArchiver::with_dirs(1, wal_dir.clone(), archive_dir.clone(), 3);
    let archives = archiver.list_archives().unwrap();

    assert_eq!(
        archives.len(),
        0,
        "Should return empty list for non-existent directory"
    );
}

#[test]
fn test_archive_header_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    let archive_dir = temp_dir.path().join("archive");
    fs::create_dir_all(&wal_dir).unwrap();

    let entries = WalEntryFactory::new().create_list(7);
    create_test_wal_file(&wal_dir, 123, entries).unwrap();

    let archiver = WalArchiver::with_dirs(42, wal_dir.clone(), archive_dir.clone(), 9);
    let archive_path = archiver.archive_log(123).unwrap();
    let archive = WalArchive::read_from_file(&archive_path).unwrap();

    // Verify header metadata
    assert_eq!(archive.header.version, 1, "Should use version 1");
    assert_eq!(archive.header.shard_id, 42, "Should record shard_id");
    assert_eq!(archive.header.log_id, 123, "Should record log_id");
    assert_eq!(archive.header.entry_count, 7, "Should record entry count");
    assert_eq!(
        archive.header.compression, "zstd",
        "Should record compression algorithm"
    );
    assert_eq!(
        archive.header.compression_level, 9,
        "Should record compression level"
    );
    assert!(
        archive.header.created_at > 0,
        "Should have creation timestamp"
    );
}
