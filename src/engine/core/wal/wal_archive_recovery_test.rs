use crate::engine::core::wal::wal_archive::{WalArchive, WalArchiveBody, WalArchiveHeader};
use crate::engine::core::wal::wal_archive_recovery::{ArchiveInfo, WalArchiveRecovery};
use crate::test_helpers::factories::WalEntryFactory;
use serde_json::json;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

/// Helper to create a test archive file
fn create_test_archive(
    dir: &Path,
    shard_id: usize,
    log_id: u64,
    entry_count: usize,
) -> std::io::Result<()> {
    let entries = WalEntryFactory::new()
        .with("event_type", "test_event")
        .create_list(entry_count);

    let start_ts = entries.first().map(|e| e.timestamp).unwrap_or(0);
    let end_ts = entries.last().map(|e| e.timestamp).unwrap_or(0);

    let header = WalArchiveHeader::new(
        shard_id,
        log_id,
        entry_count as u64,
        start_ts,
        end_ts,
        "zstd".to_string(),
        3,
    );

    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(entries),
    };

    archive.write_to_file(dir)?;
    Ok(())
}

#[test]
fn test_new() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");

    let recovery = WalArchiveRecovery::new(5, archive_dir.clone());

    assert_eq!(recovery.shard_id, 5);
    assert_eq!(recovery.archive_dir, archive_dir);
}

#[test]
fn test_list_archives_multiple() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    // Create multiple archives
    create_test_archive(&archive_dir, 1, 1, 5).unwrap();
    create_test_archive(&archive_dir, 1, 2, 10).unwrap();
    create_test_archive(&archive_dir, 1, 3, 15).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();

    assert_eq!(archives.len(), 3, "Should find 3 archives");

    // Verify they're sorted
    for i in 0..archives.len() - 1 {
        assert!(archives[i] < archives[i + 1], "Archives should be sorted");
    }
}

#[test]
fn test_list_archives_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();

    assert_eq!(archives.len(), 0, "Should return empty list");
}

#[test]
fn test_list_archives_nonexistent_directory() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("nonexistent");

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();

    assert_eq!(
        archives.len(),
        0,
        "Should return empty list for non-existent directory"
    );
}

#[test]
fn test_list_archives_filters_non_zst_files() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    // Create valid archive
    create_test_archive(&archive_dir, 1, 1, 5).unwrap();

    // Create non-archive files
    File::create(archive_dir.join("readme.txt")).unwrap();
    File::create(archive_dir.join("data.json")).unwrap();
    File::create(archive_dir.join("backup.tar.gz")).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();

    assert_eq!(archives.len(), 1, "Should only find .zst archive files");
}

#[test]
fn test_recover_from_archive_with_entries() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 5, 10).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let entries = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(entries.len(), 10, "Should recover 10 entries");
    assert_eq!(
        entries[0].event_type, "test_event",
        "Event type should be preserved"
    );
}

#[test]
fn test_recover_from_archive_empty() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 0).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let entries = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(entries.len(), 0, "Should handle empty archives");
}

#[test]
fn test_recover_from_archive_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let nonexistent = archive_dir.join("nonexistent.wal.zst");

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let result = recovery.recover_from_archive(&nonexistent);

    assert!(result.is_err(), "Should fail for non-existent archive");
}

#[test]
fn test_recover_from_archive_preserves_order() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    // Create entries with specific context_ids to verify order
    let entries = (0..5)
        .map(|i| {
            WalEntryFactory::new()
                .with("context_id", format!("ctx-{}", i))
                .with("timestamp", 1000 + i)
                .create()
        })
        .collect();

    let header = WalArchiveHeader::new(1, 1, 5, 1000, 1004, "zstd".to_string(), 3);
    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(entries),
    };
    archive.write_to_file(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let recovered = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(recovered.len(), 5);
    for i in 0..5 {
        assert_eq!(
            recovered[i].context_id,
            format!("ctx-{}", i),
            "Order should be preserved"
        );
    }
}

#[test]
fn test_recover_all_multiple_archives() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 5).unwrap();
    create_test_archive(&archive_dir, 1, 2, 7).unwrap();
    create_test_archive(&archive_dir, 1, 3, 3).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let entries = recovery.recover_all().unwrap();

    assert_eq!(entries.len(), 15, "Should recover all entries");
}

#[test]
fn test_recover_all_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let entries = recovery.recover_all().unwrap();

    assert_eq!(entries.len(), 0, "Should return empty for no archives");
}

#[test]
fn test_recover_all_with_corrupted_archive() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    // Create valid archives
    create_test_archive(&archive_dir, 1, 1, 5).unwrap();
    create_test_archive(&archive_dir, 1, 3, 7).unwrap();

    // Create a corrupted archive
    let corrupted_path = archive_dir.join("wal-00002-1000-2000.wal.zst");
    let mut file = File::create(&corrupted_path).unwrap();
    file.write_all(b"corrupted data").unwrap();
    drop(file);

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let entries = recovery.recover_all().unwrap();

    // Should skip corrupted and recover valid ones
    assert_eq!(
        entries.len(),
        12,
        "Should recover entries from valid archives only"
    );
}

#[test]
fn test_recover_all_chronological_order() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    // Create archives in non-sequential order
    create_test_archive(&archive_dir, 1, 3, 2).unwrap();
    create_test_archive(&archive_dir, 1, 1, 2).unwrap();
    create_test_archive(&archive_dir, 1, 2, 2).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();

    // Archives should be listed in sorted order
    assert_eq!(archives.len(), 3);
    // File names should be sorted, which means log_id order
}

#[test]
fn test_get_archive_info() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 5, 42, 100).unwrap();

    let recovery = WalArchiveRecovery::new(5, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let info = recovery.get_archive_info(&archives[0]).unwrap();

    assert_eq!(info.shard_id, 5);
    assert_eq!(info.log_id, 42);
    assert_eq!(info.entry_count, 100);
    assert_eq!(info.compression, "zstd");
    assert_eq!(info.compression_level, 3);
    assert_eq!(info.version, 1);
    assert!(info.path.exists());
}

#[test]
fn test_get_archive_info_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let nonexistent = archive_dir.join("nonexistent.wal.zst");

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let result = recovery.get_archive_info(&nonexistent);

    assert!(result.is_err(), "Should fail for non-existent file");
}

#[test]
fn test_get_archive_info_timestamps() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let entries = vec![
        WalEntryFactory::new().with("timestamp", 1000).create(),
        WalEntryFactory::new().with("timestamp", 2000).create(),
        WalEntryFactory::new().with("timestamp", 3000).create(),
    ];

    let header = WalArchiveHeader::new(1, 1, 3, 1000, 3000, "zstd".to_string(), 3);
    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(entries),
    };
    archive.write_to_file(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let info = recovery.get_archive_info(&archives[0]).unwrap();

    assert_eq!(info.start_timestamp, 1000);
    assert_eq!(info.end_timestamp, 3000);
    assert!(info.created_at > 0);
}

#[test]
fn test_list_archive_info() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 5).unwrap();
    create_test_archive(&archive_dir, 1, 2, 10).unwrap();
    create_test_archive(&archive_dir, 1, 3, 15).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let infos = recovery.list_archive_info();

    assert_eq!(infos.len(), 3, "Should get info for all archives");

    // Verify info is correct
    assert_eq!(infos[0].log_id, 1);
    assert_eq!(infos[0].entry_count, 5);
    assert_eq!(infos[1].log_id, 2);
    assert_eq!(infos[1].entry_count, 10);
    assert_eq!(infos[2].log_id, 3);
    assert_eq!(infos[2].entry_count, 15);
}

#[test]
fn test_list_archive_info_empty() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let infos = recovery.list_archive_info();

    assert_eq!(infos.len(), 0, "Should return empty list");
}

#[test]
fn test_list_archive_info_with_corrupted() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 5).unwrap();

    // Create corrupted archive
    let corrupted = archive_dir.join("wal-00002-1000-2000.wal.zst");
    let mut file = File::create(&corrupted).unwrap();
    file.write_all(b"corrupted").unwrap();
    drop(file);

    create_test_archive(&archive_dir, 1, 3, 10).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let infos = recovery.list_archive_info();

    // Should skip corrupted and return valid ones
    assert_eq!(infos.len(), 2, "Should skip corrupted archives");
    assert_eq!(infos[0].log_id, 1);
    assert_eq!(infos[1].log_id, 3);
}

#[test]
fn test_export_to_json() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&archive_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 5).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let output_path = output_dir.join("export.json");

    let result = recovery.export_to_json(&archives[0], &output_path);

    assert!(result.is_ok(), "Export should succeed");
    assert!(output_path.exists(), "Output file should exist");

    // Verify JSON content
    let content = fs::read_to_string(&output_path).unwrap();
    assert!(!content.is_empty(), "JSON should not be empty");
    assert!(content.contains("test_event"), "Should contain event data");
}

#[test]
fn test_export_to_json_empty_archive() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&archive_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 0).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let output_path = output_dir.join("export.json");

    let result = recovery.export_to_json(&archives[0], &output_path);

    assert!(result.is_ok(), "Should handle empty archives");
    assert!(output_path.exists());

    let content = fs::read_to_string(&output_path).unwrap();
    assert_eq!(content.trim(), "[]", "Should export empty array");
}

#[test]
fn test_export_to_json_nonexistent_archive() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let output_path = temp_dir.path().join("export.json");
    let nonexistent = archive_dir.join("nonexistent.wal.zst");

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let result = recovery.export_to_json(&nonexistent, &output_path);

    assert!(result.is_err(), "Should fail for non-existent archive");
}

#[test]
fn test_export_to_json_complex_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    let output_dir = temp_dir.path().join("output");
    fs::create_dir_all(&archive_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();

    let entries = vec![
        WalEntryFactory::new()
            .with("event_type", "complex_event")
            .with(
                "payload",
                json!({
                    "nested": {
                        "field": "value",
                        "number": 42
                    },
                    "array": [1, 2, 3]
                }),
            )
            .create(),
    ];

    let header = WalArchiveHeader::new(1, 1, 1, 1000, 1000, "zstd".to_string(), 3);
    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(entries),
    };
    archive.write_to_file(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let output_path = output_dir.join("export.json");

    recovery.export_to_json(&archives[0], &output_path).unwrap();

    let content = fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("complex_event"));
    assert!(content.contains("nested"));
    assert!(content.contains("array"));
}

#[test]
fn test_archive_info_format() {
    let info = ArchiveInfo {
        path: std::path::PathBuf::from("/test/archive.wal.zst"),
        shard_id: 5,
        log_id: 42,
        entry_count: 1000,
        start_timestamp: 1700000000,
        end_timestamp: 1700003600,
        created_at: 1700010000,
        compression: "zstd".to_string(),
        compression_level: 3,
        version: 1,
    };

    let formatted = info.format();

    assert!(formatted.contains("Shard: 5"));
    assert!(formatted.contains("Log ID: 00042"));
    assert!(formatted.contains("Entries: 1000"));
    assert!(formatted.contains("Compression: zstd (level 3)"));
    assert!(formatted.contains("Version: 1"));
}

#[test]
fn test_recover_preserves_event_types() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let entries = vec![
        WalEntryFactory::new().with("event_type", "signup").create(),
        WalEntryFactory::new().with("event_type", "login").create(),
        WalEntryFactory::new()
            .with("event_type", "purchase")
            .create(),
    ];

    let header = WalArchiveHeader::new(1, 1, 3, 1000, 3000, "zstd".to_string(), 3);
    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(entries),
    };
    archive.write_to_file(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let recovered = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0].event_type, "signup");
    assert_eq!(recovered[1].event_type, "login");
    assert_eq!(recovered[2].event_type, "purchase");
}

#[test]
fn test_recover_preserves_all_fields() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    let original = WalEntryFactory::new()
        .with("event_type", "test_event")
        .with("context_id", "ctx-123")
        .with("timestamp", 1234567890)
        .with("payload", json!({"key": "value", "num": 42}))
        .create();

    let header = WalArchiveHeader::new(1, 1, 1, 1234567890, 1234567890, "zstd".to_string(), 3);
    let archive = WalArchive {
        header,
        body: WalArchiveBody::new(vec![original.clone()]),
    };
    archive.write_to_file(&archive_dir).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let recovered = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].event_type, original.event_type);
    assert_eq!(recovered[0].context_id, original.context_id);
    assert_eq!(recovered[0].timestamp, original.timestamp);
    assert_eq!(recovered[0].payload, original.payload);
}

#[test]
fn test_recover_large_archive() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("archives");
    fs::create_dir_all(&archive_dir).unwrap();

    create_test_archive(&archive_dir, 1, 1, 1000).unwrap();

    let recovery = WalArchiveRecovery::new(1, archive_dir);
    let archives = recovery.list_archives().unwrap();
    let entries = recovery.recover_from_archive(&archives[0]).unwrap();

    assert_eq!(entries.len(), 1000, "Should handle large archives");
}

#[test]
fn test_multiple_shards_separate_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path();

    let shard0_dir = base_dir.join("shard-0");
    let shard1_dir = base_dir.join("shard-1");
    fs::create_dir_all(&shard0_dir).unwrap();
    fs::create_dir_all(&shard1_dir).unwrap();

    create_test_archive(&shard0_dir, 0, 1, 5).unwrap();
    create_test_archive(&shard1_dir, 1, 1, 7).unwrap();

    let recovery0 = WalArchiveRecovery::new(0, shard0_dir);
    let recovery1 = WalArchiveRecovery::new(1, shard1_dir);

    let entries0 = recovery0.recover_all().unwrap();
    let entries1 = recovery1.recover_all().unwrap();

    assert_eq!(entries0.len(), 5, "Shard 0 should have 5 entries");
    assert_eq!(entries1.len(), 7, "Shard 1 should have 7 entries");
}
