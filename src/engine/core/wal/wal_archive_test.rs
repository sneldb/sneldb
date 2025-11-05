use crate::engine::core::WalEntry;
use crate::engine::core::wal::wal_archive::{
    ARCHIVE_VERSION, WalArchive, WalArchiveBody, WalArchiveHeader,
};
use crate::test_helpers::factories::WalEntryFactory;
use serde_json::json;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

/// Helper to create a test WAL file
fn create_test_wal_file(path: &Path, entries: Vec<WalEntry>) -> std::io::Result<()> {
    let mut file = File::create(path)?;
    for entry in entries {
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
    }
    Ok(())
}

#[test]
fn test_archive_header_new() {
    let header = WalArchiveHeader::new(1, 42, 100, 1000, 2000, "zstd".to_string(), 3);

    assert_eq!(header.version, ARCHIVE_VERSION);
    assert_eq!(header.shard_id, 1);
    assert_eq!(header.log_id, 42);
    assert_eq!(header.entry_count, 100);
    assert_eq!(header.start_timestamp, 1000);
    assert_eq!(header.end_timestamp, 2000);
    assert_eq!(header.compression, "zstd");
    assert_eq!(header.compression_level, 3);
    assert!(header.created_at > 0, "Should have creation timestamp");
}

#[test]
fn test_archive_body_new() {
    let entries = WalEntryFactory::new().create_list(5);
    let body = WalArchiveBody::new(entries.clone());

    assert_eq!(body.entries.len(), 5);
    // Verify entries match by checking key fields
    for (i, entry) in body.entries.iter().enumerate() {
        assert_eq!(entry.timestamp, entries[i].timestamp);
        assert_eq!(entry.context_id, entries[i].context_id);
        assert_eq!(entry.event_type, entries[i].event_type);
    }
}

#[test]
fn test_from_wal_file_with_entries() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal-00001.log");

    let entries = vec![
        WalEntryFactory::new()
            .with("timestamp", 1700000000)
            .with("event_type", "signup")
            .create(),
        WalEntryFactory::new()
            .with("timestamp", 1700000100)
            .with("event_type", "login")
            .create(),
        WalEntryFactory::new()
            .with("timestamp", 1700000200)
            .with("event_type", "logout")
            .create(),
    ];

    create_test_wal_file(&wal_path, entries.clone()).unwrap();

    let archive = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3).unwrap();

    assert_eq!(archive.header.shard_id, 1);
    assert_eq!(archive.header.log_id, 5);
    assert_eq!(archive.header.entry_count, 3);
    assert_eq!(archive.header.start_timestamp, 1700000000);
    assert_eq!(archive.header.end_timestamp, 1700000200);
    assert_eq!(archive.header.compression, "zstd");
    assert_eq!(archive.header.compression_level, 3);
    assert_eq!(archive.body.entries.len(), 3);
    assert_eq!(archive.body.entries[0].event_type, "signup");
    assert_eq!(archive.body.entries[1].event_type, "login");
    assert_eq!(archive.body.entries[2].event_type, "logout");
}

#[test]
fn test_from_wal_file_empty() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal-00001.log");

    create_test_wal_file(&wal_path, vec![]).unwrap();

    let archive = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3).unwrap();

    assert_eq!(archive.header.entry_count, 0);
    assert_eq!(archive.header.start_timestamp, 0);
    assert_eq!(archive.header.end_timestamp, 0);
    assert_eq!(archive.body.entries.len(), 0);
}

#[test]
fn test_from_wal_file_with_blank_lines() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal-00001.log");

    let mut file = File::create(&wal_path).unwrap();
    let entry = WalEntryFactory::new().create();

    writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();
    writeln!(file, "").unwrap(); // blank line
    writeln!(file, "   ").unwrap(); // whitespace line
    writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();

    drop(file);

    let archive = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3).unwrap();

    assert_eq!(archive.header.entry_count, 2, "Should skip blank lines");
}

#[test]
fn test_from_wal_file_with_invalid_json() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal-00001.log");

    let mut file = File::create(&wal_path).unwrap();
    let entry = WalEntryFactory::new().create();

    writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();
    writeln!(file, "{{invalid json").unwrap();
    writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();

    drop(file);

    let archive = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3).unwrap();

    assert_eq!(
        archive.header.entry_count, 2,
        "Should skip invalid JSON lines"
    );
}

#[test]
fn test_from_wal_file_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("nonexistent.log");

    let result = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3);

    assert!(result.is_err(), "Should fail for non-existent file");
}

#[test]
fn test_from_wal_file_timestamp_range() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal-00001.log");

    let entries = vec![
        WalEntryFactory::new().with("timestamp", 5000).create(),
        WalEntryFactory::new().with("timestamp", 3000).create(),
        WalEntryFactory::new().with("timestamp", 7000).create(),
        WalEntryFactory::new().with("timestamp", 1000).create(),
    ];

    create_test_wal_file(&wal_path, entries).unwrap();

    let archive = WalArchive::from_wal_file(&wal_path, 1, 5, "zstd".to_string(), 3).unwrap();

    assert_eq!(
        archive.header.start_timestamp, 1000,
        "Should find minimum timestamp"
    );
    assert_eq!(
        archive.header.end_timestamp, 7000,
        "Should find maximum timestamp"
    );
}

#[test]
fn test_to_compressed_bytes_and_back() {
    let entries = WalEntryFactory::new().create_list(10);
    let header = WalArchiveHeader::new(1, 42, 10, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries.clone());
    let archive = WalArchive { header, body };

    let compressed = archive.to_compressed_bytes().unwrap();

    assert!(compressed.len() > 0, "Should produce compressed data");

    let recovered = WalArchive::from_compressed_bytes(&compressed).unwrap();

    assert_eq!(recovered.header.shard_id, 1);
    assert_eq!(recovered.header.log_id, 42);
    assert_eq!(recovered.header.entry_count, 10);
    assert_eq!(recovered.body.entries.len(), 10);
}

#[test]
fn test_compression_reduces_size() {
    // Create entries with repetitive data (highly compressible)
    let entries = WalEntryFactory::new()
        .with("payload", json!({"data": "x".repeat(1000)}))
        .create_list(50);

    let header = WalArchiveHeader::new(1, 42, 50, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries.clone());
    let archive = WalArchive { header, body };

    // Serialize without compression for comparison
    let uncompressed = rmp_serde::to_vec(&archive).unwrap();
    let compressed = archive.to_compressed_bytes().unwrap();

    assert!(
        compressed.len() < uncompressed.len(),
        "Compressed size {} should be less than uncompressed {}",
        compressed.len(),
        uncompressed.len()
    );

    // Should compress to less than 50% for this repetitive data
    assert!(
        compressed.len() < uncompressed.len() / 2,
        "Should achieve at least 50% compression"
    );
}

#[test]
fn test_compression_levels() {
    let entries = WalEntryFactory::new()
        .with("payload", json!({"data": "test".repeat(100)}))
        .create_list(20);

    let mut sizes = Vec::new();

    for level in [1, 3, 9, 19] {
        let header = WalArchiveHeader::new(1, 42, 20, 1000, 2000, "zstd".to_string(), level);
        let body = WalArchiveBody::new(entries.clone());
        let archive = WalArchive { header, body };

        let compressed = archive.to_compressed_bytes().unwrap();
        sizes.push((level, compressed.len()));

        // Verify it can be decompressed
        let recovered = WalArchive::from_compressed_bytes(&compressed).unwrap();
        assert_eq!(recovered.body.entries.len(), 20);
    }

    // Generally, higher compression levels should produce smaller or equal sizes
    // (though not strictly monotonic due to algorithm quirks)
    assert!(
        sizes[3].1 <= sizes[0].1,
        "Level 19 should compress better than level 1"
    );
}

#[test]
fn test_from_compressed_bytes_invalid_data() {
    let invalid_data = vec![0u8; 100];

    let result = WalArchive::from_compressed_bytes(&invalid_data);

    assert!(result.is_err(), "Should fail for invalid compressed data");
}

#[test]
fn test_from_compressed_bytes_corrupted_zstd() {
    // Create valid archive, then corrupt it
    let entries = WalEntryFactory::new().create_list(5);
    let header = WalArchiveHeader::new(1, 42, 5, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries);
    let archive = WalArchive { header, body };

    let mut compressed = archive.to_compressed_bytes().unwrap();

    // Corrupt some bytes in the middle
    if compressed.len() > 10 {
        let mid_idx = compressed.len() / 2;
        compressed[mid_idx] ^= 0xFF;
        compressed[mid_idx + 1] ^= 0xFF;
    }

    let result = WalArchive::from_compressed_bytes(&compressed);

    assert!(result.is_err(), "Should fail for corrupted data");
}

#[test]
fn test_generate_filename() {
    let header = WalArchiveHeader::new(1, 42, 10, 1700000000, 1700003600, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(vec![]);
    let archive = WalArchive { header, body };

    let filename = archive.generate_filename();

    assert_eq!(filename, "wal-00042-1700000000-1700003600.wal.zst");
}

#[test]
fn test_generate_filename_format() {
    let header = WalArchiveHeader::new(1, 5, 10, 100, 200, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(vec![]);
    let archive = WalArchive { header, body };

    let filename = archive.generate_filename();

    assert!(filename.starts_with("wal-"));
    assert!(filename.contains("-100-200"));
    assert!(filename.ends_with(".wal.zst"));

    // Verify log_id is zero-padded to 5 digits
    assert!(filename.contains("wal-00005-"));
}

#[test]
fn test_write_to_file_and_read_back() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path();

    let entries = WalEntryFactory::new()
        .with("event_type", "test_event")
        .create_list(5);
    let header = WalArchiveHeader::new(1, 42, 5, 1700000000, 1700003600, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries);
    let archive = WalArchive {
        header: header.clone(),
        body,
    };

    let archive_path = archive.write_to_file(archive_dir).unwrap();

    assert!(archive_path.exists(), "Archive file should exist");
    assert!(
        archive_path.to_string_lossy().ends_with(".wal.zst"),
        "Should have correct extension"
    );

    let recovered = WalArchive::read_from_file(&archive_path).unwrap();

    assert_eq!(recovered.header.shard_id, header.shard_id);
    assert_eq!(recovered.header.log_id, header.log_id);
    assert_eq!(recovered.header.entry_count, 5);
    assert_eq!(recovered.body.entries.len(), 5);
}

#[test]
fn test_write_to_file_creates_directory() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path().join("nested/deep/archive");

    assert!(!archive_dir.exists(), "Directory should not exist yet");

    let entries = WalEntryFactory::new().create_list(3);
    let header = WalArchiveHeader::new(1, 42, 3, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries);
    let archive = WalArchive { header, body };

    let result = archive.write_to_file(&archive_dir);

    assert!(result.is_ok(), "Should create directory and write file");
    assert!(archive_dir.exists(), "Directory should be created");
}

#[test]
fn test_read_from_file_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let nonexistent = temp_dir.path().join("nonexistent.wal.zst");

    let result = WalArchive::read_from_file(&nonexistent);

    assert!(result.is_err(), "Should fail for non-existent file");
}

#[test]
fn test_read_from_file_invalid_data() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("invalid.wal.zst");

    let mut file = File::create(&invalid_path).unwrap();
    file.write_all(b"not a valid archive").unwrap();
    drop(file);

    let result = WalArchive::read_from_file(&invalid_path);

    assert!(result.is_err(), "Should fail for invalid archive data");
}

#[test]
fn test_roundtrip_with_complex_payloads() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path();

    let entries = vec![
        WalEntryFactory::new()
            .with("event_type", "user_action")
            .with("context_id", "user-123")
            .with("timestamp", 1700000000)
            .with(
                "payload",
                json!({
                    "action": "purchase",
                    "items": [
                        {"id": 1, "name": "Widget", "price": 29.99},
                        {"id": 2, "name": "Gadget", "price": 49.99}
                    ],
                    "total": 79.98,
                    "metadata": {
                        "ip": "192.168.1.1",
                        "user_agent": "Mozilla/5.0"
                    }
                }),
            )
            .create(),
        WalEntryFactory::new()
            .with("event_type", "system_event")
            .with("context_id", "sys-456")
            .with("timestamp", 1700000100)
            .with(
                "payload",
                json!({
                    "level": "error",
                    "message": "Database connection failed",
                    "stack_trace": ["line 1", "line 2", "line 3"]
                }),
            )
            .create(),
    ];

    let header = WalArchiveHeader::new(1, 42, 2, 1700000000, 1700000100, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries.clone());
    let archive = WalArchive { header, body };

    let archive_path = archive.write_to_file(archive_dir).unwrap();
    let recovered = WalArchive::read_from_file(&archive_path).unwrap();

    assert_eq!(recovered.body.entries.len(), 2);

    let entry1 = &recovered.body.entries[0];
    assert_eq!(entry1.event_type, "user_action");
    assert_eq!(entry1.payload_as_json()["action"], json!("purchase"));
    assert_eq!(entry1.payload_as_json()["total"], json!(79.98));

    let entry2 = &recovered.body.entries[1];
    assert_eq!(entry2.event_type, "system_event");
    assert_eq!(entry2.payload_as_json()["level"], json!("error"));
}

#[test]
fn test_roundtrip_preserves_all_entry_fields() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path();

    let original_entry = WalEntryFactory::new()
        .with("event_type", "specific_event")
        .with("context_id", "ctx-789")
        .with("timestamp", 1234567890)
        .with(
            "payload",
            json!({"field1": "value1", "field2": 42, "field3": true}),
        )
        .create();

    let header = WalArchiveHeader::new(5, 99, 1, 1234567890, 1234567890, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(vec![original_entry.clone()]);
    let archive = WalArchive { header, body };

    let archive_path = archive.write_to_file(archive_dir).unwrap();
    let recovered = WalArchive::read_from_file(&archive_path).unwrap();

    let recovered_entry = &recovered.body.entries[0];
    assert_eq!(recovered_entry.event_type, original_entry.event_type);
    assert_eq!(recovered_entry.context_id, original_entry.context_id);
    assert_eq!(recovered_entry.timestamp, original_entry.timestamp);
    assert_eq!(recovered_entry.payload, original_entry.payload);
}

#[test]
fn test_archive_with_many_entries() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path();

    let entries = WalEntryFactory::new().create_list(1000);
    let header = WalArchiveHeader::new(1, 42, 1000, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries);
    let archive = WalArchive { header, body };

    let archive_path = archive.write_to_file(archive_dir).unwrap();
    let recovered = WalArchive::read_from_file(&archive_path).unwrap();

    assert_eq!(recovered.header.entry_count, 1000);
    assert_eq!(recovered.body.entries.len(), 1000);
}

#[test]
fn test_archive_version_constant() {
    assert_eq!(ARCHIVE_VERSION, 1, "Archive version should be 1");

    let header = WalArchiveHeader::new(1, 42, 10, 1000, 2000, "zstd".to_string(), 3);
    assert_eq!(
        header.version, ARCHIVE_VERSION,
        "Header should use ARCHIVE_VERSION constant"
    );
}

#[test]
fn test_header_records_compression_metadata() {
    let header1 = WalArchiveHeader::new(1, 42, 10, 1000, 2000, "zstd".to_string(), 3);
    assert_eq!(header1.compression, "zstd");
    assert_eq!(header1.compression_level, 3);

    let header2 = WalArchiveHeader::new(1, 42, 10, 1000, 2000, "lz4".to_string(), 9);
    assert_eq!(header2.compression, "lz4");
    assert_eq!(header2.compression_level, 9);
}

#[test]
fn test_file_size_after_compression() {
    let temp_dir = TempDir::new().unwrap();
    let archive_dir = temp_dir.path();

    let entries = WalEntryFactory::new()
        .with("payload", json!({"data": "test".repeat(100)}))
        .create_list(50);

    let header = WalArchiveHeader::new(1, 42, 50, 1000, 2000, "zstd".to_string(), 3);
    let body = WalArchiveBody::new(entries);
    let archive = WalArchive { header, body };

    let archive_path = archive.write_to_file(archive_dir).unwrap();
    let file_size = fs::metadata(&archive_path).unwrap().len();

    assert!(file_size > 0, "File should have non-zero size");
    assert!(
        file_size < 100000,
        "Compressed file should be reasonably small"
    );
}
