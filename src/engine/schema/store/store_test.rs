use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::store::SchemaStore;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::test_helpers::factories::SchemaRecordFactory;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn append_and_load_roundtrip_with_crc() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let file_path = temp_dir.path().join("schemas_test.bin");

    let store = SchemaStore::new(file_path.clone()).expect("Failed to create SchemaStore");
    let record = SchemaRecordFactory::new("test_event")
        .with_uid("uid-test")
        .with_field("fieldA", "string")
        .without_field("field1")
        .create();

    store.append(&record).expect("Failed to append");

    let loaded = store.load().expect("Failed to load schema records");
    assert_eq!(loaded.len(), 1);
    let loaded_record = &loaded[0];
    assert_eq!(loaded_record.event_type, "test_event");
    assert_eq!(loaded_record.uid, "uid-test");
    assert!(loaded_record.schema.fields.contains_key("fieldA"));
    assert!(!loaded_record.schema.fields.contains_key("field1"));
}

#[test]
fn load_skips_truncated_tail_but_keeps_valid_prefix() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    let first = SchemaRecordFactory::new("valid").create();
    let second = SchemaRecordFactory::new("truncated").create();
    store.append(&first).unwrap();
    store.append(&second).unwrap();

    let metadata = std::fs::metadata(&path).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(metadata.len() - 8)
        .unwrap();

    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].event_type, "valid");
}

#[test]
fn load_skips_corrupted_record_on_crc_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    store
        .append(&SchemaRecordFactory::new("needs_crc").create())
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let len = bytes.len();
    bytes[len - 1] ^= 0xFF; // flip final byte to invalidate CRC
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();

    let loaded = store.load().unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn invalid_header_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"invalid-header").unwrap();

    let store = SchemaStore::new(path.clone()).unwrap();
    let err = store.load().unwrap_err();
    assert!(matches!(err, SchemaError::IoReadFailed(_)));
}

#[test]
fn rejects_record_with_unreasonably_large_length() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(FileKind::SchemaStore.magic(), 1, 0);
    header.write_to(&mut file).unwrap();
    file.write_all(&((10 * 1024 + 1) as u32).to_le_bytes())
        .unwrap();

    let store = SchemaStore::new(path).unwrap();
    let loaded = store.load().unwrap();
    assert!(loaded.is_empty());
}

#[test]
fn append_fails_when_exclusive_lock_is_held() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    lock_file.lock_exclusive().unwrap();

    let result = store.append(&SchemaRecordFactory::new("locked").create());
    assert!(matches!(result, Err(SchemaError::IoWriteFailed(_))));
}

#[test]
fn diagnose_reports_corruption() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    store
        .append(&SchemaRecordFactory::new("needs_crc").create())
        .unwrap();

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();
    let len = bytes.len();
    bytes[len - 1] ^= 0xAA;
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();

    let diagnostics = store.diagnose().expect("diagnose should succeed");
    assert_eq!(diagnostics.valid_records, 0);
    assert_eq!(diagnostics.skipped_records, 1);
    assert!(
        diagnostics
            .issues
            .iter()
            .any(|msg| msg.contains("CRC mismatch")),
        "expected CRC issue, got {:?}",
        diagnostics.issues
    );
}

#[test]
fn repair_filters_out_corrupted_records() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("schemas.bin");
    let repaired = dir.path().join("schemas_repaired.bin");
    let store = SchemaStore::new(source.clone()).unwrap();

    let good = SchemaRecordFactory::new("good").create();
    let bad = SchemaRecordFactory::new("bad").create();
    store.append(&good).unwrap();
    store.append(&bad).unwrap();

    let metadata = std::fs::metadata(&source).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(metadata.len() - 6)
        .unwrap();

    let diagnostics = store
        .repair_to(repaired.clone())
        .expect("repair should succeed");
    assert_eq!(diagnostics.valid_records, 1);
    assert_eq!(diagnostics.skipped_records, 1);

    let repaired_store = SchemaStore::new(repaired).unwrap();
    let repaired_records = repaired_store.load().unwrap();
    assert_eq!(repaired_records.len(), 1);
    assert_eq!(repaired_records[0].event_type, "good");
}

// ============================================================================
// End-to-End Tests
// ============================================================================

/// E2E test: Full lifecycle of schema store operations
#[test]
fn e2e_full_lifecycle() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    // Create store
    let store = SchemaStore::new(path.clone()).unwrap();

    // Append multiple records
    let records = vec![
        SchemaRecordFactory::new("user_created")
            .with_uid("uid1")
            .with_field("name", "string")
            .with_field("email", "string")
            .create(),
        SchemaRecordFactory::new("order_placed")
            .with_uid("uid2")
            .with_field("order_id", "int")
            .with_field("amount", "f64")
            .create(),
        SchemaRecordFactory::new("payment_processed")
            .with_uid("uid3")
            .with_field("payment_id", "string")
            .with_field("status", "string")
            .create(),
    ];

    for record in &records {
        store.append(record).unwrap();
    }

    // Load and verify all records
    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[0].event_type, "user_created");
    assert_eq!(loaded[1].event_type, "order_placed");
    assert_eq!(loaded[2].event_type, "payment_processed");

    // Diagnose should show all valid
    let diagnostics = store.diagnose().unwrap();
    assert_eq!(diagnostics.valid_records, 3);
    assert_eq!(diagnostics.skipped_records, 0);
    assert!(diagnostics.issues.is_empty());
}

/// E2E test: Multiple append/load cycles (simulating real usage)
#[test]
fn e2e_multiple_append_load_cycles() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    // First batch
    let batch1 = vec![
        SchemaRecordFactory::new("event1").with_uid("uid1").create(),
        SchemaRecordFactory::new("event2").with_uid("uid2").create(),
    ];
    for record in &batch1 {
        store.append(record).unwrap();
    }
    let loaded1 = store.load().unwrap();
    assert_eq!(loaded1.len(), 2);

    // Second batch
    let batch2 = vec![
        SchemaRecordFactory::new("event3").with_uid("uid3").create(),
        SchemaRecordFactory::new("event4").with_uid("uid4").create(),
    ];
    for record in &batch2 {
        store.append(record).unwrap();
    }
    let loaded2 = store.load().unwrap();
    assert_eq!(loaded2.len(), 4);

    // Third batch
    let batch3 = vec![SchemaRecordFactory::new("event5").with_uid("uid5").create()];
    for record in &batch3 {
        store.append(record).unwrap();
    }
    let loaded3 = store.load().unwrap();
    assert_eq!(loaded3.len(), 5);

    // Verify all records are present and in order
    assert_eq!(loaded3[0].event_type, "event1");
    assert_eq!(loaded3[1].event_type, "event2");
    assert_eq!(loaded3[2].event_type, "event3");
    assert_eq!(loaded3[3].event_type, "event4");
    assert_eq!(loaded3[4].event_type, "event5");
}

/// E2E test: Fsync option ensures durability
#[test]
fn e2e_fsync_option_ensures_durability() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    // Create store with fsync enabled
    use crate::engine::schema::store::SchemaStoreOptions;
    let options = SchemaStoreOptions { fsync: true };
    let store = SchemaStore::new_with_options(path.clone(), options).unwrap();

    let record = SchemaRecordFactory::new("critical_event")
        .with_uid("critical_uid")
        .create();

    // Append with fsync
    store.append(&record).unwrap();

    // Immediately load - should be available
    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].event_type, "critical_event");
}

/// E2E test: Diagnose and repair workflow with mixed corruption
#[test]
fn e2e_diagnose_and_repair_workflow() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("schemas.bin");
    let repaired = dir.path().join("schemas_repaired.bin");

    let store = SchemaStore::new(source.clone()).unwrap();

    // Append multiple records
    let good1 = SchemaRecordFactory::new("good1").create();
    let good2 = SchemaRecordFactory::new("good2").create();
    let good3 = SchemaRecordFactory::new("good3").create();

    store.append(&good1).unwrap();
    store.append(&good2).unwrap();
    store.append(&good3).unwrap();

    // Corrupt the middle record by flipping CRC
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();

    // Find and corrupt the CRC of the second record
    // Skip header (20 bytes) + first record (len + crc + data)
    // We'll corrupt the CRC of the second record
    let header_len = 20;
    let mut offset = header_len as usize;

    // Skip first record
    let len1 = u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]) as usize;
    offset += 4; // skip len
    offset += 4; // skip crc
    offset += len1; // skip data

    // Corrupt second record's CRC
    bytes[offset + 1] ^= 0xFF;
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();
    drop(file);

    // Diagnose should detect corruption
    let diagnostics = store.diagnose().unwrap();
    // After corruption, we may have fewer valid records
    assert!(diagnostics.valid_records <= 2); // At most first two records
    assert!(diagnostics.skipped_records >= 1);
    // Should have some issues reported (CRC mismatch or other corruption)
    assert!(!diagnostics.issues.is_empty());

    // Repair should recover valid records
    let repair_diagnostics = store.repair_to(repaired.clone()).unwrap();
    assert_eq!(repair_diagnostics.valid_records, 1); // First record is valid
    assert!(repair_diagnostics.skipped_records >= 1);

    // Verify repaired store
    let repaired_store = SchemaStore::new(repaired).unwrap();
    let repaired_records = repaired_store.load().unwrap();
    assert_eq!(repaired_records.len(), 1);
    assert_eq!(repaired_records[0].event_type, "good1");
}

/// E2E test: Large number of records
#[test]
fn e2e_large_number_of_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    // Append 100 records
    let num_records = 100;
    for i in 0..num_records {
        let record = SchemaRecordFactory::new(&format!("event_{}", i))
            .with_uid(&format!("uid_{}", i))
            .create();
        store.append(&record).unwrap();
    }

    // Load all records
    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), num_records);

    // Verify all are present
    for (i, record) in loaded.iter().enumerate() {
        assert_eq!(record.event_type, format!("event_{}", i));
        assert_eq!(record.uid, format!("uid_{}", i));
    }

    // Diagnose should show all valid
    let diagnostics = store.diagnose().unwrap();
    assert_eq!(diagnostics.valid_records, num_records);
    assert_eq!(diagnostics.skipped_records, 0);
}

/// E2E test: Error recovery - corrupted file with partial recovery
#[test]
fn e2e_error_recovery_partial_corruption() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("schemas.bin");
    let repaired = dir.path().join("schemas_repaired.bin");

    let store = SchemaStore::new(source.clone()).unwrap();

    // Append 5 records
    for i in 1..=5 {
        let record = SchemaRecordFactory::new(&format!("record_{}", i)).create();
        store.append(&record).unwrap();
    }

    // Truncate file to simulate partial write failure
    let metadata = std::fs::metadata(&source).unwrap();
    let truncated_len = metadata.len() - 50; // Remove last 50 bytes
    std::fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .unwrap()
        .set_len(truncated_len)
        .unwrap();

    // Load should recover what it can
    let loaded = store.load().unwrap();
    assert!(loaded.len() < 5); // Some records should be lost
    assert!(loaded.len() > 0); // But some should be recoverable

    // Diagnose should report the issue
    let diagnostics = store.diagnose().unwrap();
    assert!(diagnostics.valid_records > 0);
    assert!(diagnostics.skipped_records > 0);
    assert!(
        diagnostics
            .issues
            .iter()
            .any(|i| i.contains("failed to read"))
    );

    // Repair should create clean file with valid records
    let repair_diagnostics = store.repair_to(repaired.clone()).unwrap();
    assert_eq!(repair_diagnostics.valid_records, loaded.len());

    let repaired_store = SchemaStore::new(repaired).unwrap();
    let repaired_records = repaired_store.load().unwrap();
    assert_eq!(repaired_records.len(), loaded.len());
}

/// E2E test: Empty store operations
#[test]
fn e2e_empty_store_operations() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    // Load empty store
    let loaded = store.load().unwrap();
    assert!(loaded.is_empty());

    // Diagnose empty store
    let diagnostics = store.diagnose().unwrap();
    assert_eq!(diagnostics.valid_records, 0);
    assert_eq!(diagnostics.skipped_records, 0);
    assert!(diagnostics.issues.is_empty());

    // Repair empty store
    let repaired = dir.path().join("repaired.bin");
    let repair_diagnostics = store.repair_to(repaired.clone()).unwrap();
    assert_eq!(repair_diagnostics.valid_records, 0);

    let repaired_store = SchemaStore::new(repaired).unwrap();
    let repaired_records = repaired_store.load().unwrap();
    assert!(repaired_records.is_empty());
}

/// E2E test: Store with various field types
#[test]
fn e2e_various_field_types() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    let records = vec![
        SchemaRecordFactory::new("string_fields")
            .with_field("name", "string")
            .with_field("description", "string")
            .create(),
        SchemaRecordFactory::new("numeric_fields")
            .with_field("count", "int")
            .with_field("price", "f64")
            .with_field("quantity", "u64")
            .create(),
        SchemaRecordFactory::new("mixed_fields")
            .with_field("id", "int")
            .with_field("name", "string")
            .with_field("active", "bool")
            .with_field("score", "f64")
            .create(),
    ];

    for record in &records {
        store.append(record).unwrap();
    }

    let loaded = store.load().unwrap();
    assert_eq!(loaded.len(), 3);

    // Verify field types are preserved
    assert!(loaded[0].schema.fields.contains_key("name"));
    assert!(loaded[1].schema.fields.contains_key("count"));
    assert!(loaded[2].schema.fields.contains_key("id"));
    assert!(loaded[2].schema.fields.contains_key("name"));
    assert!(loaded[2].schema.fields.contains_key("active"));
    assert!(loaded[2].schema.fields.contains_key("score"));
}

/// E2E test: Repair with multiple corruption points
#[test]
fn e2e_repair_multiple_corruption_points() {
    let dir = tempdir().unwrap();
    let source = dir.path().join("schemas.bin");
    let repaired = dir.path().join("schemas_repaired.bin");

    let store = SchemaStore::new(source.clone()).unwrap();

    // Append 10 records
    for i in 1..=10 {
        let record = SchemaRecordFactory::new(&format!("record_{}", i)).create();
        store.append(&record).unwrap();
    }

    // Corrupt multiple records by flipping random bytes
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();

    // Corrupt records at positions 2, 5, and 8 by flipping CRC bytes
    let header_len = 20;
    let mut offset = header_len as usize;

    for record_num in 1..=10 {
        let len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]) as usize;
        offset += 4; // skip len

        // Corrupt CRC for records 2, 5, 8
        if record_num == 2 || record_num == 5 || record_num == 8 {
            bytes[offset] ^= 0xFF;
            bytes[offset + 1] ^= 0xFF;
        }

        offset += 4; // skip crc
        offset += len; // skip data
    }

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();
    drop(file);

    // Diagnose should detect multiple corruptions
    let diagnostics = store.diagnose().unwrap();
    assert!(diagnostics.skipped_records >= 3);
    assert!(diagnostics.valid_records < 10);

    // Repair should recover all valid records
    let repair_diagnostics = store.repair_to(repaired.clone()).unwrap();
    assert!(repair_diagnostics.valid_records > 0);
    assert!(repair_diagnostics.valid_records < 10);

    // Verify repaired store only has valid records
    let repaired_store = SchemaStore::new(repaired).unwrap();
    let repaired_records = repaired_store.load().unwrap();
    assert_eq!(repaired_records.len(), repair_diagnostics.valid_records);

    // Verify all loaded records are valid (can be deserialized)
    for record in &repaired_records {
        assert!(!record.event_type.is_empty());
        assert!(!record.uid.is_empty());
    }
}

/// E2E test: Store persistence across instances
#[test]
fn e2e_persistence_across_instances() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    // First instance - create and append
    {
        let store = SchemaStore::new(path.clone()).unwrap();
        let record1 = SchemaRecordFactory::new("persistent1").create();
        let record2 = SchemaRecordFactory::new("persistent2").create();
        store.append(&record1).unwrap();
        store.append(&record2).unwrap();
    }

    // Second instance - should see previous records
    {
        let store = SchemaStore::new(path.clone()).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].event_type, "persistent1");
        assert_eq!(loaded[1].event_type, "persistent2");
    }

    // Third instance - append more
    {
        let store = SchemaStore::new(path.clone()).unwrap();
        let record3 = SchemaRecordFactory::new("persistent3").create();
        store.append(&record3).unwrap();
    }

    // Fourth instance - should see all three
    {
        let store = SchemaStore::new(path.clone()).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].event_type, "persistent1");
        assert_eq!(loaded[1].event_type, "persistent2");
        assert_eq!(loaded[2].event_type, "persistent3");
    }
}

/// E2E test: Diagnose with various error conditions
#[test]
fn e2e_diagnose_various_error_conditions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");

    // Test 1: Invalid header
    {
        let mut file = File::create(&path).unwrap();
        file.write_all(b"INVALID_HEADER_DATA").unwrap();
        drop(file);

        let store = SchemaStore::new(path.clone()).unwrap();
        let diagnostics = store.diagnose().unwrap();
        assert!(!diagnostics.issues.is_empty());
        assert!(
            diagnostics
                .issues
                .iter()
                .any(|i| i.contains("failed to read"))
        );
    }

    // Test 2: Wrong magic
    {
        use crate::shared::storage_header::BinaryHeader;
        let mut file = File::create(&path).unwrap();
        let header = BinaryHeader::new(*b"WRONG\0\0\0", 1, 0);
        header.write_to(&mut file).unwrap();
        drop(file);

        let store = SchemaStore::new(path.clone()).unwrap();
        let diagnostics = store.diagnose().unwrap();
        assert!(!diagnostics.issues.is_empty());
        assert!(
            diagnostics
                .issues
                .iter()
                .any(|i| i.contains("invalid magic"))
        );
    }

    // Test 3: Wrong version
    {
        use crate::shared::storage_header::{BinaryHeader, FileKind};
        let mut file = File::create(&path).unwrap();
        let header = BinaryHeader::new(FileKind::SchemaStore.magic(), 999, 0);
        header.write_to(&mut file).unwrap();
        drop(file);

        let store = SchemaStore::new(path.clone()).unwrap();
        let diagnostics = store.diagnose().unwrap();
        assert!(!diagnostics.issues.is_empty());
        assert!(diagnostics.issues.iter().any(|i| i.contains("unsupported")));
    }
}

/// E2E test: Repair prevents overwriting source
#[test]
fn e2e_repair_prevents_overwriting_source() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schemas.bin");
    let store = SchemaStore::new(path.clone()).unwrap();

    let record = SchemaRecordFactory::new("test").create();
    store.append(&record).unwrap();

    // Attempt to repair to same path should fail
    let result = store.repair_to(path.clone());
    assert!(matches!(result, Err(_)));
    if let Err(e) = result {
        assert!(format!("{}", e).contains("must differ"));
    }
}
