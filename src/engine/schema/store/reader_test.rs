use crate::engine::schema::store::reader::{
    read_records, read_single_record, read_u32, record_skipped_record,
};
use crate::engine::schema::store::types::{
    MAX_RECORD_LEN_BYTES, RecordReadResult, SchemaStoreDiagnostics,
};
use crate::engine::schema::store::writer::{compute_crc32, write_record};
use crate::shared::storage_header::BinaryHeader;
use crate::test_helpers::factories::SchemaRecordFactory;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn read_u32_reads_valid_u32() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let value: u32 = 12345;
    file.write_all(&value.to_le_bytes()).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0;
    let result = read_u32(&mut file, &mut offset).unwrap();
    assert_eq!(result, Some(12345));
    assert_eq!(offset, 4);
}

#[test]
fn read_u32_returns_none_on_eof() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0;
    let result = read_u32(&mut file, &mut offset).unwrap();
    assert_eq!(result, None);
    assert_eq!(offset, 0);
}

#[test]
fn read_u32_handles_partial_read() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    file.write_all(&[0x01, 0x02]).unwrap(); // Only 2 bytes
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0;
    let result = read_u32(&mut file, &mut offset).unwrap();
    assert_eq!(result, None); // EOF
    assert_eq!(offset, 0);
}

#[test]
fn record_skipped_record_updates_diagnostics() {
    let mut diag = SchemaStoreDiagnostics::default();
    let mut diagnostics = Some(&mut diag);
    record_skipped_record(&mut diagnostics, 100, "test reason");
    assert_eq!(diag.skipped_records, 1);
    assert_eq!(diag.issues.len(), 1);
    assert!(diag.issues[0].contains("test reason"));
}

#[test]
fn record_skipped_record_works_without_diagnostics() {
    let mut diagnostics = None;
    // Should not panic
    record_skipped_record(&mut diagnostics, 100, "test reason");
}

#[test]
fn read_single_record_reads_valid_record() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record = SchemaRecordFactory::new("test_event").create();
    write_record(&mut file, &record).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0u64;
    let mut diagnostics = None;
    let result = read_single_record(&mut file, &mut offset, &mut diagnostics).unwrap();

    match result {
        RecordReadResult::Valid(decoded) => {
            assert_eq!(decoded.event_type, "test_event");
        }
        _ => panic!("Expected Valid record"),
    }
}

#[test]
fn read_single_record_returns_eof_on_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0u64;
    let mut diagnostics = None;
    let result = read_single_record(&mut file, &mut offset, &mut diagnostics).unwrap();

    match result {
        RecordReadResult::Eof => {}
        _ => panic!("Expected EOF"),
    }
}

#[test]
fn read_single_record_rejects_too_large_length() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    // Write invalid length that's too large
    let invalid_len = MAX_RECORD_LEN_BYTES + 1;
    file.write_all(&invalid_len.to_le_bytes()).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0u64;
    let mut diag = SchemaStoreDiagnostics::default();
    let mut diagnostics = Some(&mut diag);
    let result = read_single_record(&mut file, &mut offset, &mut diagnostics).unwrap();

    match result {
        RecordReadResult::Eof => {}
        _ => panic!("Expected EOF for invalid length"),
    }
    assert_eq!(diagnostics.unwrap().skipped_records, 1);
}

#[test]
fn read_single_record_detects_crc_mismatch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record = SchemaRecordFactory::new("test_event").create();
    write_record(&mut file, &record).unwrap();
    drop(file);

    // Corrupt the CRC
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(4)).unwrap(); // Skip length, go to CRC
    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).unwrap();
    let mut corrupted_crc = u32::from_le_bytes(crc_buf);
    corrupted_crc ^= 0xFFFF; // Flip bits
    file.seek(SeekFrom::Start(4)).unwrap();
    file.write_all(&corrupted_crc.to_le_bytes()).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0u64;
    let mut diag = SchemaStoreDiagnostics::default();
    let mut diagnostics = Some(&mut diag);
    let result = read_single_record(&mut file, &mut offset, &mut diagnostics).unwrap();

    match result {
        RecordReadResult::Corrupted => {}
        _ => panic!("Expected Corrupted for CRC mismatch"),
    }
    assert_eq!(diag.skipped_records, 1);
}

#[test]
fn read_single_record_handles_truncated_record() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record = SchemaRecordFactory::new("test_event").create();
    let encoded = bincode::serialize(&record).unwrap();
    file.write_all(&(encoded.len() as u32).to_le_bytes())
        .unwrap();
    file.write_all(&compute_crc32(&encoded).to_le_bytes())
        .unwrap();
    file.write_all(&encoded[..encoded.len() / 2]).unwrap(); // Only write half
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut offset = 0u64;
    let mut diag = SchemaStoreDiagnostics::default();
    let mut diagnostics = Some(&mut diag);
    let result = read_single_record(&mut file, &mut offset, &mut diagnostics).unwrap();

    match result {
        RecordReadResult::Eof => {}
        _ => panic!("Expected EOF for truncated record"),
    }
}

#[test]
fn read_records_reads_multiple_valid_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    // Write header first
    let header = BinaryHeader::new(
        crate::shared::storage_header::FileKind::SchemaStore.magic(),
        1,
        0,
    );
    header.write_to(&mut file).unwrap();

    let record1 = SchemaRecordFactory::new("event1").create();
    let record2 = SchemaRecordFactory::new("event2").create();
    write_record(&mut file, &record1).unwrap();
    write_record(&mut file, &record2).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(BinaryHeader::TOTAL_LEN as u64))
        .unwrap();
    let mut records = Vec::new();
    read_records(&mut file, &mut records, None).unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].event_type, "event1");
    assert_eq!(records[1].event_type, "event2");
}

#[test]
fn read_records_skips_corrupted_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    // Write header
    let header = BinaryHeader::new(
        crate::shared::storage_header::FileKind::SchemaStore.magic(),
        1,
        0,
    );
    header.write_to(&mut file).unwrap();

    let good_record = SchemaRecordFactory::new("good").create();
    write_record(&mut file, &good_record).unwrap();

    // Write corrupted record
    let bad_record = SchemaRecordFactory::new("bad").create();
    let encoded = bincode::serialize(&bad_record).unwrap();
    file.write_all(&(encoded.len() as u32).to_le_bytes())
        .unwrap();
    file.write_all(&0xDEADBEEFu32.to_le_bytes()).unwrap(); // Wrong CRC
    file.write_all(&encoded).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(BinaryHeader::TOTAL_LEN as u64))
        .unwrap();
    let mut records = Vec::new();
    let mut diag = SchemaStoreDiagnostics::default();
    read_records(&mut file, &mut records, Some(&mut diag)).unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event_type, "good");
    assert_eq!(diag.skipped_records, 1);
}

#[test]
fn read_records_handles_empty_file_after_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    // Write only header
    let header = BinaryHeader::new(
        crate::shared::storage_header::FileKind::SchemaStore.magic(),
        1,
        0,
    );
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(BinaryHeader::TOTAL_LEN as u64))
        .unwrap();
    let mut records = Vec::new();
    read_records(&mut file, &mut records, None).unwrap();

    assert_eq!(records.len(), 0);
}
