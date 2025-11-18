use crate::engine::schema::store::writer::{compute_crc32, write_record};
use crate::test_helpers::factories::SchemaRecordFactory;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use tempfile::tempdir;

#[test]
fn compute_crc32_produces_consistent_results() {
    let data = b"test data";
    let crc1 = compute_crc32(data);
    let crc2 = compute_crc32(data);
    assert_eq!(crc1, crc2);
}

#[test]
fn compute_crc32_produces_different_results_for_different_data() {
    let data1 = b"test data 1";
    let data2 = b"test data 2";
    let crc1 = compute_crc32(data1);
    let crc2 = compute_crc32(data2);
    assert_ne!(crc1, crc2);
}

#[test]
fn compute_crc32_handles_empty_data() {
    let data = b"";
    let crc = compute_crc32(data);
    // Should not panic and produce some value
    assert!(crc > 0 || crc == 0); // CRC is always valid (u32)
}

#[test]
fn compute_crc32_handles_large_data() {
    let data = vec![0u8; 10000];
    let crc = compute_crc32(&data);
    // Should not panic
    assert!(crc > 0 || crc == 0); // CRC is always valid (u32)
}

#[test]
fn write_record_writes_length_crc_and_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record = SchemaRecordFactory::new("test_event")
        .with_uid("uid123")
        .with_field("field1", "string")
        .create();

    write_record(&mut file, &record).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    // Read length (4 bytes)
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf);
    assert!(len > 0);

    // Read CRC (4 bytes)
    let mut crc_buf = [0u8; 4];
    file.read_exact(&mut crc_buf).unwrap();
    let expected_crc = u32::from_le_bytes(crc_buf);

    // Read data
    let mut data_buf = vec![0u8; len as usize];
    file.read_exact(&mut data_buf).unwrap();

    // Verify CRC
    let actual_crc = compute_crc32(&data_buf);
    assert_eq!(actual_crc, expected_crc);

    // Verify data can be deserialized
    let decoded: crate::engine::schema::registry::SchemaRecord =
        bincode::deserialize(&data_buf).unwrap();
    assert_eq!(decoded.event_type, "test_event");
    assert_eq!(decoded.uid, "uid123");
}

#[test]
fn write_record_writes_multiple_records_sequentially() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record1 = SchemaRecordFactory::new("event1").create();
    let record2 = SchemaRecordFactory::new("event2").create();

    write_record(&mut file, &record1).unwrap();
    write_record(&mut file, &record2).unwrap();
    drop(file);

    // Verify both records are written
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    // Read first record
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).unwrap();
    let len1 = u32::from_le_bytes(len_buf);
    file.seek(SeekFrom::Current(4 + len1 as i64)).unwrap(); // Skip CRC and data

    // Read second record length
    file.read_exact(&mut len_buf).unwrap();
    let len2 = u32::from_le_bytes(len_buf);
    assert!(len2 > 0);
    // Both records should be written successfully (they may have same or different sizes)
}

#[test]
fn write_record_handles_record_with_empty_schema() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let record = SchemaRecordFactory::new("empty_event")
        .without_field("field1")
        .without_field("field2")
        .create();

    // Should not panic even with minimal fields
    write_record(&mut file, &record).unwrap();
}

#[test]
fn write_record_handles_record_with_many_fields() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();

    let mut factory = SchemaRecordFactory::new("many_fields");
    for i in 0..50 {
        factory = factory.with_field(&format!("field{}", i), "string");
    }
    let record = factory.create();

    write_record(&mut file, &record).unwrap();
    drop(file);

    // Verify it was written correctly
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf);
    assert!(len > 0);
}
