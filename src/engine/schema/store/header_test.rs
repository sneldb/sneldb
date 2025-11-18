use crate::engine::schema::errors::SchemaError;
use crate::engine::schema::store::header::{
    ensure_header, is_file_empty, read_and_validate_header,
};
use crate::engine::schema::store::types::{SCHEMA_STORE_VERSION, SchemaStoreDiagnostics};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

#[test]
fn is_file_empty_returns_true_for_new_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let file = File::create(&path).unwrap();
    assert!(is_file_empty(&file).unwrap());
}

#[test]
fn is_file_empty_returns_false_for_file_with_content() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"test").unwrap();
    file.flush().unwrap();
    drop(file);

    let file = File::open(&path).unwrap();
    assert!(!is_file_empty(&file).unwrap());
}

#[test]
fn ensure_header_writes_header_to_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    ensure_header(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let header = BinaryHeader::read_from(&mut file).unwrap();
    assert_eq!(header.magic, FileKind::SchemaStore.magic());
    assert_eq!(header.version, SCHEMA_STORE_VERSION);
}

#[test]
fn ensure_header_does_not_overwrite_existing_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let original_header = BinaryHeader::new(FileKind::SchemaStore.magic(), SCHEMA_STORE_VERSION, 0);
    original_header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    ensure_header(&mut file).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();

    let header = BinaryHeader::read_from(&mut file).unwrap();
    assert_eq!(header.magic, FileKind::SchemaStore.magic());
    assert_eq!(header.version, SCHEMA_STORE_VERSION);
}

#[test]
fn read_and_validate_header_succeeds_with_valid_header() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(FileKind::SchemaStore.magic(), SCHEMA_STORE_VERSION, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let result = read_and_validate_header(&mut file, None).unwrap();
    assert!(result.is_some());
    let header = result.unwrap();
    assert_eq!(header.magic, FileKind::SchemaStore.magic());
    assert_eq!(header.version, SCHEMA_STORE_VERSION);
}

#[test]
fn read_and_validate_header_fails_with_invalid_magic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(*b"INVALID\0", SCHEMA_STORE_VERSION, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let result = read_and_validate_header(&mut file, None);
    assert!(matches!(result, Err(SchemaError::IoReadFailed(_))));
}

#[test]
fn read_and_validate_header_fails_with_wrong_version() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(FileKind::SchemaStore.magic(), 999, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let result = read_and_validate_header(&mut file, None);
    assert!(matches!(result, Err(SchemaError::IoReadFailed(_))));
}

#[test]
fn read_and_validate_header_records_errors_in_diagnostics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    file.write_all(b"invalid").unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut diagnostics = SchemaStoreDiagnostics::default();
    let result = read_and_validate_header(&mut file, Some(&mut diagnostics)).unwrap();
    assert!(result.is_none());
    assert!(!diagnostics.issues.is_empty());
    assert!(
        diagnostics
            .issues
            .iter()
            .any(|i| i.contains("failed to read schema store header"))
    );
}

#[test]
fn read_and_validate_header_records_invalid_magic_in_diagnostics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(*b"INVALID\0", SCHEMA_STORE_VERSION, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut diagnostics = SchemaStoreDiagnostics::default();
    let result = read_and_validate_header(&mut file, Some(&mut diagnostics)).unwrap();
    assert!(result.is_none());
    assert!(!diagnostics.issues.is_empty());
    assert!(
        diagnostics
            .issues
            .iter()
            .any(|i| i.contains("invalid magic"))
    );
}

#[test]
fn read_and_validate_header_records_wrong_version_in_diagnostics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(FileKind::SchemaStore.magic(), 999, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut diagnostics = SchemaStoreDiagnostics::default();
    let result = read_and_validate_header(&mut file, Some(&mut diagnostics)).unwrap();
    assert!(result.is_none());
    assert!(!diagnostics.issues.is_empty());
    assert!(
        diagnostics
            .issues
            .iter()
            .any(|i| i.contains("unsupported schema store version"))
    );
}

#[test]
fn read_and_validate_header_sets_version_in_diagnostics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.bin");
    let mut file = File::create(&path).unwrap();
    let header = BinaryHeader::new(FileKind::SchemaStore.magic(), SCHEMA_STORE_VERSION, 0);
    header.write_to(&mut file).unwrap();
    drop(file);

    let mut file = File::open(&path).unwrap();
    let mut diagnostics = SchemaStoreDiagnostics::default();
    let result = read_and_validate_header(&mut file, Some(&mut diagnostics)).unwrap();
    assert!(result.is_some());
    assert_eq!(diagnostics.version, Some(SCHEMA_STORE_VERSION));
}
