use crate::engine::schema::store::types::{
    MAX_RECORD_LEN_BYTES, RecordReadResult, SCHEMA_STORE_VERSION, SchemaStoreDiagnostics,
    SchemaStoreOptions,
};
use crate::test_helpers::factories::SchemaRecordFactory;

#[test]
fn schema_store_options_default() {
    let options = SchemaStoreOptions::default();
    assert_eq!(options.fsync, false);
}

#[test]
fn schema_store_options_custom() {
    let options = SchemaStoreOptions { fsync: true };
    assert_eq!(options.fsync, true);
}

#[test]
fn schema_store_diagnostics_default() {
    let diag = SchemaStoreDiagnostics::default();
    assert_eq!(diag.version, None);
    assert_eq!(diag.valid_records, 0);
    assert_eq!(diag.skipped_records, 0);
    assert_eq!(diag.issues.len(), 0);
}

#[test]
fn schema_store_diagnostics_can_be_updated() {
    let mut diag = SchemaStoreDiagnostics::default();
    diag.version = Some(1);
    diag.valid_records = 5;
    diag.skipped_records = 2;
    diag.issues.push("test issue".to_string());

    assert_eq!(diag.version, Some(1));
    assert_eq!(diag.valid_records, 5);
    assert_eq!(diag.skipped_records, 2);
    assert_eq!(diag.issues.len(), 1);
}

#[test]
fn schema_store_diagnostics_is_cloneable() {
    let mut diag = SchemaStoreDiagnostics::default();
    diag.version = Some(1);
    diag.issues.push("test".to_string());

    let cloned = diag.clone();
    assert_eq!(cloned.version, Some(1));
    assert_eq!(cloned.issues.len(), 1);
}

#[test]
fn record_read_result_valid_contains_record() {
    let record = SchemaRecordFactory::new("test").create();
    let result = RecordReadResult::Valid(record.clone());
    match result {
        RecordReadResult::Valid(r) => assert_eq!(r.event_type, "test"),
        _ => panic!("Expected Valid"),
    }
}

#[test]
fn record_read_result_corrupted() {
    let result = RecordReadResult::Corrupted;
    match result {
        RecordReadResult::Corrupted => {}
        _ => panic!("Expected Corrupted"),
    }
}

#[test]
fn record_read_result_eof() {
    let result = RecordReadResult::Eof;
    match result {
        RecordReadResult::Eof => {}
        _ => panic!("Expected Eof"),
    }
}

#[test]
fn constants_have_expected_values() {
    assert_eq!(SCHEMA_STORE_VERSION, 1);
    assert_eq!(MAX_RECORD_LEN_BYTES, 10 * 1024);
}
