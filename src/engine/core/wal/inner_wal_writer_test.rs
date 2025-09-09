use crate::engine::core::InnerWalWriter;
use crate::engine::core::WalEntry;
use crate::test_helpers::factories::WalEntryFactory;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_inner_wal_writer_end_to_end() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().to_path_buf();

    let mut writer = InnerWalWriter::new(wal_dir.clone()).unwrap();
    writer.start_next_log_file().unwrap();

    let entry = WalEntryFactory::new().create();
    writer.append_immediate(&entry).unwrap();
    writer.flush_and_close().unwrap();

    let files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();

    assert_eq!(files.len(), 1);
    assert!(
        files[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("wal-")
    );

    let contents = fs::read_to_string(&files[0]).unwrap();
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 1);

    let parsed: WalEntry = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed.timestamp, entry.timestamp);
    assert_eq!(parsed.context_id, entry.context_id);
    assert_eq!(parsed.event_type, entry.event_type);
    assert_eq!(parsed.payload, entry.payload);
}

#[test]
fn test_log_rotation_creates_new_file() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().to_path_buf();
    let mut writer = InnerWalWriter::new(wal_dir.clone()).unwrap();
    writer.start_next_log_file().unwrap();

    let entry = WalEntryFactory::new().create();

    // Manually force a rotate
    writer.append_immediate(&entry).unwrap();
    writer.rotate_log_file().unwrap();
    writer.append_immediate(&entry).unwrap();
    writer.flush_and_close().unwrap();

    let files: Vec<_> = fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();

    assert_eq!(files.len(), 2);
}
