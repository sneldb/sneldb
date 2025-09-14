use crate::engine::core::snapshot::snapshot_reader::SnapshotReader;
use crate::engine::core::snapshot::snapshot_writer::SnapshotWriter;
use crate::test_helpers::factories::EventFactory;
use tempfile::tempdir;

#[test]
fn snapshot_reader_handles_empty_and_partial_files() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.snp");

    // Empty file path won't exist yet; reading should yield empty vec (via open error -> Ok(vec![])?),
    // so we first write a valid but empty snapshot using writer for determinism
    SnapshotWriter::new(&path).write_all(&[]).unwrap();
    let loaded = SnapshotReader::new(&path).read_all().unwrap();
    assert_eq!(loaded.len(), 0);

    // Now write 2 events and then truncate mid-record to simulate partial
    let events = EventFactory::new().create_list(2);
    SnapshotWriter::new(&path).write_all(&events).unwrap();

    // Truncate file to cut into the last record payload
    let metadata = std::fs::metadata(&path).unwrap();
    let new_len = metadata.len() - 2; // remove last 2 bytes
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(new_len).unwrap();

    // Reader should warn and stop at truncation without panicking
    let loaded_partial = SnapshotReader::new(&path).read_all().unwrap();
    assert!(loaded_partial.len() >= 1);
}

#[test]
fn snapshot_reader_reads_valid_snapshot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ok.snp");

    // Build deterministic set preserving order
    let events = vec![
        EventFactory::new().with("event_type", "a").create(),
        EventFactory::new().with("event_type", "b").create(),
        EventFactory::new().with("event_type", "a").create(),
    ];

    SnapshotWriter::new(&path).write_all(&events).unwrap();
    let loaded = SnapshotReader::new(&path).read_all().unwrap();

    assert_eq!(loaded.len(), events.len());
    for (expected, got) in events.iter().zip(loaded.iter()) {
        assert_eq!(got.event_type, expected.event_type);
        assert!(!got.context_id.is_empty());
        assert!(got.timestamp > 0);
        assert!(got.payload.get("key").is_some());
    }
}
