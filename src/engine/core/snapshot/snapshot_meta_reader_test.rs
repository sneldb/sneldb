use crate::engine::core::snapshot::snapshot_meta_reader::SnapshotMetaReader;
use crate::engine::core::snapshot::snapshot_meta_writer::SnapshotMetaWriter;
use crate::test_helpers::factories::SnapshotMetaFactory;
use std::fs::OpenOptions;
use tempfile::tempdir;

#[test]
fn snapshot_meta_reader_handles_empty_and_partial_files() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metas_partial.smt");

    // Empty snapshot
    SnapshotMetaWriter::new(&path).write_all(&[]).unwrap();
    let loaded_empty = SnapshotMetaReader::new(&path).read_all().unwrap();
    assert_eq!(loaded_empty.len(), 0);

    // Write some metas
    let metas = SnapshotMetaFactory::new()
        .with_uid("batch")
        .with_context_id("ctx")
        .with_range(10, 20)
        .create_list(2);
    SnapshotMetaWriter::new(&path).write_all(&metas).unwrap();

    // Truncate mid-payload
    let metadata = std::fs::metadata(&path).unwrap();
    let new_len = metadata.len() - 2; // remove last 2 bytes
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(new_len).unwrap();

    // Reader should not panic and should return at least one record
    let loaded_partial = SnapshotMetaReader::new(&path).read_all().unwrap();
    assert!(loaded_partial.len() >= 1);
}
