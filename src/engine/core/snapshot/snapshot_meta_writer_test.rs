use crate::engine::core::snapshot::snapshot_meta_reader::SnapshotMetaReader;
use crate::engine::core::snapshot::snapshot_meta_writer::SnapshotMetaWriter;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::test_helpers::factories::SnapshotMetaFactory;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn snapshot_meta_writer_writes_header_and_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metas.smt");

    let meta1 = SnapshotMetaFactory::new()
        .with_uid("uid-a")
        .with_context_id("ctx-1")
        .with_range(100, 200)
        .create();
    let meta2 = SnapshotMetaFactory::new()
        .with_uid("uid-b")
        .with_context_id("ctx-2")
        .with_range(150, 250)
        .create();
    let metas = vec![meta1, meta2];

    SnapshotMetaWriter::new(&path).write_all(&metas).unwrap();

    // Validate header
    let mut f = File::open(&path).unwrap();
    let hdr = BinaryHeader::read_from(&mut f).unwrap();
    assert_eq!(hdr.magic, FileKind::EventSnapshotMeta.magic());

    // Read back using reader for end-to-end validation
    let loaded = SnapshotMetaReader::new(&path).read_all().unwrap();
    assert_eq!(loaded, metas);
}
