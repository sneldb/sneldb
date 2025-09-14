use crate::engine::core::snapshot::snapshot_reader::SnapshotReader;
use crate::engine::core::snapshot::snapshot_writer::SnapshotWriter;
use crate::shared::storage_header::{BinaryHeader, FileKind};
use crate::test_helpers::factories::EventFactory;
use std::fs::File;
use tempfile::tempdir;

#[test]
fn snapshot_writer_roundtrip_with_reader() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("events.snp");

    // Build events from multiple types
    let mut events = Vec::new();
    events.extend(
        EventFactory::new()
            .with("event_type", "signup")
            .create_list(2),
    );
    events.extend(
        EventFactory::new()
            .with("event_type", "purchase")
            .create_list(1),
    );

    // Write
    SnapshotWriter::new(&path).write_all(&events).unwrap();

    // Validate header
    let mut f = File::open(&path).unwrap();
    let hdr = BinaryHeader::read_from(&mut f).unwrap();
    assert_eq!(hdr.magic, FileKind::EventSnapshot.magic());

    // Read back
    let loaded = SnapshotReader::new(&path).read_all().unwrap();
    assert_eq!(loaded.len(), events.len());

    // Spot-check types to ensure heterogeneity preserved
    let types: Vec<_> = loaded.iter().map(|e| e.event_type.as_str()).collect();
    assert!(types.contains(&"signup"));
    assert!(types.contains(&"purchase"));
}
