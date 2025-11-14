use crate::engine::core::read::catalog::{IndexKind, SegmentIndexCatalog};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use std::fs::OpenOptions;
use std::io::Write;

#[test]
fn catalog_new_and_mutators_work() {
    let mut cat = SegmentIndexCatalog::new("uid1".to_string(), "seg1".to_string());
    assert_eq!(cat.uid, "uid1");
    assert_eq!(cat.segment_id, "seg1");
    assert!(cat.field_kinds.is_empty());
    assert!(cat.global_kinds.is_empty());

    let kinds_ts = IndexKind::TS_CALENDAR | IndexKind::TS_ZTI;
    cat.set_field_kind("timestamp", kinds_ts);
    assert_eq!(cat.field_kinds.get("timestamp").copied().unwrap(), kinds_ts);

    let g = IndexKind::ZONE_INDEX | IndexKind::RLTE;
    cat.add_global_kind(g);
    assert_eq!(cat.global_kinds, g);

    // Adding more global kinds should OR them
    let g2 = IndexKind::ZONE_SURF;
    cat.add_global_kind(g2);
    assert_eq!(cat.global_kinds, g | g2);
}

#[test]
fn catalog_save_and_load_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("u.icx");

    let mut write_cat = SegmentIndexCatalog::new("uid2".to_string(), "segA".to_string());
    write_cat.set_field_kind(
        "event_type",
        IndexKind::ZONE_XOR_INDEX | IndexKind::ENUM_BITMAP,
    );
    write_cat.set_field_kind("timestamp", IndexKind::TS_CALENDAR | IndexKind::TS_ZTI);
    write_cat.add_global_kind(IndexKind::ZONE_INDEX);

    write_cat.save(&path).expect("save icx");

    let read_cat = SegmentIndexCatalog::load(&path).expect("load icx");
    assert_eq!(read_cat.uid, "uid2");
    assert_eq!(read_cat.segment_id, "segA");
    assert_eq!(
        read_cat.field_kinds.get("event_type").copied().unwrap(),
        IndexKind::ZONE_XOR_INDEX | IndexKind::ENUM_BITMAP
    );
    assert_eq!(
        read_cat.field_kinds.get("timestamp").copied().unwrap(),
        IndexKind::TS_CALENDAR | IndexKind::TS_ZTI
    );
    assert_eq!(read_cat.global_kinds, IndexKind::ZONE_INDEX);
}

#[test]
fn catalog_save_overwrites_and_reloads_new_content() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("o.icx");

    // First write
    let mut c1 = SegmentIndexCatalog::new("u".to_string(), "seg1".to_string());
    c1.set_field_kind("a", IndexKind::ENUM_BITMAP);
    c1.save(&path).unwrap();

    // Second write with different content
    let mut c2 = SegmentIndexCatalog::new("u".to_string(), "seg2".to_string());
    c2.set_field_kind("b", IndexKind::ZONE_XOR_INDEX);
    c2.add_global_kind(IndexKind::RLTE);
    c2.save(&path).unwrap();

    let loaded = SegmentIndexCatalog::load(&path).unwrap();
    assert_eq!(loaded.segment_id, "seg2");
    assert!(loaded.field_kinds.get("a").is_none());
    assert_eq!(
        loaded.field_kinds.get("b").copied().unwrap(),
        IndexKind::ZONE_XOR_INDEX
    );
    assert_eq!(loaded.global_kinds, IndexKind::RLTE);
}

#[test]
fn catalog_load_fails_on_missing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("missing.icx");
    let err = SegmentIndexCatalog::load(&path)
        .err()
        .expect("expect io error");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn catalog_load_fails_on_bad_header_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bad.icx");

    // Manually write wrong header, then some bytes
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    // Use a different file kind to corrupt magic
    BinaryHeader::new(FileKind::ZoneIndex.magic(), 1, 0)
        .write_to(&mut f)
        .unwrap();
    f.write_all(&[1, 2, 3, 4]).unwrap();
    drop(f);

    let err = SegmentIndexCatalog::load(&path)
        .err()
        .expect("invalid magic");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn catalog_load_fails_on_corrupted_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("corrupt.icx");

    // Write valid header for IndexCatalog, but garbage payload (not bincode)
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    BinaryHeader::new(FileKind::IndexCatalog.magic(), 1, 0)
        .write_to(&mut f)
        .unwrap();
    // Write smaller/invalid bytes to force bincode error
    f.write_all(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE]).unwrap();
    drop(f);

    let err = SegmentIndexCatalog::load(&path)
        .err()
        .expect("bincode error");
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
}
