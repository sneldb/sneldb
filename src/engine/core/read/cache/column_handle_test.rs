use crate::engine::core::read::cache::ColumnHandle;
use crate::shared::storage_header::FileKind;
use crate::test_helpers::factories::column_factory::ColumnFactory;
use std::fs::create_dir_all;

#[test]
fn open_succeeds_with_minimal_files() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let segment_id = "00000";
    let uid = "uid_test";
    let field = "field_a";
    let seg_dir = base.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Minimal, valid files via factory
    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .write_minimal();

    let handle = ColumnHandle::open(&seg_dir, uid, field).expect("open handle");
    assert!(handle.col_mmap.len() >= crate::shared::storage_header::BinaryHeader::TOTAL_LEN);
    assert!(handle.zfc_index.entries.is_empty());
}

#[test]
fn open_fails_on_invalid_col_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let segment_id = "00000";
    let uid = "uid_test";
    let field = "field_a";
    let seg_dir = base.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Write files via factory but with wrong .col header kind
    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .write_with_col_kind(FileKind::ZoneIndex);

    let err = ColumnHandle::open(&seg_dir, uid, field).expect_err("expected error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}
