use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use memmap2::MmapOptions;
use tempfile::tempdir;

use crate::engine::core::column::compression::ZoneBlockEntry;
use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::reader::io::{compressed_range, map_column_file};
use crate::engine::errors::QueryExecutionError;
use crate::shared::storage_header::{BinaryHeader, FileKind};

fn write_col_with_header(path: &PathBuf) {
    let mut f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let hdr = BinaryHeader::new(FileKind::SegmentColumn.magic(), 1, 0);
    hdr.write_to(&mut f).unwrap();
    f.flush().unwrap();
}

#[test]
fn map_column_file_success() {
    let tmp = tempdir().unwrap();
    let seg_dir = tmp.path().join("seg");
    std::fs::create_dir_all(&seg_dir).unwrap();
    let uid = "u";
    let field = "f";
    let col_path = seg_dir.join(format!("{}_{}.col", uid, field));
    write_col_with_header(&col_path);

    let mmap = map_column_file(&seg_dir, uid, field).expect("mmap ok");
    assert!(mmap.len() >= BinaryHeader::TOTAL_LEN);
}

#[test]
fn map_column_file_invalid_magic_fails() {
    let tmp = tempdir().unwrap();
    let seg_dir = tmp.path().join("seg");
    std::fs::create_dir_all(&seg_dir).unwrap();
    let col_path = seg_dir.join("u_f.col");

    // Write wrong magic
    let mut f = File::create(&col_path).unwrap();
    let hdr = BinaryHeader::new(FileKind::ZoneOffsets.magic(), 1, 0);
    hdr.write_to(&mut f).unwrap();
    f.flush().unwrap();

    let err = map_column_file(&seg_dir, "u", "f").unwrap_err();
    match err {
        QueryExecutionError::ColRead(msg) => {
            assert!(msg.contains("invalid magic"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn compressed_range_success_and_oob() {
    let entry = ZoneBlockEntry {
        zone_id: 1,
        block_start: 128,
        comp_len: 64,
        uncomp_len: 256,
        num_rows: 10,
    };
    let (start, end) = compressed_range(&entry, 256).expect("in bounds");
    assert_eq!((start, end), (128, 192));

    // OOB case
    let err = compressed_range(&entry, 150).unwrap_err();
    match err {
        QueryExecutionError::ColRead(msg) => assert!(msg.contains("OOB")),
        other => panic!("unexpected: {other:?}"),
    }
}
