use std::fs::OpenOptions;
use std::io::Write;

use tempfile::tempdir;

use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::compression::compressed_column_index::ZoneBlockEntry;
use crate::shared::storage_header::{BinaryHeader, FileKind};

#[test]
fn compressed_index_write_and_load_roundtrip() {
    let tmp = tempdir().unwrap();
    let segment_dir = tmp.path();

    let mut idx = CompressedColumnIndex::default();
    idx.entries.insert(
        1,
        ZoneBlockEntry {
            zone_id: 1,
            block_start: 128,
            comp_len: 64,
            uncomp_len: 256,
            num_rows: 3,
            in_block_offsets: vec![0, 10, 25],
        },
    );
    idx.entries.insert(
        2,
        ZoneBlockEntry {
            zone_id: 2,
            block_start: 192,
            comp_len: 40,
            uncomp_len: 100,
            num_rows: 2,
            in_block_offsets: vec![0, 20],
        },
    );

    let path = CompressedColumnIndex::path_for("001", "device", segment_dir);
    idx.write_to_path(&path).expect("write zfc failed");

    let loaded = CompressedColumnIndex::load_from_path(&path).expect("load zfc failed");

    let z1 = loaded.entries.get(&1).expect("missing zone 1");
    assert_eq!(z1.block_start, 128);
    assert_eq!(z1.comp_len, 64);
    assert_eq!(z1.uncomp_len, 256);
    assert_eq!(z1.num_rows, 3);
    assert_eq!(z1.in_block_offsets, vec![0, 10, 25]);

    let z2 = loaded.entries.get(&2).expect("missing zone 2");
    assert_eq!(z2.block_start, 192);
    assert_eq!(z2.comp_len, 40);
    assert_eq!(z2.uncomp_len, 100);
    assert_eq!(z2.num_rows, 2);
    assert_eq!(z2.in_block_offsets, vec![0, 20]);
}

#[test]
fn compressed_index_ignores_trailing_partial_record() {
    let tmp = tempdir().unwrap();
    let segment_dir = tmp.path();

    // Write a valid index with a single entry
    let mut idx = CompressedColumnIndex::default();
    idx.entries.insert(
        9,
        ZoneBlockEntry {
            zone_id: 9,
            block_start: 777,
            comp_len: 11,
            uncomp_len: 22,
            num_rows: 1,
            in_block_offsets: vec![123],
        },
    );
    let path = CompressedColumnIndex::path_for("001", "ip", segment_dir);
    idx.write_to_path(&path).expect("write zfc failed");

    // Append a header followed by a few garbage bytes simulating a truncated record tail
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    let mut buf: Vec<u8> = Vec::new();
    BinaryHeader::new(FileKind::ZoneCompressedOffsets.magic(), 1, 0)
        .write_to(&mut buf)
        .expect("write header");
    buf.extend_from_slice(&[1, 2, 3]);
    file.write_all(&buf).unwrap();

    // Should load without panic and include the valid entry; trailing bytes are ignored
    let loaded = CompressedColumnIndex::load_from_path(&path).expect("load zfc failed");
    let z = loaded.entries.get(&9).expect("missing zone 9");
    assert_eq!(z.in_block_offsets, vec![123]);
}
