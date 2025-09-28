use crate::engine::core::column::column_reader::ColumnReader;
use crate::engine::core::column::compression::{CompressionCodec, Lz4Codec};
use crate::engine::core::read::cache::QueryCaches;
use crate::shared::storage_header::BinaryHeader;
use crate::test_helpers::factories::column_factory::ColumnFactory;
use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

#[test]
fn zone_index_cache_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-00000";
    let uid = "uid_test";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Build and write a tiny ZoneIndex
    let index = ZoneIndexFactory::new().with_entry("ev", "ctx", 1).create();
    index
        .write_to_path(&seg_dir.join(format!("{}.idx", uid)))
        .expect("write zone index");

    let caches = QueryCaches::new(base_dir);

    let a1 = caches.get_or_load_zone_index(segment_id, uid).unwrap();
    let a2 = caches.get_or_load_zone_index(segment_id, uid).unwrap();
    assert!(Arc::ptr_eq(&a1, &a2), "expected same Arc from cache");

    // Validate per-query counters reflect 1 miss then 1 hit
    let summary = caches.zone_index_summary_line();
    assert!(summary.contains("hits=1"), "summary: {}", summary);
    assert!(summary.contains("misses=1"), "summary: {}", summary);
}

#[test]
fn zone_index_cache_per_query_counters_miss_hit_reload() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-reload";
    let uid = "uid_test";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Initial index
    let index = ZoneIndexFactory::new().with_entry("ev", "ctx", 1).create();
    index
        .write_to_path(&seg_dir.join(format!("{}.idx", uid)))
        .expect("write zone index");

    let caches = QueryCaches::new(base_dir.clone());

    // Miss then Hit
    let a1 = caches.get_or_load_zone_index(segment_id, uid).unwrap();
    let a2 = caches.get_or_load_zone_index(segment_id, uid).unwrap();
    assert!(Arc::ptr_eq(&a1, &a2));

    // Rewrite index with mtime tick to force reload
    std::thread::sleep(std::time::Duration::from_millis(1200));
    let index2 = ZoneIndexFactory::new()
        .with_entry("ev", "ctx", 1)
        .with_entry("ev", "ctx", 2)
        .create();
    index2
        .write_to_path(&seg_dir.join(format!("{}.idx", uid)))
        .expect("rewrite zone index");

    let a3 = caches.get_or_load_zone_index(segment_id, uid).unwrap();

    // With no reload semantics in global cache, per-query memoization should return same Arc and count as Hit
    assert!(Arc::ptr_eq(&a2, &a3));

    let summary = caches.zone_index_summary_line();
    // Expect 2 hits (second and third call) and 1 miss (first call), 0 reloads
    assert!(summary.contains("hits=2"), "summary: {}", summary);
    assert!(summary.contains("misses=1"), "summary: {}", summary);
    assert!(summary.contains("reloads=0"), "summary: {}", summary);
}

#[test]
fn zone_index_cache_shared_global_cache_across_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-shared";
    let uid = "uid_test";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    let index = ZoneIndexFactory::new().with_entry("ev", "ctx", 1).create();
    index
        .write_to_path(&seg_dir.join(format!("{}.idx", uid)))
        .expect("write zone index");

    // First query: load once (miss)
    let caches1 = QueryCaches::new(base_dir.clone());
    let _ = caches1.get_or_load_zone_index(segment_id, uid).unwrap();
    let s1 = caches1.zone_index_summary_line();
    assert!(s1.contains("hits=0"), "summary: {}", s1);
    assert!(s1.contains("misses=1"), "summary: {}", s1);

    // Second query: first load should be a Hit due to global cache
    let caches2 = QueryCaches::new(base_dir.clone());
    let _ = caches2.get_or_load_zone_index(segment_id, uid).unwrap();
    let s2 = caches2.zone_index_summary_line();
    assert!(s2.contains("hits=1"), "summary: {}", s2);
    assert!(s2.contains("misses=0"), "summary: {}", s2);

    // Another hit for caches2
    let _ = caches2.get_or_load_zone_index(segment_id, uid).unwrap();
    let s2b = caches2.zone_index_summary_line();
    assert!(s2b.contains("hits=2"), "summary: {}", s2b);
}

#[test]
fn zone_index_cache_multiple_segments_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();

    let segments = ["segA", "segB"];
    for seg in &segments {
        let dir = base_dir.join(seg);
        create_dir_all(&dir).unwrap();
        let idx = ZoneIndexFactory::new().with_entry("ev", "ctx", 1).create();
        idx.write_to_path(&dir.join("u.idx"))
            .expect("write zone index");
    }

    let caches = QueryCaches::new(base_dir);

    // First round: both misses
    let _ = caches.get_or_load_zone_index("segA", "u").unwrap();
    let _ = caches.get_or_load_zone_index("segB", "u").unwrap();

    // Second round: both hits
    let _ = caches.get_or_load_zone_index("segA", "u").unwrap();
    let _ = caches.get_or_load_zone_index("segB", "u").unwrap();

    let summary = caches.zone_index_summary_line();
    assert!(summary.contains("hits=2"), "summary: {}", summary);
    assert!(summary.contains("misses=2"), "summary: {}", summary);
}

#[test]
fn zone_index_cache_missing_file_error_counters_unchanged() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-missing";
    let uid = "uid_test";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    let caches = QueryCaches::new(base_dir);
    let res = caches.get_or_load_zone_index(segment_id, uid);
    assert!(res.is_err());

    let summary = caches.zone_index_summary_line();
    assert!(summary.contains("hits=0"), "summary: {}", summary);
    assert!(summary.contains("misses=0"), "summary: {}", summary);
    assert!(summary.contains("reloads=0"), "summary: {}", summary);
}

#[test]
fn column_handle_cache_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-00000";
    let uid = "uid_test";
    let field = "field_a";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Minimal valid files via ColumnFactory
    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .write_minimal();

    let caches = QueryCaches::new(base_dir);
    let h1 = caches
        .get_or_load_column_handle(segment_id, uid, field)
        .unwrap();
    let h2 = caches
        .get_or_load_column_handle(segment_id, uid, field)
        .unwrap();
    assert!(Arc::ptr_eq(&h1, &h2), "expected same Arc from cache");
}

fn write_decompressed_values(values: &[&str]) -> (Vec<u8>, Vec<u32>) {
    let mut buf = Vec::new();
    let mut offsets = Vec::new();
    let mut cursor: u32 = 0;
    for v in values {
        offsets.push(cursor);
        let bytes = v.as_bytes();
        let len = bytes.len() as u16;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(bytes);
        cursor += 2 + bytes.len() as u32;
    }
    (buf, offsets)
}

#[test]
fn per_query_memo_for_decompressed_blocks_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-arc";
    let uid = "uid_z";
    let field = "f";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    // Build decompressed payload and offsets
    let (decomp, offsets) = write_decompressed_values(&["a", "bb"]);
    let codec = Lz4Codec;
    let comp = CompressionCodec::compress(&codec, &decomp).expect("compress");

    let block_start = BinaryHeader::TOTAL_LEN as u64;
    let zone_id = 7u32;
    // Write zfc and col header
    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .with_zfc_entry(
            zone_id,
            block_start,
            comp.len() as u32,
            decomp.len() as u32,
            offsets.len() as u32,
            offsets.clone(),
        )
        .write_minimal();

    // Append compressed block at the specified offset
    let col_path = seg_dir.join(format!("{}_{}.col", uid, field));
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&col_path)
        .unwrap();
    f.seek(SeekFrom::Start(block_start)).unwrap();
    f.write_all(&comp).unwrap();
    f.flush().unwrap();

    let caches = QueryCaches::new(base_dir.clone());
    let handle = caches
        .get_or_load_column_handle(segment_id, uid, field)
        .expect("handle");
    let entry = handle.zfc_index.entries.get(&zone_id).expect("entry");

    let b1 = caches
        .get_or_load_decompressed_block(&handle, segment_id, uid, field, zone_id, entry)
        .expect("block1");
    let b2 = caches
        .get_or_load_decompressed_block(&handle, segment_id, uid, field, zone_id, entry)
        .expect("block2");
    assert!(
        Arc::ptr_eq(&b1, &b2),
        "expected same Arc from per-query memo"
    );
}

#[test]
fn column_reader_loads_values_using_per_query_memo() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "segment-read";
    let uid = "uid_r";
    let field = "f";
    let seg_dir = base_dir.join(segment_id);
    create_dir_all(&seg_dir).unwrap();

    let (decomp, offsets) = write_decompressed_values(&["hello", "z"]);
    let codec = Lz4Codec;
    let comp = CompressionCodec::compress(&codec, &decomp).expect("compress");
    let block_start = BinaryHeader::TOTAL_LEN as u64;
    let zone_id = 5u32;

    let _ = ColumnFactory::new()
        .with_segment_dir(&seg_dir)
        .with_uid(uid)
        .with_field(field)
        .with_zfc_entry(
            zone_id,
            block_start,
            comp.len() as u32,
            decomp.len() as u32,
            offsets.len() as u32,
            offsets.clone(),
        )
        .write_minimal();

    let col_path = seg_dir.join(format!("{}_{}.col", uid, field));
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&col_path)
        .unwrap();
    f.seek(SeekFrom::Start(block_start)).unwrap();
    f.write_all(&comp).unwrap();
    f.flush().unwrap();

    let caches = QueryCaches::new(base_dir.clone());

    // First load
    let vals1 = ColumnReader::load_for_zone_with_cache(
        &base_dir,
        segment_id,
        uid,
        field,
        zone_id,
        Some(&caches),
    )
    .expect("load1");
    assert_eq!(vals1, vec!["hello".to_string(), "z".to_string()]);

    // Second load should reuse per-query memo/global cache transparently
    let vals2 = ColumnReader::load_for_zone_with_cache(
        &base_dir,
        segment_id,
        uid,
        field,
        zone_id,
        Some(&caches),
    )
    .expect("load2");
    assert_eq!(vals2, vals1);
}
