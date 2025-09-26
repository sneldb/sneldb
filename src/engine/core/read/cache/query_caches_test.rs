use crate::engine::core::read::cache::QueryCaches;
use crate::test_helpers::factories::column_factory::ColumnFactory;
use crate::test_helpers::factories::zone_index_factory::ZoneIndexFactory;
use std::fs::create_dir_all;
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

    // Arc should differ after rewrite (either Reload or Miss if evicted globally)
    assert!(!Arc::ptr_eq(&a2, &a3));

    // Summary should reflect either a reload or an extra miss depending on global LRU interference
    let summary = caches.zone_index_summary_line();
    let ok_reload_path =
        summary.contains("hits=1") && summary.contains("misses=1") && summary.contains("reloads=1");
    let ok_evicted_path =
        summary.contains("hits=1") && summary.contains("misses=2") && summary.contains("reloads=0");
    assert!(ok_reload_path || ok_evicted_path, "summary: {}", summary);
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
