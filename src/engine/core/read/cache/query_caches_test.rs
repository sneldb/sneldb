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
