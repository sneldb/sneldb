use crate::engine::core::read::cache::global_temporal_index_cache::{
    GlobalFieldTemporalIndexCache, GlobalTemporalIndexCache,
};
use crate::engine::core::time::ZoneTemporalIndex;
use std::sync::Arc;

#[test]
fn temporal_index_global_cache_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00000";
    let uid = "uid_tfi";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    let zti = ZoneTemporalIndex::from_timestamps(vec![1, 2, 3], 1, 4);
    zti.save(uid, 9, &seg_dir).expect("save tfi");

    let cache = GlobalTemporalIndexCache::instance();
    let a1 = cache
        .get_or_load(&base_dir, segment_id, uid, 9)
        .expect("load1");
    let a2 = cache
        .get_or_load(&base_dir, segment_id, uid, 9)
        .expect("load2");
    assert!(Arc::ptr_eq(&a1, &a2), "expected same Arc from global cache");
}

#[test]
fn field_temporal_index_global_cache_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00001";
    let uid = "uid_ftfi";
    let field = "created_at";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    let zti = ZoneTemporalIndex::from_timestamps(vec![10, 20, 30], 1, 4);
    zti.save_for_field(uid, field, 3, &seg_dir)
        .expect("save field tfi");

    let cache = GlobalFieldTemporalIndexCache::instance();
    let a1 = cache
        .get_or_load(&base_dir, segment_id, uid, field, 3)
        .expect("load1");
    let a2 = cache
        .get_or_load(&base_dir, segment_id, uid, field, 3)
        .expect("load2");
    assert!(
        Arc::ptr_eq(&a1, &a2),
        "expected same Arc from global field cache"
    );
}
