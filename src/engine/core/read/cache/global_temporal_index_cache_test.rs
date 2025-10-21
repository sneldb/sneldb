use crate::engine::core::read::cache::global_temporal_index_cache::GlobalFieldTemporalIndexCache;
use crate::engine::core::time::ZoneTemporalIndex;
use std::sync::Arc;

#[test]
fn field_temporal_index_global_cache_reuses_arc() {
    let tmp = tempfile::tempdir().unwrap();
    let base_dir = tmp.path().to_path_buf();
    let segment_id = "00001";
    let uid = "uid_ftfi";
    let field = "created_at";
    let seg_dir = base_dir.join(segment_id);
    std::fs::create_dir_all(&seg_dir).unwrap();

    // Write slab by saving a single zone via slab API
    let zti = ZoneTemporalIndex::from_timestamps(vec![10, 20, 30], 1, 4);
    // Use slab writer with one entry to ensure slab path is tested
    ZoneTemporalIndex::save_field_slab(uid, field, &seg_dir, &[(3u32, &zti)])
        .expect("save field slab tfi");

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
