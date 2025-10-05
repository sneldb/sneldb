use super::*;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use std::sync::Arc;
use tempfile::tempdir;

fn clear_cache_for_tests() {
    let cache = GlobalZoneSurfCache::instance();
    cache.resize_bytes(0);
    cache.resize_bytes(100 * 1024 * 1024);
}

#[test]
fn test_zone_surf_cache_basic_functionality() {
    // Create a temporary directory
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().join("segment-00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Create a test surf filter
    let filter = ZoneSurfFilter { entries: vec![] };
    let path = segment_dir.join("test_uid_test_field.zsrf");
    filter.save(&path).unwrap();

    // Test cache loading
    let cache = GlobalZoneSurfCache::instance();
    let segment_id = "segment-00000";
    let uid = "test_uid";
    let field = "test_field";
    let key = ZoneSurfCacheKey::from_context(Some(0usize), segment_id, uid, field);

    // First load should be a miss
    let (loaded_filter, outcome) = cache
        .load_from_file(key.clone(), segment_id, uid, field, &path)
        .unwrap();
    assert_eq!(outcome, global_zone_surf_cache::CacheOutcome::Miss);
    assert_eq!(loaded_filter.entries.len(), 0);

    // Second load should be a hit
    let (loaded_filter2, outcome2) = cache
        .load_from_file(key, segment_id, uid, field, &path)
        .unwrap();
    assert_eq!(outcome2, global_zone_surf_cache::CacheOutcome::Hit);
    assert_eq!(loaded_filter2.entries.len(), 0);

    // Verify they're the same instance (shared via Arc)
    assert!(Arc::ptr_eq(&loaded_filter, &loaded_filter2));
}

#[test]
fn test_zone_surf_cache_stats() {
    let cache = GlobalZoneSurfCache::instance();
    let stats = cache.stats();

    // Basic stats should be available (all are u64/usize, so >= 0 is always true)
    // Just verify the stats struct is accessible
    let _ = stats.hits;
    let _ = stats.misses;
    let _ = stats.reloads;
    let _ = stats.evictions;
    let _ = stats.current_bytes;
    let _ = stats.current_items;
}

#[test]
fn test_zone_surf_cache_resize() {
    // Create a temporary directory
    clear_cache_for_tests();
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().join("segment-00000");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Create a test surf filter
    let filter = ZoneSurfFilter { entries: vec![] };
    let path = segment_dir.join("test_uid_test_field.zsrf");
    filter.save(&path).unwrap();

    let cache = GlobalZoneSurfCache::instance();
    let segment_id = "segment-00000";
    let uid = "test_uid";
    let field = "test_field";
    let key = ZoneSurfCacheKey::from_context(Some(0usize), segment_id, uid, field);

    // Load the filter to populate the cache
    let (_loaded_filter, _outcome) = cache
        .load_from_file(key, segment_id, uid, field, &path)
        .unwrap();

    // Check initial stats
    let stats_before = cache.stats();
    assert_eq!(stats_before.current_items, 1);
    assert!(stats_before.current_bytes > 0);

    // Resize to a smaller capacity (should trigger eviction)
    cache.resize_bytes(1); // 1 byte - should evict everything

    // Check stats after resize
    let stats_after = cache.stats();
    assert_eq!(stats_after.current_items, 0);
    assert_eq!(stats_after.current_bytes, 0);
    assert!(stats_after.evictions > stats_before.evictions);

    // Resize back to a larger capacity
    cache.resize_bytes(100 * 1024 * 1024); // 100MB

    // Load again - should work fine
    let key2 = ZoneSurfCacheKey::from_context(Some(0usize), segment_id, uid, field);
    let (_loaded_filter2, _outcome2) = cache
        .load_from_file(key2, segment_id, uid, field, &path)
        .unwrap();
    let stats_final = cache.stats();
    assert_eq!(stats_final.current_items, 1);
    assert!(stats_final.current_bytes > 0);
}
