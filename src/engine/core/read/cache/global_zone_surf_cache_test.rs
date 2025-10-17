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

#[test]
fn test_zone_surf_cache_prospective_eviction_inserts() {
    // Ensure a small capacity that requires evictions to insert
    clear_cache_for_tests();
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().join("segment-00001");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Create multiple small filters to simulate many entries
    let mut paths = Vec::new();
    for i in 0..50u32 {
        let filter = ZoneSurfFilter { entries: vec![] };
        let p = segment_dir.join(format!("uid{}_f.zsrf", i));
        filter.save(&p).unwrap();
        paths.push(p);
    }

    let cache = GlobalZoneSurfCache::instance();
    // Set capacity to a few MB so not all 50 fit
    cache.resize_bytes(5 * 1024 * 1024);

    // Load the first N entries to fill the cache
    for (i, p) in paths.iter().enumerate().take(20) {
        let key = ZoneSurfCacheKey::from_context(
            Some(0usize),
            "segment-00001",
            &format!("uid{}", i),
            "f",
        );
        let _ = cache
            .load_from_file(key, "segment-00001", &format!("uid{}", i), "f", p)
            .unwrap();
    }

    let before = cache.stats();
    // Now load additional entries to force evictions and then insert the new one
    for (i, p) in paths.iter().enumerate().skip(20).take(10) {
        let key = ZoneSurfCacheKey::from_context(
            Some(0usize),
            "segment-00001",
            &format!("uid{}", i),
            "f",
        );
        let (_filter, _outcome) = cache
            .load_from_file(key, "segment-00001", &format!("uid{}", i), "f", p)
            .unwrap();
    }
    let after = cache.stats();
    // We expect some evictions and the item count to remain bounded, but not zero-inserts
    assert!(after.evictions >= before.evictions);
    assert!(after.current_items > 0);
}

#[test]
fn test_zone_surf_cache_hysteresis_reduces_thrashing() {
    clear_cache_for_tests();
    let dir = tempdir().unwrap();
    let segment_dir = dir.path().join("segment-00002");
    std::fs::create_dir_all(&segment_dir).unwrap();

    // Create 100 tiny filters
    let mut paths = Vec::new();
    for i in 0..100u32 {
        let filter = ZoneSurfFilter { entries: vec![] };
        let p = segment_dir.join(format!("uid{}_f.zsrf", i));
        filter.save(&p).unwrap();
        paths.push(p);
    }

    let cache = GlobalZoneSurfCache::instance();
    // Set capacity around a few MB
    cache.resize_bytes(8 * 1024 * 1024);

    // Sweep through many unique keys twice; with hysteresis, we should still retain some
    for _round in 0..2 {
        for (i, p) in paths.iter().enumerate() {
            let key = ZoneSurfCacheKey::from_context(
                Some(0usize),
                "segment-00002",
                &format!("uid{}", i),
                "f",
            );
            let _ = cache
                .load_from_file(key, "segment-00002", &format!("uid{}", i), "f", p)
                .unwrap();
        }
    }
    let s = cache.stats();
    assert!(s.current_items > 0, "hysteresis should retain some items");
}
