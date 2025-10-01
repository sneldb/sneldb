#![cfg(test)]

use super::*;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use std::sync::Arc;
use tempfile::tempdir;

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
    let key = ZoneSurfCacheKey::new("segment-00000", "test_uid", "test_field");

    // First load should be a miss
    let (loaded_filter, outcome) = cache.load_from_file(key.clone(), &path).unwrap();
    assert_eq!(outcome, global_zone_surf_cache::CacheOutcome::Miss);
    assert_eq!(loaded_filter.entries.len(), 0);

    // Second load should be a hit
    let (loaded_filter2, outcome2) = cache.load_from_file(key, &path).unwrap();
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
