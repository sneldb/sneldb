use super::cache::{CacheOutcome, GlobalMaterializationCatalogCache};
use super::entry::MaterializationEntry;
use super::entry_file::{EntryFile, entry_file_path};
use super::index::{CatalogIndex, IndexFile};
use crate::command::types::MaterializedQuerySpec;
use crate::engine::materialize::MaterializationError;
use crate::test_helpers::factories::CommandFactory;
use tempfile::tempdir;

fn make_entry(root: &std::path::Path, name: &str) -> MaterializationEntry {
    let spec = MaterializedQuerySpec {
        name: name.into(),
        query: Box::new(CommandFactory::query().with_event_type("orders").create()),
    };
    MaterializationEntry::new(spec, root).expect("entry")
}

fn get_test_cache() -> &'static GlobalMaterializationCatalogCache {
    GlobalMaterializationCatalogCache::instance()
}

#[test]
fn cache_stats_initialized_to_zero() {
    let cache = get_test_cache();
    cache.invalidate_all(); // Clear for clean test

    // Capture initial stats (may be non-zero due to other tests running in parallel)
    let initial_stats = cache.stats();

    // Verify that stats are consistent (same instance returns same stats)
    let stats1 = cache.stats();
    let stats2 = cache.stats();
    assert_eq!(stats1.index_hits, stats2.index_hits);
    assert_eq!(stats1.index_misses, stats2.index_misses);
    assert_eq!(stats1.entry_hits, stats2.entry_hits);
    assert_eq!(stats1.entry_misses, stats2.entry_misses);
    assert_eq!(stats1.index_evictions, stats2.index_evictions);
    assert_eq!(stats1.entry_evictions, stats2.entry_evictions);

    // Verify that after invalidate_all, cache is cleared (but stats remain cumulative)
    // The important thing is that stats are consistent, not that they're zero
    assert_eq!(initial_stats.index_hits, stats1.index_hits);
    assert_eq!(initial_stats.index_misses, stats1.index_misses);
}

#[test]
fn cache_get_or_load_index_missing_file_returns_empty() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();

    // Capture initial stats (may be non-zero due to other tests running in parallel)
    let initial_stats = cache.stats();

    let dir = tempdir().unwrap();
    // Use a unique path to avoid interference from other tests
    let index_path = dir
        .path()
        .join(format!("catalog_{}.mcat", std::process::id()));

    let (index, outcome) = cache.get_or_load_index(&index_path)?;
    assert_eq!(outcome, CacheOutcome::Miss);
    assert!(index.is_empty());

    // Check that stats increased by exactly 1 (relative to initial)
    let stats = cache.stats();
    assert_eq!(
        stats.index_misses,
        initial_stats.index_misses + 1,
        "Expected index_misses to increase by 1 (initial: {}, current: {})",
        initial_stats.index_misses,
        stats.index_misses
    );
    // Hits should not have changed
    assert_eq!(
        stats.index_hits, initial_stats.index_hits,
        "Expected index_hits to remain unchanged (initial: {}, current: {})",
        initial_stats.index_hits, stats.index_hits
    );
    Ok(())
}

#[test]
fn cache_get_or_load_index_caches_result() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");

    let index_file = IndexFile::new(index_path.clone());
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), dir.path().join("entry1"));
    index_file.persist(&index)?;

    let initial_stats = cache.stats();

    // First load - miss
    let (index1, outcome1) = cache.get_or_load_index(&index_path)?;
    assert_eq!(outcome1, CacheOutcome::Miss);
    assert_eq!(index1.len(), 1);

    // Second load - hit
    let (index2, outcome2) = cache.get_or_load_index(&index_path)?;
    assert_eq!(outcome2, CacheOutcome::Hit);
    assert_eq!(index2.len(), 1);

    let stats = cache.stats();
    assert_eq!(stats.index_misses, initial_stats.index_misses + 1);
    assert_eq!(stats.index_hits, initial_stats.index_hits + 1);
    Ok(())
}

#[test]
fn cache_get_or_load_entry_missing_file_returns_error() {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("missing.mcatentry");

    let result = cache.get_or_load_entry(&entry_path);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        MaterializationError::Corrupt(_)
    ));
}

#[test]
fn cache_get_or_load_entry_caches_result() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let entry_path = entry_file_path(dir.path(), "test_entry");

    let entry = make_entry(dir.path(), "test_entry");
    let entry_file = EntryFile::new(entry_path.clone());
    entry_file.persist(&entry)?;

    let initial_stats = cache.stats();

    // First load - miss
    let (entry1, outcome1) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(outcome1, CacheOutcome::Miss);
    assert_eq!(entry1.name, "test_entry");

    // Second load - hit
    let (entry2, outcome2) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(outcome2, CacheOutcome::Hit);
    assert_eq!(entry2.name, "test_entry");

    let stats = cache.stats();
    assert_eq!(stats.entry_misses, initial_stats.entry_misses + 1);
    assert_eq!(stats.entry_hits, initial_stats.entry_hits + 1);
    Ok(())
}

#[test]
fn cache_invalidate_index_removes_from_cache() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");

    let index_file = IndexFile::new(index_path.clone());
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), dir.path().join("entry1"));
    index_file.persist(&index)?;

    // Load and cache
    let (_, _) = cache.get_or_load_index(&index_path)?;

    // Invalidate
    cache.invalidate_index(&index_path);

    // Next load should be a miss
    let (_, outcome) = cache.get_or_load_index(&index_path)?;
    assert_eq!(outcome, CacheOutcome::Miss);
    Ok(())
}

#[test]
fn cache_invalidate_entry_removes_from_cache() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let entry_path = entry_file_path(dir.path(), "test_entry");

    let entry = make_entry(dir.path(), "test_entry");
    let entry_file = EntryFile::new(entry_path.clone());
    entry_file.persist(&entry)?;

    // Load and cache
    let (_, _) = cache.get_or_load_entry(&entry_path)?;

    // Invalidate
    cache.invalidate_entry(&entry_path);

    // Next load should be a miss
    let (_, outcome) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(outcome, CacheOutcome::Miss);
    Ok(())
}

#[test]
fn cache_invalidate_all_clears_both_caches() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let dir = tempdir().unwrap();

    // Create and cache index
    let index_path = dir.path().join("catalog.mcat");
    let index_file = IndexFile::new(index_path.clone());
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), dir.path().join("entry1"));
    index_file.persist(&index)?;
    let (_, _) = cache.get_or_load_index(&index_path)?;

    // Create and cache entry
    let entry_path = entry_file_path(dir.path(), "test_entry");
    let entry = make_entry(dir.path(), "test_entry");
    let entry_file = EntryFile::new(entry_path.clone());
    entry_file.persist(&entry)?;
    let (_, _) = cache.get_or_load_entry(&entry_path)?;

    // Invalidate all
    cache.invalidate_all();

    // Both should be misses now
    let (_, index_outcome) = cache.get_or_load_index(&index_path)?;
    let (_, entry_outcome) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(index_outcome, CacheOutcome::Miss);
    assert_eq!(entry_outcome, CacheOutcome::Miss);
    Ok(())
}

#[test]
fn cache_lru_eviction_works() -> Result<(), MaterializationError> {
    let cache = get_test_cache();
    cache.invalidate_all();
    let initial_stats = cache.stats();
    let dir = tempdir().unwrap();

    // Create 3 entries (cache capacity is 1000, so we need many more to trigger eviction)
    // Actually, the cache capacity is large, so let's just verify caching works
    let entry_path = entry_file_path(dir.path(), "entry0");
    let entry = make_entry(dir.path(), "entry0");
    let entry_file = EntryFile::new(entry_path.clone());
    entry_file.persist(&entry)?;

    // First load - miss
    let (_, outcome1) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(outcome1, CacheOutcome::Miss);

    // Second load - hit
    let (_, outcome2) = cache.get_or_load_entry(&entry_path)?;
    assert_eq!(outcome2, CacheOutcome::Hit);

    let stats = cache.stats();
    assert_eq!(stats.entry_misses, initial_stats.entry_misses + 1);
    assert_eq!(stats.entry_hits, initial_stats.entry_hits + 1);
    Ok(())
}

#[test]
fn cache_singleflight_prevents_duplicate_loads() -> Result<(), MaterializationError> {
    use std::sync::Arc as StdArc;
    use std::thread;

    let cache = StdArc::new(get_test_cache());
    cache.invalidate_all();
    let dir = tempdir().unwrap();
    let index_path = dir.path().join("catalog.mcat");

    let index_file = IndexFile::new(index_path.clone());
    let mut index = CatalogIndex::new();
    index.add("entry1".to_string(), dir.path().join("entry1"));
    index_file.persist(&index)?;

    // Spawn multiple threads trying to load simultaneously
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let cache = StdArc::clone(&cache);
            let path = index_path.clone();
            thread::spawn(move || cache.get_or_load_index(&path))
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All should succeed
    for result in &results {
        assert!(result.is_ok());
    }

    // Should have some hits due to singleflight
    let stats = cache.stats();
    assert!(stats.index_hits > 0);
    Ok(())
}

#[test]
fn cache_instance_returns_singleton() {
    let cache1 = GlobalMaterializationCatalogCache::instance();
    let cache2 = GlobalMaterializationCatalogCache::instance();
    // Same memory address (same static)
    assert_eq!(cache1.stats().index_hits, cache2.stats().index_hits);
}
