use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::catalog::{
    CatalogIndex, EntryFile, IndexFile, MaterializationEntry,
};
use lru::LruCache;
use once_cell::sync::Lazy;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogIndexCacheKey {
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntryCacheKey {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Reload,
}

#[derive(Debug, Clone, Copy)]
pub struct MaterializationCatalogCacheStats {
    pub index_hits: u64,
    pub index_misses: u64,
    pub entry_hits: u64,
    pub entry_misses: u64,
    pub index_evictions: u64,
    pub entry_evictions: u64,
}

/// Global cache for materialization catalog index and individual entries
///
/// This cache provides:
/// - Fast access to the lightweight index (name -> path mappings)
/// - On-demand loading of individual entry files
/// - Singleflight pattern to prevent duplicate loads
/// - LRU eviction for memory management
#[derive(Debug)]
pub struct GlobalMaterializationCatalogCache {
    // Cache for the lightweight index file
    index_cache: Mutex<LruCache<CatalogIndexCacheKey, Arc<CatalogIndex>>>,
    // Cache for individual entry files
    entry_cache: Mutex<LruCache<EntryCacheKey, Arc<MaterializationEntry>>>,
    // Singleflight locks for index
    index_inflight: Mutex<std::collections::HashMap<CatalogIndexCacheKey, Arc<Mutex<()>>>>,
    // Singleflight locks for entries
    entry_inflight: Mutex<std::collections::HashMap<EntryCacheKey, Arc<Mutex<()>>>>,
    // Statistics
    index_hits: AtomicU64,
    index_misses: AtomicU64,
    entry_hits: AtomicU64,
    entry_misses: AtomicU64,
    index_evictions: AtomicU64,
    entry_evictions: AtomicU64,
}

impl GlobalMaterializationCatalogCache {
    fn new(index_capacity: usize, entry_capacity: usize) -> Self {
        let index_cap_nz = NonZeroUsize::new(index_capacity.max(1)).unwrap();
        let entry_cap_nz = NonZeroUsize::new(entry_capacity.max(1)).unwrap();

        Self {
            index_cache: Mutex::new(LruCache::new(index_cap_nz)),
            entry_cache: Mutex::new(LruCache::new(entry_cap_nz)),
            index_inflight: Mutex::new(std::collections::HashMap::new()),
            entry_inflight: Mutex::new(std::collections::HashMap::new()),
            index_hits: AtomicU64::new(0),
            index_misses: AtomicU64::new(0),
            entry_hits: AtomicU64::new(0),
            entry_misses: AtomicU64::new(0),
            index_evictions: AtomicU64::new(0),
            entry_evictions: AtomicU64::new(0),
        }
    }

    pub fn instance() -> &'static Self {
        &GLOBAL_MATERIALIZATION_CATALOG_CACHE
    }

    pub fn stats(&self) -> MaterializationCatalogCacheStats {
        MaterializationCatalogCacheStats {
            index_hits: self.index_hits.load(Ordering::Relaxed),
            index_misses: self.index_misses.load(Ordering::Relaxed),
            entry_hits: self.entry_hits.load(Ordering::Relaxed),
            entry_misses: self.entry_misses.load(Ordering::Relaxed),
            index_evictions: self.index_evictions.load(Ordering::Relaxed),
            entry_evictions: self.entry_evictions.load(Ordering::Relaxed),
        }
    }

    /// Get or load the catalog index
    pub fn get_or_load_index(
        &self,
        index_path: &Path,
    ) -> Result<(Arc<CatalogIndex>, CacheOutcome), MaterializationError> {
        let key = CatalogIndexCacheKey {
            path: index_path.to_path_buf(),
        };

        // Fast path: check cache
        if let Ok(mut guard) = self.index_cache.lock() {
            if let Some(index_arc) = guard.get(&key) {
                self.index_hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(index_arc), CacheOutcome::Hit));
            }
        }

        // Singleflight: prevent duplicate loads
        let lock_arc = {
            let mut map = self.index_inflight.lock().unwrap();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _loader_guard = lock_arc.lock().unwrap();

        // Re-check cache under singleflight lock
        if let Ok(mut guard) = self.index_cache.lock() {
            if let Some(index_arc) = guard.get(&key) {
                self.index_hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.index_inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(index_arc), CacheOutcome::Hit));
            }
        }

        // Load index from disk
        let index_file = IndexFile::new(key.path.clone());
        let index = index_file.load()?;
        let index_arc = Arc::new(index);

        // Insert into cache
        let outcome = if let Ok(mut guard) = self.index_cache.lock() {
            if guard.len() == guard.cap().get() {
                self.index_evictions.fetch_add(1, Ordering::Relaxed);
            }
            guard.put(key.clone(), Arc::clone(&index_arc));
            CacheOutcome::Miss
        } else {
            CacheOutcome::Miss
        };

        self.index_misses.fetch_add(1, Ordering::Relaxed);
        let mut map = self.index_inflight.lock().unwrap();
        map.remove(&key);

        Ok((index_arc, outcome))
    }

    /// Get or load a specific entry
    pub fn get_or_load_entry(
        &self,
        entry_path: &Path,
    ) -> Result<(Arc<MaterializationEntry>, CacheOutcome), MaterializationError> {
        let key = EntryCacheKey {
            path: entry_path.to_path_buf(),
        };

        // Fast path: check cache
        if let Ok(mut guard) = self.entry_cache.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                self.entry_hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(entry_arc), CacheOutcome::Hit));
            }
        }

        // Singleflight: prevent duplicate loads
        let lock_arc = {
            let mut map = self.entry_inflight.lock().unwrap();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _loader_guard = lock_arc.lock().unwrap();

        // Re-check cache under singleflight lock
        if let Ok(mut guard) = self.entry_cache.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                self.entry_hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.entry_inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(entry_arc), CacheOutcome::Hit));
            }
        }

        // Load entry from disk
        let entry_file = EntryFile::new(key.path.clone());
        let entry = entry_file.load()?;
        let entry_arc = Arc::new(entry);

        // Insert into cache
        let outcome = if let Ok(mut guard) = self.entry_cache.lock() {
            if guard.len() == guard.cap().get() {
                self.entry_evictions.fetch_add(1, Ordering::Relaxed);
            }
            guard.put(key.clone(), Arc::clone(&entry_arc));
            CacheOutcome::Miss
        } else {
            CacheOutcome::Miss
        };

        self.entry_misses.fetch_add(1, Ordering::Relaxed);
        let mut map = self.entry_inflight.lock().unwrap();
        map.remove(&key);

        Ok((entry_arc, outcome))
    }

    /// Invalidate the index cache for a given path
    pub fn invalidate_index(&self, index_path: &Path) {
        let key = CatalogIndexCacheKey {
            path: index_path.to_path_buf(),
        };
        if let Ok(mut guard) = self.index_cache.lock() {
            guard.pop(&key);
        }
    }

    /// Invalidate an entry cache for a given path
    pub fn invalidate_entry(&self, entry_path: &Path) {
        let key = EntryCacheKey {
            path: entry_path.to_path_buf(),
        };
        if let Ok(mut guard) = self.entry_cache.lock() {
            guard.pop(&key);
        }
    }

    /// Invalidate all caches (useful for testing or full reload)
    pub fn invalidate_all(&self) {
        if let Ok(mut index_guard) = self.index_cache.lock() {
            index_guard.clear();
        }
        if let Ok(mut entry_guard) = self.entry_cache.lock() {
            entry_guard.clear();
        }
    }
}

// Default cache sizes: 10 index entries (small, frequently accessed), 1000 entry files
static GLOBAL_MATERIALIZATION_CATALOG_CACHE: Lazy<GlobalMaterializationCatalogCache> =
    Lazy::new(|| GlobalMaterializationCatalogCache::new(10, 1000));
