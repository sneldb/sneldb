use crate::engine::core::read::catalog::SegmentIndexCatalog;
use lru::LruCache;
use once_cell::sync::Lazy;
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexCatalogCacheKey {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Reload,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexCatalogCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub reloads: u64,
    pub evictions: u64,
}

#[derive(Debug)]
pub struct GlobalIndexCatalogCache {
    inner: Mutex<LruCache<IndexCatalogCacheKey, Arc<SegmentIndexCatalog>>>,
    inflight: Mutex<std::collections::HashMap<IndexCatalogCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    reloads: AtomicU64,
    evictions: AtomicU64,
}

impl GlobalIndexCatalogCache {
    fn new(capacity: usize) -> Self {
        let cap_nz = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap_nz)),
            inflight: Mutex::new(std::collections::HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            reloads: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn instance() -> &'static Self {
        &GLOBAL_INDEX_CATALOG_CACHE
    }

    pub fn stats(&self) -> IndexCatalogCacheStats {
        IndexCatalogCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    pub fn invalidate_segment(&self, segment_label: &str) {
        if let Ok(mut guard) = self.inner.lock() {
            let keys: Vec<_> = guard
                .iter()
                .filter(|(key, _)| key.path.ends_with(segment_label))
                .map(|(key, _)| key.clone())
                .collect();
            for key in keys {
                guard.pop(&key);
            }
        }

        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.retain(|key, _| !key.path.ends_with(segment_label));
        }
    }

    pub fn get_or_load(
        &self,
        base_dir: &Path,
        segment_id: &str,
        uid: &str,
    ) -> Result<(Arc<SegmentIndexCatalog>, CacheOutcome), io::Error> {
        let path = base_dir.join(segment_id).join(format!("{}.icx", uid));
        let key = IndexCatalogCacheKey { path: path.clone() };

        // Fast path
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(entry_arc), CacheOutcome::Hit));
            }
        }

        // Singleflight
        let lock_arc = {
            let mut map = self.inflight.lock().unwrap();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _loader_guard = lock_arc.lock().unwrap();

        // Re-check
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(entry_arc), CacheOutcome::Hit));
            }
        }

        // Load
        let catalog = match fs::metadata(&path) {
            Ok(_) => SegmentIndexCatalog::load(&path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
            Err(e) => {
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("icx metadata: {}", e),
                ));
            }
        };

        let arc = Arc::new(catalog);

        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let had_before = guard.contains(&key);
            let will_evict = !had_before && guard.len() == guard.cap().get();
            guard.put(key.clone(), Arc::clone(&arc));
            if will_evict {
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
            if had_before {
                self.reloads.fetch_add(1, Ordering::Relaxed);
                CacheOutcome::Reload
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                CacheOutcome::Miss
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            CacheOutcome::Miss
        };

        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((arc, outcome))
    }
}

pub static GLOBAL_INDEX_CATALOG_CACHE: Lazy<GlobalIndexCatalogCache> =
    Lazy::new(|| GlobalIndexCatalogCache::new(2048));
