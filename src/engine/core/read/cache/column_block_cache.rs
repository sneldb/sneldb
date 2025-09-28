use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use super::column_block_cache_key::ColumnBlockCacheKey;
use super::column_block_cache_stats::ColumnBlockCacheStats;
use super::decompressed_block::DecompressedBlock;
use super::global_zone_index_cache::CacheOutcome;

#[derive(Debug)]
pub struct GlobalColumnBlockCache {
    inner: Mutex<LruCache<ColumnBlockCacheKey, Arc<DecompressedBlock>>>,
    inflight: Mutex<std::collections::HashMap<ColumnBlockCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    reloads: AtomicU64,
    evictions: AtomicU64,
    current_bytes: AtomicUsize,
    capacity_bytes: AtomicUsize,
}

impl GlobalColumnBlockCache {
    fn new(capacity_bytes: usize, max_items_hint: usize) -> Self {
        // The underlying LruCache is count-based. We keep a byte counter and evict in a loop when exceeding capacity_bytes.
        let cap_nz = NonZeroUsize::new(max_items_hint.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap_nz)),
            inflight: Mutex::new(std::collections::HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            reloads: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            current_bytes: AtomicUsize::new(0),
            capacity_bytes: AtomicUsize::new(capacity_bytes),
        }
    }

    pub fn instance() -> &'static Self {
        &GLOBAL_COLUMN_BLOCK_CACHE
    }

    pub fn resize_bytes(&self, new_capacity_bytes: usize) {
        self.capacity_bytes
            .store(new_capacity_bytes, Ordering::Relaxed);
        self.evict_until_within_cap();
    }

    pub fn stats(&self) -> ColumnBlockCacheStats {
        ColumnBlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes: self.current_bytes.load(Ordering::Relaxed),
            capacity_bytes: self.capacity_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn get_or_load<F>(
        &self,
        col_path: &Path,
        zone_id: u32,
        loader: F,
    ) -> Result<(Arc<DecompressedBlock>, CacheOutcome), io::Error>
    where
        F: FnOnce() -> Result<Vec<u8>, io::Error>,
    {
        let abs_path = if col_path.is_absolute() {
            col_path.to_path_buf()
        } else {
            std::fs::canonicalize(col_path).unwrap_or_else(|_| col_path.to_path_buf())
        };
        let key = ColumnBlockCacheKey::new(abs_path, zone_id);

        // Try hit
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(block) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(block), CacheOutcome::Hit));
            }
        }

        // Singleflight lock
        let lock_arc = {
            let mut map = self.inflight.lock().unwrap();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _loader_guard = lock_arc.lock().unwrap();

        // Re-check under singleflight
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(block) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(block), CacheOutcome::Hit));
            }
        }

        // Load outside cache lock
        let bytes = loader()?;
        let block = Arc::new(DecompressedBlock::from_bytes(bytes));

        // Insert and manage eviction
        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let had_before = guard.contains(&key);

            // If replacing existing, subtract its bytes first
            if had_before {
                if let Some(prev) = guard.pop(&key) {
                    self.current_bytes.fetch_sub(prev.size, Ordering::Relaxed);
                }
            }

            // Preempt internal count-based eviction by popping LRU entries ourselves
            while guard.len() >= guard.cap().get() {
                if let Some((_k, v)) = guard.pop_lru() {
                    self.current_bytes.fetch_sub(v.size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            // Insert the new block
            guard.put(key.clone(), Arc::clone(&block));

            // Update size then evict by bytes if needed
            self.current_bytes.fetch_add(block.size, Ordering::Relaxed);
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            while self.current_bytes.load(Ordering::Relaxed) > cap {
                if let Some((_k, v)) = guard.pop_lru() {
                    self.current_bytes.fetch_sub(v.size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
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

        // Clear inflight
        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((block, outcome))
    }

    fn evict_until_within_cap(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            while self.current_bytes.load(Ordering::Relaxed) > cap {
                if let Some((_k, v)) = guard.pop_lru() {
                    self.current_bytes.fetch_sub(v.size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }
    }
}

pub static GLOBAL_COLUMN_BLOCK_CACHE: Lazy<GlobalColumnBlockCache> = Lazy::new(|| {
    // Default: 256 MiB capacity, with a larger item hint to minimize count-based pressure.
    GlobalColumnBlockCache::new(256 * 1024 * 1024, 65_536)
});
