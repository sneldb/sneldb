use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use crate::engine::core::read::flow::ColumnBatch;
use crate::engine::materialize::MaterializationError;
use crate::engine::materialize::StoredFrameMeta;

use super::materialized_frame_cache_entry::MaterializedFrameCacheEntry;
use super::materialized_frame_cache_key::MaterializedFrameCacheKey;
use super::materialized_frame_cache_stats::MaterializedFrameCacheStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
}

/// Estimates the size of a ColumnBatch in bytes.
/// Uses the uncompressed_len from metadata as a proxy for the decoded batch size.
fn estimate_batch_size(meta: &StoredFrameMeta) -> usize {
    // Use uncompressed_len as the size estimate - this represents the decoded data size
    // Add some overhead for the ColumnBatch structure itself
    let overhead = 1024; // Rough overhead for schema Arc, Vec allocations, etc.
    meta.uncompressed_len as usize + overhead
}

#[derive(Debug)]
pub struct GlobalMaterializedFrameCache {
    inner: Mutex<LruCache<MaterializedFrameCacheKey, Arc<MaterializedFrameCacheEntry>>>,
    inflight: Mutex<std::collections::HashMap<MaterializedFrameCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    current_bytes: AtomicUsize,
    capacity_bytes: AtomicUsize,
}

impl GlobalMaterializedFrameCache {
    fn new(capacity_bytes: usize, max_items_hint: usize) -> Self {
        let cap_nz = NonZeroUsize::new(max_items_hint.max(1)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap_nz)),
            inflight: Mutex::new(std::collections::HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            current_bytes: AtomicUsize::new(0),
            capacity_bytes: AtomicUsize::new(capacity_bytes),
        }
    }

    pub fn instance() -> &'static Self {
        &GLOBAL_MATERIALIZED_FRAME_CACHE
    }

    pub fn stats(&self) -> MaterializedFrameCacheStats {
        let current_bytes = self.current_bytes.load(Ordering::Relaxed);
        let current_items = if let Ok(guard) = self.inner.lock() {
            guard.len()
        } else {
            0
        };

        MaterializedFrameCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes,
            capacity_bytes: self.capacity_bytes.load(Ordering::Relaxed),
            current_items,
        }
    }

    pub fn resize_bytes(&self, new_capacity_bytes: usize) {
        self.capacity_bytes
            .store(new_capacity_bytes, Ordering::Relaxed);
        self.evict_until_within_cap();
    }

    /// Remove an entry from the cache and return it if it exists.
    /// This is useful for zero-copy extraction when you need ownership.
    /// Returns the Arc<ColumnBatch> (cloning the Arc is cheap, just increments refcount).
    pub fn take(&self, frame_dir: &Path, file_name: &str) -> Option<Arc<ColumnBatch>> {
        let key = MaterializedFrameCacheKey::from_file_name(frame_dir, file_name);
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry) = guard.pop(&key) {
                let size = entry.estimated_size();
                self.current_bytes.fetch_sub(size, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                // Clone the Arc (cheap - just increments refcount)
                return Some(Arc::clone(&entry.batch));
            }
        }
        None
    }

    /// Re-insert an entry into the cache after temporary removal.
    pub fn put_back(&self, frame_dir: &Path, meta: &StoredFrameMeta, batch: Arc<ColumnBatch>) {
        let key = MaterializedFrameCacheKey::from_file_name(frame_dir, &meta.file_name);
        let estimated_size = estimate_batch_size(meta);
        let entry = Arc::new(MaterializedFrameCacheEntry::new(batch, estimated_size));

        if let Ok(mut guard) = self.inner.lock() {
            // Evict if needed to make space
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            while self.current_bytes.load(Ordering::Relaxed) + estimated_size > cap
                && !guard.is_empty()
            {
                if let Some((_k, v)) = guard.pop_lru() {
                    let evicted_size = v.estimated_size();
                    self.current_bytes
                        .fetch_sub(evicted_size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            // Insert if we have space
            if self.current_bytes.load(Ordering::Relaxed) + estimated_size <= cap {
                guard.put(key, entry);
                self.current_bytes
                    .fetch_add(estimated_size, Ordering::Relaxed);
            }
        }
    }

    pub fn get_or_load<F>(
        &self,
        frame_dir: &Path,
        meta: &StoredFrameMeta,
        loader: F,
    ) -> Result<(Arc<ColumnBatch>, CacheOutcome), MaterializationError>
    where
        F: FnOnce() -> Result<ColumnBatch, MaterializationError>,
    {
        let key = MaterializedFrameCacheKey::from_file_name(frame_dir, &meta.file_name);

        // Try hit
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((Arc::clone(&entry.batch), CacheOutcome::Hit));
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
            if let Some(entry) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(&entry.batch), CacheOutcome::Hit));
            }
        }

        // Load outside cache lock
        let batch = loader()?;
        let estimated_size = estimate_batch_size(meta);
        let batch_arc = Arc::new(batch);
        let entry = Arc::new(MaterializedFrameCacheEntry::new(
            Arc::clone(&batch_arc),
            estimated_size,
        ));

        // Insert and manage eviction
        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let had_before = guard.contains(&key);

            // If replacing existing, subtract its bytes first
            if had_before {
                if let Some(prev) = guard.pop(&key) {
                    let prev_size = prev.estimated_size();
                    self.current_bytes.fetch_sub(prev_size, Ordering::Relaxed);
                }
            }

            // Preempt internal count-based eviction by popping LRU entries ourselves
            while guard.len() >= guard.cap().get() {
                if let Some((_k, v)) = guard.pop_lru() {
                    let evicted_size = v.estimated_size();
                    self.current_bytes
                        .fetch_sub(evicted_size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            // Evict by bytes if needed
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            while self.current_bytes.load(Ordering::Relaxed) + estimated_size > cap
                && !guard.is_empty()
            {
                if let Some((_k, v)) = guard.pop_lru() {
                    let evicted_size = v.estimated_size();
                    self.current_bytes
                        .fetch_sub(evicted_size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            // Insert the new entry if we have space
            if self.current_bytes.load(Ordering::Relaxed) + estimated_size <= cap {
                guard.put(key.clone(), Arc::clone(&entry));
                self.current_bytes
                    .fetch_add(estimated_size, Ordering::Relaxed);
                self.misses.fetch_add(1, Ordering::Relaxed);
                CacheOutcome::Miss
            } else {
                // No space available after eviction; skip insert
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

        Ok((batch_arc, outcome))
    }

    pub fn invalidate_frame(&self, frame_dir: &Path, file_name: &str) {
        let key = MaterializedFrameCacheKey::from_file_name(frame_dir, file_name);
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(removed) = guard.pop(&key) {
                let size = removed.estimated_size();
                self.current_bytes.fetch_sub(size, Ordering::Relaxed);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }

        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.remove(&key);
        }
    }

    pub fn invalidate_all(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.clear();
        }
        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.clear();
        }
        self.current_bytes.store(0, Ordering::Relaxed);
        // Note: We don't reset hits/misses/evictions as they're cumulative stats
    }

    fn evict_until_within_cap(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            while self.current_bytes.load(Ordering::Relaxed) > cap {
                if let Some((_k, v)) = guard.pop_lru() {
                    let evicted_size = v.estimated_size();
                    self.current_bytes
                        .fetch_sub(evicted_size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }
    }
}

// Default capacity: 512 MiB for materialized frames (they can be large)
static GLOBAL_MATERIALIZED_FRAME_CACHE: Lazy<GlobalMaterializedFrameCache> =
    Lazy::new(|| GlobalMaterializedFrameCache::new(512 * 1024 * 1024, 10_000));
