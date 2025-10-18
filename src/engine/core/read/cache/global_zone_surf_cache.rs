use super::zone_surf_cache_entry::ZoneSurfCacheEntry;
use super::zone_surf_cache_key::ZoneSurfCacheKey;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use lru::LruCache;
use once_cell::sync::Lazy;
use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Reload,
}

#[derive(Debug, Clone, Copy)]
pub struct ZoneSurfCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub reloads: u64,
    pub evictions: u64,
    pub current_bytes: usize,
    pub current_items: usize,
}

#[derive(Debug)]
pub struct GlobalZoneSurfCache {
    inner: Mutex<LruCache<ZoneSurfCacheKey, Arc<ZoneSurfCacheEntry>>>,
    inflight: Mutex<std::collections::HashMap<ZoneSurfCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    reloads: AtomicU64,
    evictions: AtomicU64,
    current_bytes: AtomicUsize,
    capacity_bytes: AtomicUsize,
}

impl GlobalZoneSurfCache {
    #[inline]
    fn compute_item_cap(capacity_bytes: usize) -> usize {
        // Rough average entry size ~380 KB; ensure a sane minimum
        let avg_entry_bytes: usize = 380_000;
        let by_bytes = capacity_bytes.saturating_div(avg_entry_bytes);
        by_bytes.max(1000)
    }

    fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(Self::compute_item_cap(capacity_bytes)).unwrap(),
            )),
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
        static INSTANCE: Lazy<GlobalZoneSurfCache> = Lazy::new(|| {
            // Default capacity: 100MB
            GlobalZoneSurfCache::new(100 * 1024 * 1024)
        });
        &INSTANCE
    }

    pub fn stats(&self) -> ZoneSurfCacheStats {
        let current_bytes = self.current_bytes.load(Ordering::Relaxed);
        let current_items = if let Ok(guard) = self.inner.lock() {
            guard.len()
        } else {
            0
        };

        ZoneSurfCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes,
            current_items,
        }
    }

    /// Resize the cache capacity in bytes
    pub fn resize_bytes(&self, new_capacity_bytes: usize) {
        self.capacity_bytes
            .store(new_capacity_bytes, Ordering::Relaxed);
        // Resize item cap to align with byte capacity
        if let Ok(mut guard) = self.inner.lock() {
            let new_items = Self::compute_item_cap(new_capacity_bytes);
            guard.resize(NonZeroUsize::new(new_items).unwrap());
        }
        // Evict entries if we're over the new capacity
        self.evict_until_within_cap();
    }

    fn evict_until_within_cap(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            let cap = self.capacity_bytes.load(Ordering::Relaxed);
            // Hysteresis: when over capacity, evict down to 80% to create headroom
            let low_watermark = cap.saturating_mul(80).saturating_div(100);
            while self.current_bytes.load(Ordering::Relaxed) > low_watermark {
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

    pub fn get_or_load<F>(
        &self,
        key: ZoneSurfCacheKey,
        loader: F,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), io::Error>
    where
        F: FnOnce() -> Result<ZoneSurfCacheEntry, io::Error>,
    {
        // Try hit
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(_entry) = guard.get(&key) {
                // We only have compact ids here; log numeric identifiers
                if tracing::enabled!(tracing::Level::INFO) {
                    tracing::info!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneSuRF cache HIT (per-process)");
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                // Return cloned filter
                let entry = guard.get(&key).unwrap();
                return Ok((Arc::clone(&entry.filter), CacheOutcome::Hit));
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
                if tracing::enabled!(tracing::Level::INFO) {
                    tracing::info!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneSuRF cache HIT after singleflight check");
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(&entry.filter), CacheOutcome::Hit));
            }
        }

        // Load outside cache lock
        let entry = loader()?;
        let entry_arc = Arc::new(entry);
        let filter = Arc::clone(&entry_arc.filter);

        // Insert and manage eviction
        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let estimated_size = entry_arc.estimated_size();
            let capacity_bytes = self.capacity_bytes.load(Ordering::Relaxed);
            let mut prospective = self.current_bytes.load(Ordering::Relaxed);

            // Fast path: entry larger than total capacity, never insert
            if estimated_size > capacity_bytes {
                self.misses.fetch_add(1, Ordering::Relaxed);
                if tracing::enabled!(tracing::Level::WARN) {
                    tracing::warn!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, capacity_bytes = capacity_bytes, "ZoneSuRF entry larger than capacity; skipping insert");
                }
                CacheOutcome::Miss
            } else {
                // Evict until we can fit the new entry
                while prospective + estimated_size > capacity_bytes && !guard.is_empty() {
                    if let Some((_, evicted_entry)) = guard.pop_lru() {
                        let evicted_size = evicted_entry.estimated_size();
                        prospective = prospective.saturating_sub(evicted_size);
                        self.current_bytes
                            .fetch_sub(evicted_size, Ordering::Relaxed);
                        self.evictions.fetch_add(1, Ordering::Relaxed);
                    } else {
                        break;
                    }
                }
                // Apply hysteresis to reduce thrash when we had to evict
                if prospective + estimated_size > capacity_bytes {
                    let low_watermark = capacity_bytes.saturating_mul(80).saturating_div(100);
                    while prospective > low_watermark && !guard.is_empty() {
                        if let Some((_, evicted_entry)) = guard.pop_lru() {
                            let evicted_size = evicted_entry.estimated_size();
                            prospective = prospective.saturating_sub(evicted_size);
                            self.current_bytes
                                .fetch_sub(evicted_size, Ordering::Relaxed);
                            self.evictions.fetch_add(1, Ordering::Relaxed);
                        } else {
                            break;
                        }
                    }
                }

                if prospective + estimated_size <= capacity_bytes {
                    guard.put(key.clone(), Arc::clone(&entry_arc));
                    self.current_bytes
                        .fetch_add(estimated_size, Ordering::Relaxed);
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    if tracing::enabled!(tracing::Level::INFO) {
                        tracing::info!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, current_bytes = prospective + estimated_size, capacity_bytes = capacity_bytes, "ZoneSuRF cache MISS -> inserted entry");
                    }
                    CacheOutcome::Miss
                } else {
                    // No space available after eviction; skip insert
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    if tracing::enabled!(tracing::Level::WARN) {
                        tracing::warn!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, current_bytes = prospective, capacity_bytes = capacity_bytes, "ZoneSuRF cache MISS but not inserted (over capacity)");
                    }
                    CacheOutcome::Miss
                }
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            if tracing::enabled!(tracing::Level::WARN) {
                tracing::warn!(target: "cache::zone_surf", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneSuRF cache MISS but cache lock unavailable");
            }
            CacheOutcome::Miss
        };

        // Clean up inflight
        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((filter, outcome))
    }

    /// Load a surf filter from file with validation (supports compressed & legacy)
    pub fn load_from_file(
        &self,
        key: ZoneSurfCacheKey,
        segment_id: &str,
        uid: &str,
        field: &str,
        path: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), io::Error> {
        self.get_or_load(key, || {
            // Get file metadata for validation
            let (ino, mtime, size) = file_identity(path)?;
            if tracing::enabled!(tracing::Level::INFO) {
                tracing::info!(target: "cache::zone_surf", %segment_id, %uid, %field, path = %path.display(), ino = ino, mtime = mtime, size = size, "Loading ZoneSuRF from file");
            }

            // Load via format-aware loader
            let filter = ZoneSurfFilter::load(path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            Ok(ZoneSurfCacheEntry::new(
                Arc::new(filter),
                path.to_path_buf(),
                segment_id.parse::<u32>().unwrap_or_else(|_| 0),
                uid.to_string(),
                field.to_string(),
                ino,
                mtime,
                size,
            ))
        })
    }
}

fn file_identity(path: &Path) -> Result<(u64, i64, u64), io::Error> {
    let meta = std::fs::metadata(path)?;
    let size = meta.len();

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let ino = meta.ino();
        let mtime = meta.mtime();
        return Ok((ino, mtime, size));
    }

    #[cfg(not(unix))]
    {
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Ok((0, mtime, size))
    }
}
