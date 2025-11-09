use super::seg_id::parse_segment_id_u64;
use super::zone_xor_filter_cache_entry::ZoneXorFilterCacheEntry;
use super::zone_xor_filter_cache_key::ZoneXorFilterCacheKey;
use crate::engine::core::zone::zone_xor_index::ZoneXorFilterIndex;
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
pub struct ZoneXorFilterCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub reloads: u64,
    pub evictions: u64,
    pub current_bytes: usize,
    pub current_items: usize,
}

#[derive(Debug)]
pub struct GlobalZoneXorFilterCache {
    inner: Mutex<LruCache<ZoneXorFilterCacheKey, Arc<ZoneXorFilterCacheEntry>>>,
    inflight: Mutex<std::collections::HashMap<ZoneXorFilterCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    reloads: AtomicU64,
    evictions: AtomicU64,
    current_bytes: AtomicUsize,
    capacity_bytes: AtomicUsize,
}

impl GlobalZoneXorFilterCache {
    #[inline]
    fn compute_item_cap(capacity_bytes: usize) -> usize {
        // Rough average entry size ~50 KB; ensure a sane minimum
        let avg_entry_bytes: usize = 50_000;
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
        static INSTANCE: Lazy<GlobalZoneXorFilterCache> = Lazy::new(|| {
            // Default capacity: 50MB
            GlobalZoneXorFilterCache::new(50 * 1024 * 1024)
        });
        &INSTANCE
    }

    pub fn stats(&self) -> ZoneXorFilterCacheStats {
        let current_bytes = self.current_bytes.load(Ordering::Relaxed);
        let current_items = if let Ok(guard) = self.inner.lock() {
            guard.len()
        } else {
            0
        };

        ZoneXorFilterCacheStats {
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
        key: ZoneXorFilterCacheKey,
        loader: F,
    ) -> Result<(Arc<ZoneXorFilterIndex>, CacheOutcome), io::Error>
    where
        F: FnOnce() -> Result<ZoneXorFilterCacheEntry, io::Error>,
    {
        // Try hit
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(_entry) = guard.get(&key) {
                if tracing::enabled!(tracing::Level::INFO) {
                    tracing::info!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneXorFilter cache HIT (per-process)");
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                // Return cloned index
                let entry = guard.get(&key).unwrap();
                return Ok((Arc::clone(&entry.index), CacheOutcome::Hit));
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
                    tracing::info!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneXorFilter cache HIT after singleflight check");
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                let mut map = self.inflight.lock().unwrap();
                map.remove(&key);
                return Ok((Arc::clone(&entry.index), CacheOutcome::Hit));
            }
        }

        // Load outside cache lock
        let entry = loader()?;
        let entry_arc = Arc::new(entry);
        let index = Arc::clone(&entry_arc.index);

        // Insert and manage eviction
        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let estimated_size = entry_arc.estimated_size();
            let capacity_bytes = self.capacity_bytes.load(Ordering::Relaxed);
            let mut prospective = self.current_bytes.load(Ordering::Relaxed);

            // Fast path: entry larger than total capacity, never insert
            if estimated_size > capacity_bytes {
                self.misses.fetch_add(1, Ordering::Relaxed);
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, capacity_bytes = capacity_bytes, "ZoneXorFilter entry larger than capacity; skipping insert");
                }
                CacheOutcome::Miss
            } else {
                // Check if this is a reload (key already exists)
                let had_before = guard.contains(&key);

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
                    if had_before {
                        self.reloads.fetch_add(1, Ordering::Relaxed);
                        if tracing::enabled!(tracing::Level::INFO) {
                            tracing::info!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, current_bytes = prospective + estimated_size, capacity_bytes = capacity_bytes, "ZoneXorFilter cache RELOAD -> updated entry");
                        }
                        CacheOutcome::Reload
                    } else {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                        if tracing::enabled!(tracing::Level::INFO) {
                            tracing::info!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, current_bytes = prospective + estimated_size, capacity_bytes = capacity_bytes, "ZoneXorFilter cache MISS -> inserted entry");
                        }
                        CacheOutcome::Miss
                    }
                } else {
                    // No space available after eviction; skip insert
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        tracing::debug!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, size_bytes = estimated_size, current_bytes = prospective, capacity_bytes = capacity_bytes, "ZoneXorFilter cache MISS but not inserted (over capacity)");
                    }
                    CacheOutcome::Miss
                }
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(target: "cache::zone_xor_filter", shard_id = key.shard_id, segment_id = key.segment_id, uid_id = key.uid_id, field_id = key.field_id, "ZoneXorFilter cache MISS but cache lock unavailable");
            }
            CacheOutcome::Miss
        };

        // Clean up inflight
        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((index, outcome))
    }

    pub fn invalidate_segment(&self, segment_label: &str) {
        let segment_id = parse_segment_id_u64(segment_label);
        if let Ok(mut guard) = self.inner.lock() {
            let keys: Vec<_> = guard
                .iter()
                .filter(|(key, _)| key.segment_id == segment_id)
                .map(|(key, _)| *key)
                .collect();
            for key in keys {
                if let Some(entry) = guard.pop(&key) {
                    let size = entry.estimated_size();
                    self.current_bytes.fetch_sub(size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if let Ok(mut inflight) = self.inflight.lock() {
            inflight.retain(|key, _| key.segment_id != segment_id);
        }
    }

    /// Load a zone XOR filter index from file with validation
    pub fn load_from_file(
        &self,
        key: ZoneXorFilterCacheKey,
        segment_id: &str,
        uid: &str,
        field: &str,
        path: &Path,
    ) -> Result<(Arc<ZoneXorFilterIndex>, CacheOutcome), io::Error> {
        self.get_or_load(key, || {
            // Get file metadata for validation
            let (ino, mtime, size) = match file_identity(path) {
                Ok(meta) => meta,
                Err(e) => {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        tracing::debug!(
                            target: "cache::zone_xor_filter",
                            %segment_id,
                            %uid,
                            %field,
                            path = %path.display(),
                            error = %e,
                            "Failed to get file metadata for ZoneXorFilter"
                        );
                    }
                    return Err(e);
                }
            };

            if tracing::enabled!(tracing::Level::INFO) {
                tracing::info!(target: "cache::zone_xor_filter", %segment_id, %uid, %field, path = %path.display(), ino = ino, mtime = mtime, size = size, "Loading ZoneXorFilter from file");
            }

            // Load blocking I/O in a way that works with both single-threaded and multi-threaded runtimes
            // Since this is called from a synchronous context, we use std::thread::spawn when in tokio runtime
            let index = if tokio::runtime::Handle::try_current().is_ok() {
                // We're in a tokio runtime - use std::thread::spawn to avoid blocking the runtime
                // This works for both single-threaded and multi-threaded runtimes
                let path = path.to_path_buf();
                std::thread::spawn(move || {
                    ZoneXorFilterIndex::load(&path).map_err(|e| {
                        io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
                    })
                })
                .join()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Thread join failed"))?
            } else {
                // Not in a tokio runtime, load directly
                ZoneXorFilterIndex::load(path).map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
                })
            }?;

            Ok(ZoneXorFilterCacheEntry::new(
                Arc::new(index),
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

