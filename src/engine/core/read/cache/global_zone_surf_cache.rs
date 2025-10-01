use std::io;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use super::zone_surf_cache_entry::ZoneSurfCacheEntry;
use super::zone_surf_cache_key::ZoneSurfCacheKey;
use crate::engine::core::filter::zone_surf_filter::ZoneSurfFilter;
use crate::shared::storage_header::open_and_header_offset;

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
    fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(
                NonZeroUsize::new(1000).unwrap(), // Max items - will be limited by byte capacity
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
            if let Some(entry) = guard.get(&key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
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
            let current_bytes = self.current_bytes.load(Ordering::Relaxed);
            let capacity_bytes = self.capacity_bytes.load(Ordering::Relaxed);

            // Evict items if we would exceed capacity
            while current_bytes + estimated_size > capacity_bytes && !guard.is_empty() {
                if let Some((_, evicted_entry)) = guard.pop_lru() {
                    let evicted_size = evicted_entry.estimated_size();
                    self.current_bytes
                        .fetch_sub(evicted_size, Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            // Only insert if we have space
            if current_bytes + estimated_size <= capacity_bytes {
                guard.put(key.clone(), Arc::clone(&entry_arc));
                self.current_bytes
                    .fetch_add(estimated_size, Ordering::Relaxed);
                self.misses.fetch_add(1, Ordering::Relaxed);
                CacheOutcome::Miss
            } else {
                // No space available, but we still loaded it
                self.misses.fetch_add(1, Ordering::Relaxed);
                CacheOutcome::Miss
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            CacheOutcome::Miss
        };

        // Clean up inflight
        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((filter, outcome))
    }

    /// Load a surf filter from file with validation
    pub fn load_from_file(
        &self,
        key: ZoneSurfCacheKey,
        path: &Path,
    ) -> Result<(Arc<ZoneSurfFilter>, CacheOutcome), io::Error> {
        let segment_id = key.segment_id.clone();
        let uid = key.uid.clone();
        let field = key.field.clone();

        self.get_or_load(key, || {
            // Get file metadata for validation
            let (ino, mtime, size) = file_identity(path)?;

            // Load the surf filter
            let (file, _header_offset) = open_and_header_offset(
                path,
                crate::shared::storage_header::FileKind::ZoneSurfFilter.magic(),
            )?;
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
            let filter: ZoneSurfFilter = bincode::deserialize(
                &mmap[crate::shared::storage_header::BinaryHeader::TOTAL_LEN..],
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            Ok(ZoneSurfCacheEntry::new(
                Arc::new(filter),
                path.to_path_buf(),
                segment_id,
                uid,
                field,
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
