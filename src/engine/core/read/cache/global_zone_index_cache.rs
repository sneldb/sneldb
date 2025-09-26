use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
// no global UNIX_EPOCH import; only used under non-unix cfg inside function

use lru::LruCache;
use once_cell::sync::Lazy;

use crate::engine::core::zone::zone_index::ZoneIndex;

use super::zone_index_cache_types::{ZoneIndexCacheKey, ZoneIndexEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Reload,
}

#[derive(Debug, Clone, Copy)]
pub struct ZoneIndexCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub reloads: u64,
    pub evictions: u64,
}

#[derive(Debug)]
pub struct GlobalZoneIndexCache {
    inner: Mutex<LruCache<ZoneIndexCacheKey, Arc<RwLock<ZoneIndexEntry>>>>,
    inflight: Mutex<std::collections::HashMap<ZoneIndexCacheKey, Arc<Mutex<()>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    reloads: AtomicU64,
    evictions: AtomicU64,
}

impl GlobalZoneIndexCache {
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
        &GLOBAL_ZONE_INDEX_CACHE
    }

    /// Resizes the LRU capacity.
    ///
    /// Semantics:
    /// - Increasing capacity preserves all current entries and their recency order.
    /// - Decreasing capacity drops the least-recently-used entries until the size fits.
    /// - Recency for remaining entries is preserved, so the most recently used items survive.
    /// - Counters and inflight state are unaffected by resizing.
    pub fn resize(&self, new_capacity: usize) {
        if let Ok(mut guard) = self.inner.lock() {
            let nz = NonZeroUsize::new(new_capacity.max(1)).unwrap();
            guard.resize(nz);
        }
    }

    pub fn stats(&self) -> ZoneIndexCacheStats {
        ZoneIndexCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            reloads: self.reloads.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    pub fn get_or_load(
        &self,
        base_dir: &Path,
        segment_id: &str,
        uid: &str,
        shard_id: Option<usize>,
    ) -> Result<(Arc<ZoneIndex>, CacheOutcome), io::Error> {
        let index_path = base_dir.join(segment_id).join(format!("{}.idx", uid));
        let abs_path = canonicalize_or_identity(&index_path);
        let key = ZoneIndexCacheKey {
            path: abs_path.clone(),
        };

        // Try hit with validation
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                let (cur_ino, cur_mtime, cur_size) = file_identity(&abs_path)?;
                let entry = entry_arc.read().unwrap();
                if entry.ino == cur_ino && entry.mtime == cur_mtime && entry.size == cur_size {
                    let zi = entry.zone_index.clone();
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok((zi, CacheOutcome::Hit));
                }
            }
        }

        // Singleflight: acquire per-key loader lock
        let lock_arc = {
            let mut map = self.inflight.lock().unwrap();
            map.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _loader_guard = lock_arc.lock().unwrap();

        // Re-check under singleflight guard
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry_arc) = guard.get(&key) {
                let (cur_ino, cur_mtime, cur_size) = file_identity(&abs_path)?;
                let entry = entry_arc.read().unwrap();
                if entry.ino == cur_ino && entry.mtime == cur_mtime && entry.size == cur_size {
                    let zi = entry.zone_index.clone();
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    // Clean inflight entry before return
                    let mut map = self.inflight.lock().unwrap();
                    map.remove(&key);
                    return Ok((zi, CacheOutcome::Hit));
                }
            }
        }

        // Load outside inner cache lock
        let zi = ZoneIndex::load_from_path(&abs_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let zone_arc = Arc::new(zi);

        // Snapshot
        let (ino, mtime, size) = file_identity(&abs_path)?;

        let entry = ZoneIndexEntry::new(
            Arc::clone(&zone_arc),
            abs_path.clone(),
            segment_id.to_string(),
            uid.to_string(),
            shard_id,
            ino,
            mtime,
            size,
        );
        let entry_arc = Arc::new(RwLock::new(entry));

        // Insert/replace and determine outcome
        let outcome = if let Ok(mut guard) = self.inner.lock() {
            let had_before = guard.contains(&key);
            let will_evict = !had_before && guard.len() == guard.cap().get();
            guard.put(key.clone(), entry_arc);
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

        // Clear inflight
        let mut map = self.inflight.lock().unwrap();
        map.remove(&key);

        Ok((zone_arc, outcome))
    }
}

pub static GLOBAL_ZONE_INDEX_CACHE: Lazy<GlobalZoneIndexCache> =
    Lazy::new(|| GlobalZoneIndexCache::new(1024));

fn canonicalize_or_identity(path: &Path) -> PathBuf {
    match fs::canonicalize(path) {
        Ok(p) => p,
        Err(_) => path.to_path_buf(),
    }
}

fn file_identity(path: &Path) -> Result<(u64, i64, u64), io::Error> {
    let meta = fs::metadata(path)?;
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
