use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use super::column_handle::ColumnHandle;
use super::column_handle_key::ColumnHandleKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
}

#[derive(Debug)]
pub struct GlobalColumnHandleCache {
    inner: Mutex<LruCache<ColumnHandleKey, Arc<ColumnHandle>>>,
}

impl GlobalColumnHandleCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(LruCache::new(NonZeroUsize::new(2048).unwrap())),
        }
    }

    pub fn instance() -> &'static Self {
        static INSTANCE: Lazy<GlobalColumnHandleCache> =
            Lazy::new(|| GlobalColumnHandleCache::new());
        &INSTANCE
    }

    pub fn get_or_open(
        &self,
        key: ColumnHandleKey,
        segment_dir: &std::path::Path,
        uid: &str,
        field: &str,
    ) -> Result<(Arc<ColumnHandle>, CacheOutcome), io::Error> {
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(entry) = guard.get(&key) {
                return Ok((Arc::clone(entry), CacheOutcome::Hit));
            }
        }

        // Open outside lock
        let handle = ColumnHandle::open(segment_dir, uid, field)?;
        let arc = Arc::new(handle);

        if let Ok(mut guard) = self.inner.lock() {
            guard.put(key, Arc::clone(&arc));
        }

        Ok((arc, CacheOutcome::Miss))
    }
}
