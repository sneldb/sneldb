use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use once_cell::sync::Lazy;

use super::column_handle::ColumnHandle;
use super::column_handle_key::ColumnHandleKey;
use super::seg_id::parse_segment_id_u64;

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

    pub fn invalidate_segment(&self, segment_label: &str) {
        let segment_id = parse_segment_id_u64(segment_label);
        if let Ok(mut guard) = self.inner.lock() {
            let keys: Vec<_> = guard
                .iter()
                .filter(|(k, _)| k.segment_id == segment_id)
                .map(|(k, _)| *k)
                .collect();
            for key in keys {
                guard.pop(&key);
            }
        }
    }
}
