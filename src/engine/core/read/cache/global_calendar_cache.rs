use crate::engine::core::time::CalendarDir;
use lru::LruCache;
use once_cell::sync::Lazy;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct CalendarCacheKey {
    pub path: PathBuf,
}

pub struct GlobalCalendarCache {
    inner: Mutex<LruCache<CalendarCacheKey, Arc<CalendarDir>>>,
}

impl GlobalCalendarCache {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self { inner: Mutex::new(LruCache::new(cap)) }
    }

    pub fn instance() -> &'static Self { &GLOBAL_CALENDAR_CACHE }

    pub fn get_or_load(
        &self,
        base_dir: &std::path::Path,
        segment_id: &str,
        uid: &str,
    ) -> std::io::Result<Arc<CalendarDir>> {
        let path = base_dir.join(segment_id).join(format!("{}.cal", uid));
        let key = CalendarCacheKey { path: path.clone() };
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(v) = guard.get(&key) { return Ok(Arc::clone(v)); }
        }
        let dir = CalendarDir::load(uid, &path.parent().unwrap())?;
        let arc = Arc::new(dir);
        if let Ok(mut guard) = self.inner.lock() { guard.put(key, Arc::clone(&arc)); }
        Ok(arc)
    }
}

pub static GLOBAL_CALENDAR_CACHE: Lazy<GlobalCalendarCache> =
    Lazy::new(|| GlobalCalendarCache::new(1024));


