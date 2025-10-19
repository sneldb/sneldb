use crate::engine::core::time::ZoneTemporalIndex;
use lru::LruCache;
use once_cell::sync::Lazy;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct TemporalIndexKey {
    pub path: PathBuf,
}

pub struct GlobalTemporalIndexCache {
    inner: Mutex<LruCache<TemporalIndexKey, Arc<ZoneTemporalIndex>>>,
}

impl GlobalTemporalIndexCache {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self { inner: Mutex::new(LruCache::new(cap)) }
    }

    pub fn instance() -> &'static Self { &GLOBAL_TEMPORAL_INDEX_CACHE }

    pub fn get_or_load(
        &self,
        base_dir: &std::path::Path,
        segment_id: &str,
        uid: &str,
        zone_id: u32,
    ) -> std::io::Result<Arc<ZoneTemporalIndex>> {
        let path = base_dir.join(segment_id).join(format!("{}_{}.tfi", uid, zone_id));
        let key = TemporalIndexKey { path: path.clone() };
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(v) = guard.get(&key) { return Ok(Arc::clone(v)); }
        }
        let dir = path.parent().unwrap();
        let zti = ZoneTemporalIndex::load(uid, zone_id, dir)?;
        let arc = Arc::new(zti);
        if let Ok(mut guard) = self.inner.lock() { guard.put(key, Arc::clone(&arc)); }
        Ok(arc)
    }
}

pub static GLOBAL_TEMPORAL_INDEX_CACHE: Lazy<GlobalTemporalIndexCache> =
    Lazy::new(|| GlobalTemporalIndexCache::new(4096));


