use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::engine::core::zone::zone_index::ZoneIndex;

use super::column_handle::ColumnHandle;
use super::global_zone_index_cache::{CacheOutcome, GlobalZoneIndexCache};

#[derive(Debug)]
pub struct QueryCaches {
    pub(crate) base_dir: PathBuf,
    shard_id: Option<usize>,
    // Per-query counters
    zone_index_hits: AtomicU64,
    zone_index_misses: AtomicU64,
    zone_index_reloads: AtomicU64,
    column_handle_by_key: Mutex<HashMap<(String, String, String), Arc<ColumnHandle>>>,
}

impl QueryCaches {
    pub fn new(base_dir: PathBuf) -> Self {
        let shard_id = parse_shard_id(&base_dir);
        Self {
            base_dir,
            shard_id,
            zone_index_hits: AtomicU64::new(0),
            zone_index_misses: AtomicU64::new(0),
            zone_index_reloads: AtomicU64::new(0),
            column_handle_by_key: Mutex::new(HashMap::new()),
        }
    }

    #[inline]
    fn segment_dir(&self, segment_id: &str) -> PathBuf {
        self.base_dir.join(segment_id)
    }

    pub fn get_or_load_zone_index(
        &self,
        segment_id: &str,
        uid: &str,
    ) -> Result<Arc<ZoneIndex>, std::io::Error> {
        let (arc, outcome) = GlobalZoneIndexCache::instance()
            .get_or_load(&self.base_dir, segment_id, uid, self.shard_id)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        match outcome {
            CacheOutcome::Hit => {
                self.zone_index_hits.fetch_add(1, Ordering::Relaxed);
            }
            CacheOutcome::Miss => {
                self.zone_index_misses.fetch_add(1, Ordering::Relaxed);
            }
            CacheOutcome::Reload => {
                self.zone_index_reloads.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(arc)
    }

    pub fn get_or_load_column_handle(
        &self,
        segment_id: &str,
        uid: &str,
        field: &str,
    ) -> Result<Arc<ColumnHandle>, std::io::Error> {
        let key = (segment_id.to_string(), uid.to_string(), field.to_string());
        if let Some(v) = self
            .column_handle_by_key
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .cloned()
        {
            return Ok(v);
        }

        let segment_dir = self.segment_dir(segment_id);
        let handle = ColumnHandle::open(&segment_dir, uid, field)?;
        let arc = Arc::new(handle);

        let mut map = self
            .column_handle_by_key
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let entry = map.entry(key).or_insert_with(|| Arc::clone(&arc));
        Ok(Arc::clone(entry))
    }

    pub fn zone_index_summary_line(&self) -> String {
        let h = self.zone_index_hits.load(Ordering::Relaxed);
        let m = self.zone_index_misses.load(Ordering::Relaxed);
        let r = self.zone_index_reloads.load(Ordering::Relaxed);
        format!("zone_index_cache: hits={} misses={} reloads={}", h, m, r)
    }
}

fn parse_shard_id(base_dir: &PathBuf) -> Option<usize> {
    base_dir
        .file_name()
        .and_then(|os| os.to_str())
        .and_then(|name| name.strip_prefix("shard-"))
        .and_then(|id| id.parse::<usize>().ok())
}
