use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::engine::core::zone::zone_index::ZoneIndex;

use super::column_handle::ColumnHandle;

#[derive(Debug)]
pub struct QueryCaches {
    pub(crate) base_dir: PathBuf,
    zone_index_by_segment_uid: Mutex<HashMap<(String, String), Arc<ZoneIndex>>>,
    column_handle_by_key: Mutex<HashMap<(String, String, String), Arc<ColumnHandle>>>,
}

impl QueryCaches {
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            zone_index_by_segment_uid: Mutex::new(HashMap::new()),
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
        let key = (segment_id.to_string(), uid.to_string());
        if let Some(v) = self
            .zone_index_by_segment_uid
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .cloned()
        {
            return Ok(v);
        }

        let path = self.segment_dir(segment_id).join(format!("{}.idx", uid));
        let zi = ZoneIndex::load_from_path(&path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let arc = Arc::new(zi);

        let mut map = self
            .zone_index_by_segment_uid
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let entry = map.entry(key).or_insert_with(|| Arc::clone(&arc));
        Ok(Arc::clone(entry))
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
}
